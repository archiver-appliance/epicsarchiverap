package org.epics.archiverappliance.reshard;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Simple test to test resharding a PV from one appliance to another...
 * <ul>
 * <li>Bring up a cluster of two appliances.</li>
 * <li>Archive the PV and wait for it to connect etc.</li>
 * <li>Determine the appliance for the PV.</li>
 * <li>Generate data for a PV making sure we have more than one data source and more than one chunk.</li>
 * <li>Pause the PV.</li>
 * <li>Reshard to the other appliance. </li>
 * <li>Resume the PV.</li>
 * <li>Check for data loss and resumption of archiving etc,</li>
 * </ul>
 *
 * This test will probably fail at the beginning of the year; we generate data into MTS and LTS and if there is an overlap we get an incorrect number of events.
 *
 * @author mshankar
 *
 */
@Tag("localEpics")
@Tag("integration")
public class BasicReshardingTest {
    private static final Logger logger = LogManager.getLogger(BasicReshardingTest.class.getName());
    private final String pvPrefix = BasicReshardingTest.class.getSimpleName();
    private final String pvName = pvPrefix + "UnitTestNoNamingConvention:sine";
    private ConfigServiceForTests configService;
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup(pvPrefix);
    String folderSTS = ConfigServiceForTests.getDefaultShortTermFolder() + File.separator + "reshardSTS";
    String folderMTS = ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "reshardMTS";
    String folderLTS = ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "reshardLTS";

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);

        System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER", folderSTS);
        System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", folderMTS);
        System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER", folderLTS);

        FileUtils.deleteDirectory(new File(folderSTS));
        FileUtils.deleteDirectory(new File(folderMTS));
        FileUtils.deleteDirectory(new File(folderLTS));

        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpClusterWithWebApps(this.getClass().getSimpleName(), 2);
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();

        FileUtils.deleteDirectory(new File(folderSTS));
        FileUtils.deleteDirectory(new File(folderMTS));
        FileUtils.deleteDirectory(new File(folderLTS));
    }

    @Test
    public void testReshardPV() throws Exception {
        String pvNameToArchive = pvName;
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
                mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);

        PVTypeInfo typeInfoBeforePausing = getPVTypeInfo();
        // We determine the appliance for the PV by getting it's typeInfo.
        String applianceIdentity = typeInfoBeforePausing.getApplianceIdentity();
        Assertions.assertNotNull(applianceIdentity, "Cannot determine appliance identity for pv from typeinfo ");

        // We use the PV's PVTypeInfo creation date for moving data. This PVTypeInfo was just created.
        // We need to fake this to an old value so that the data is moved correctly.
        // The LTS data spans 2 years, so we set a creation time of about 4 years ago.
        typeInfoBeforePausing.setCreationTime(TimeUtils.getStartOfYear(TimeUtils.getCurrentYear() - 4));
        String updatePVTypeInfoURL = "http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv="
                + URLEncoder.encode(pvName, "UTF-8") + "&override=true";
        GetUrlContent.postDataAndGetContentAsJSONObject(
                updatePVTypeInfoURL, JSONEncoder.getEncoder(PVTypeInfo.class).encode(typeInfoBeforePausing));

        Instant beforeReshardingCreationTimedstamp = typeInfoBeforePausing.getCreationTime();

        // Generate some data into the MTS and LTS
        String[] dataStores = typeInfoBeforePausing.getDataStores();
        Assertions.assertTrue(
                dataStores != null && dataStores.length > 1, "Data stores is null or empty for pv from typeinfo ");
        for (String dataStore : dataStores) {
            logger.info("Data store for pv " + dataStore);
            StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(dataStore, configService);
            String name = plugin.getName();
            if (name.equals("MTS")) {
                // For the MTS we generate a couple of days worth of data
                Instant startOfMtsData = TimeUtils.minusDays(TimeUtils.now(), 3);
                long startOfMtsDataSecs = TimeUtils.convertToEpochSeconds(startOfMtsData);
                ArrayListEventStream strm = new ArrayListEventStream(
                        0,
                        new RemotableEventStreamDesc(
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                pvName,
                                TimeUtils.convertToYearSecondTimestamp(startOfMtsDataSecs)
                                        .getYear()));
                for (long offsetSecs = 0; offsetSecs < 2 * 24 * 60 * 60; offsetSecs += 60) {
                    strm.add(new SimulationEvent(
                            TimeUtils.convertToYearSecondTimestamp(startOfMtsDataSecs + offsetSecs),
                            ArchDBRTypes.DBR_SCALAR_DOUBLE,
                            new ScalarValue<Double>((double) offsetSecs)));
                }
                try (BasicContext context = new BasicContext()) {
                    plugin.appendData(context, pvName, strm);
                }
            } else if (name.equals("LTS")) {
                // For the LTS we generate a couple of years worth of data
                long startofLtsDataSecs = TimeUtils.getStartOfYearInSeconds(TimeUtils.getCurrentYear() - 2);
                ArrayListEventStream strm = new ArrayListEventStream(
                        0,
                        new RemotableEventStreamDesc(
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                pvName,
                                TimeUtils.convertToYearSecondTimestamp(startofLtsDataSecs)
                                        .getYear()));
                for (long offsetSecs = 0; offsetSecs < 2 * 365 * 24 * 60 * 60; offsetSecs += 24 * 60 * 60) {
                    strm.add(new SimulationEvent(
                            TimeUtils.convertToYearSecondTimestamp(startofLtsDataSecs + offsetSecs),
                            ArchDBRTypes.DBR_SCALAR_DOUBLE,
                            new ScalarValue<Double>((double) offsetSecs)));
                }
                try (BasicContext context = new BasicContext()) {
                    plugin.appendData(context, pvName, strm);
                }
            }
        }
        logger.info("Done generating data. Now making sure the setup is correct by fetching some data.");

        // Get the number of events before resharding...
        long eventCount = getNumberOfEvents();
        long expectedMinEventCount = 2 * 24 * 60 + 2 * 365;
        logger.info("Got " + eventCount + " events");
        Assertions.assertTrue(
                eventCount >= expectedMinEventCount,
                "Expecting at least " + expectedMinEventCount + " got " + eventCount + " for ");

        String otherAppliance = "appliance1";
        if (applianceIdentity.equals(otherAppliance)) {
            otherAppliance = "appliance0";
        }

        // Let's pause the PV.
        String pausePVURL = "http://localhost:17665/mgmt/bpl/pauseArchivingPV?pv="
                + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        JSONObject pauseStatus = GetUrlContent.getURLContentAsJSONObject(pausePVURL);
        Assertions.assertTrue(
                pauseStatus.containsKey("status") && pauseStatus.get("status").equals("ok"), "Cannot pause PV");
        Thread.sleep(1000);
        logger.info("Successfully paused the PV; other appliance is " + otherAppliance);

        GetUrlContent.getURLContentWithQueryParameters(
                mgmtURL + "reshardPV",
                Map.of(
                        "pv", pvNameToArchive,
                        "storage", "LTS",
                        "appliance", otherAppliance),
                false);
        Thread.sleep(2 * 1000);

        String obtainedAppliance = PVAccessUtil.getPVDetail(pvNameToArchive, mgmtURL, "Instance archiving PV");
        Assertions.assertEquals(
                otherAppliance,
                obtainedAppliance,
                "Expecting appliance to be " + otherAppliance + "; instead it is " + obtainedAppliance);
        logger.info("Resharding BPL is done.");

        PVTypeInfo typeInfoAfterResharding = getPVTypeInfo();
        String afterReshardingAppliance = typeInfoAfterResharding.getApplianceIdentity();
        Assertions.assertTrue(
                afterReshardingAppliance != null && afterReshardingAppliance.equals(otherAppliance),
                "Invalid appliance identity after resharding " + afterReshardingAppliance);
        Instant afterReshardingCreationTimedstamp = typeInfoAfterResharding.getCreationTime();

        // Let's resume the PV.
        String resumePVURL = "http://localhost:17665/mgmt/bpl/resumeArchivingPV?pv="
                + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        JSONObject resumeStatus = GetUrlContent.getURLContentAsJSONObject(resumePVURL);
        Assertions.assertTrue(
                resumeStatus.containsKey("status") && resumeStatus.get("status").equals("ok"), "Cannot resume PV");

        long postReshardEventCount = getNumberOfEvents();
        logger.info("After resharding, got " + postReshardEventCount + " events");
        Assertions.assertTrue(
                postReshardEventCount >= expectedMinEventCount,
                "Expecting at least " + expectedMinEventCount + " got " + postReshardEventCount + " for ");

        checkRemnantShardPVs();

        // Make sure the creation timestamps are ok. If we have external integration, these play a part and you can not
        // serve data because the creation timestamp is off
        Assertions.assertEquals(
                beforeReshardingCreationTimedstamp,
                afterReshardingCreationTimedstamp,
                "Creation timestamps before "
                        + TimeUtils.convertToHumanReadableString(beforeReshardingCreationTimedstamp)
                        + " and after "
                        + TimeUtils.convertToHumanReadableString(afterReshardingCreationTimedstamp)
                        + " should be the same");
    }

    private void checkRemnantShardPVs() {
        // Make sure we do not have any temporary PV's present.
        String tempReshardPVs = "http://localhost:17665/mgmt/bpl/getAllPVs?pv=*_reshard_*";
        JSONArray reshardPVs = GetUrlContent.getURLContentAsJSONArray(tempReshardPVs);
        StringWriter buf = new StringWriter();
        for (Object reshardPV : reshardPVs) {
            buf.append(reshardPV.toString());
            buf.append(",");
        }
        Assertions.assertTrue(reshardPVs.isEmpty(), "We seem to have some reshard temporary PV's present " + buf);
    }

    private PVTypeInfo getPVTypeInfo() throws Exception {
        String getPVTypeInfoURL =
                "http://localhost:17665/mgmt/bpl/getPVTypeInfo?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        JSONObject typeInfoJSON = GetUrlContent.getURLContentAsJSONObject(getPVTypeInfoURL);
        Assertions.assertNotNull(typeInfoJSON, "Cannot get typeinfo for pv using " + getPVTypeInfoURL);
        PVTypeInfo unmarshalledTypeInfo = new PVTypeInfo();
        JSONDecoder<PVTypeInfo> typeInfoDecoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        typeInfoDecoder.decode((JSONObject) typeInfoJSON, unmarshalledTypeInfo);
        return unmarshalledTypeInfo;
    }

    private long getNumberOfEvents() throws Exception {
        Instant start =
                TimeUtils.convertFromEpochSeconds(TimeUtils.getStartOfYearInSeconds(TimeUtils.getCurrentYear() - 2), 0);
        Instant end = TimeUtils.now();
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant obtainedFirstSample = null;
        long eventCount = 0;
        try (EventStream stream = rawDataRetrieval.getDataForPVS(new String[] {pvName}, start, end, null)) {
            if (stream != null) {
                for (Event e : stream) {
                    if (obtainedFirstSample == null) {
                        obtainedFirstSample = e.getEventTimeStamp();
                    }
                    logger.debug("Sample from " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
                    eventCount++;
                }
            } else {
                Assertions.fail("Stream is null when retrieving data.");
            }
        }
        return eventCount;
    }
}
