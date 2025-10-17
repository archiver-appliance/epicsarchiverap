package org.epics.archiverappliance.reshard;

import static edu.stanford.slac.archiverappliance.plain.pb.PBPlainFileHandler.PB_PLUGIN_IDENTIFIER;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.time.Instant;

/**
 * Simple test to test appending data from an older PV to a newer one.
 * <ul>
 * <li>Bring up a cluster of two appliances.</li>
 * <li>Archive multiple sets of 2 PV's.</li>
 * <li>Generate data for older and newer PV with various stages of overlap.</li>
 * <li>Pause each set of PV's</li>
 * <li>AppendAlias older PV to newer PV </li>
 * <li>Resume the newer PV.</li>
 * <li>Check for data loss and resumption of archiving etc</li>
 * </ul>
 *
 *
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class AppendAndAliasPVTest {
    private static Logger logger = LogManager.getLogger(AppendAndAliasPVTest.class.getName());
    private ConfigServiceForTests configService;
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();
    String folderSTS = ConfigServiceForTests.getDefaultShortTermFolder() + File.separator + "appendAliasSTS";
    String folderMTS = ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "appendAliasMTS";
    String folderLTS = ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "appendAliasLTS";

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

    private void addPVToCluster(String pvName, String appliance, Instant creationTime) throws Exception {
        // Load a sample PVTypeInfo from a prototype file.
        JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File(
                "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
        PVTypeInfo srcPVTypeInfo = new PVTypeInfo();
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        decoder.decode(srcPVTypeInfoJSON, srcPVTypeInfo);
        PVTypeInfo newPVTypeInfo = new PVTypeInfo(pvName, srcPVTypeInfo);
        newPVTypeInfo.setPaused(true);
        newPVTypeInfo.setApplianceIdentity(appliance);
        newPVTypeInfo.setChunkKey(pvName + ":");
        newPVTypeInfo.setCreationTime(creationTime);
        Assertions.assertTrue(
                newPVTypeInfo.getPvName().equals(pvName),
                "Expecting PV typeInfo for " + pvName + "; instead it is " + srcPVTypeInfo.getPvName());
        JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
        GetUrlContent.postDataAndGetContentAsJSONObject(
                "http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8")
                        + "&createnew=true",
                encoder.encode(newPVTypeInfo));
    }

    private void generateData(String pvName, Instant startTime, Instant endTime) throws IOException {
        StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(
                PB_PLUGIN_IDENTIFIER
                        + "://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR",
                configService);
        try (BasicContext context = new BasicContext()) {
            for (long epoch = startTime.toEpochMilli() / 1000; epoch < endTime.toEpochMilli() / 1000; epoch += 60) {
                ArrayListEventStream strm = new ArrayListEventStream(
                        0,
                        new RemotableEventStreamDesc(
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                pvName,
                                TimeUtils.convertToYearSecondTimestamp(epoch).getYear()));
                strm.add(new POJOEvent(
                        ArchDBRTypes.DBR_SCALAR_DOUBLE,
                        TimeUtils.convertFromEpochSeconds(epoch, 0),
                        new ScalarValue<Double>((double) epoch),
                        0,
                        0));
                plugin.appendData(context, pvName, strm);
            }
        }
    }

    private long getEventCountBetween(String pvName, Instant startTime, Instant endTime) throws IOException {
        logger.info(
                "Looking for data for pv " + pvName + " between " + TimeUtils.convertToHumanReadableString(startTime)
                        + " and " + TimeUtils.convertToHumanReadableString(endTime));
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        long totalEvents = 0;
        try (EventStream stream = rawDataRetrieval.getDataForPVS(new String[] {pvName}, startTime, endTime, null)) {
            if (stream != null) {
                for (@SuppressWarnings("unused") Event e : stream) {
                    totalEvents++;
                }
            } else {
                Assertions.fail("Stream is null when retrieving data.");
            }
        }
        return totalEvents;
    }

    @Test
    public void testAppendAliasPV() throws Exception {
        {
            // No overlap use case
            logger.info("Testing the no overlap usecase.");
            addPVToCluster("test_1", "appliance0", TimeUtils.minusDays(TimeUtils.now(), 3 * 365));
            addPVToCluster("test_2", "appliance1", TimeUtils.minusDays(TimeUtils.now(), 5));
            generateData(
                    "test_1", TimeUtils.minusDays(TimeUtils.now(), 2 * 365), TimeUtils.minusDays(TimeUtils.now(), 7));
            generateData("test_2", TimeUtils.minusDays(TimeUtils.now(), 5), TimeUtils.now());

            long eventCountBefore = getEventCountBetween(
                            "test_1",
                            TimeUtils.minusDays(TimeUtils.now(), 2 * 365),
                            TimeUtils.minusDays(TimeUtils.now(), 7))
                    + getEventCountBetween("test_2", TimeUtils.minusDays(TimeUtils.now(), 5), TimeUtils.now());

            String appendAliasPVURL =
                    "http://localhost:17665/mgmt/bpl/appendAndAliasPV?olderpv=test_1&newerpv=test_2&storage=LTS";
            JSONObject appendAliasStatus = GetUrlContent.getURLContentAsJSONObject(appendAliasPVURL);
            logger.info("Append alias response " + appendAliasStatus.toJSONString());
            Assertions.assertTrue(
                    appendAliasStatus.containsKey("status")
                            && appendAliasStatus.get("status").equals("ok"),
                    "Cannot append and alias PV test_1 to test_2");

            long eventCountAfterT1 =
                    getEventCountBetween("test_1", TimeUtils.minusDays(TimeUtils.now(), 2 * 365), TimeUtils.now());
            Assertions.assertTrue(
                    Math.abs(eventCountAfterT1 - eventCountBefore) < 10,
                    "Different event counts Before " + eventCountBefore + " and after " + eventCountAfterT1);
            long eventCountAfterT2 =
                    getEventCountBetween("test_2", TimeUtils.minusDays(TimeUtils.now(), 2 * 365), TimeUtils.now());
            Assertions.assertTrue(
                    Math.abs(eventCountAfterT2 - eventCountBefore) < 10,
                    "Different event counts Before " + eventCountBefore + " and after " + eventCountAfterT2);
        }
        {
            // Overlap use case
            logger.info("Testing the overlap usecase.");
            addPVToCluster("test_3", "appliance0", TimeUtils.minusDays(TimeUtils.now(), 3 * 365));
            addPVToCluster("test_4", "appliance1", TimeUtils.minusDays(TimeUtils.now(), 5));
            generateData(
                    "test_3", TimeUtils.minusDays(TimeUtils.now(), 2 * 365), TimeUtils.minusDays(TimeUtils.now(), 5));
            generateData("test_4", TimeUtils.minusDays(TimeUtils.now(), 7), TimeUtils.now());

            long eventCountBefore = getEventCountBetween(
                            "test_3",
                            TimeUtils.minusDays(TimeUtils.now(), 2 * 365),
                            TimeUtils.minusDays(TimeUtils.now(), 7))
                    + getEventCountBetween("test_4", TimeUtils.minusDays(TimeUtils.now(), 7), TimeUtils.now());

            String appendAliasPVURL =
                    "http://localhost:17665/mgmt/bpl/appendAndAliasPV?olderpv=test_3&newerpv=test_4&storage=LTS";
            JSONObject appendAliasStatus = GetUrlContent.getURLContentAsJSONObject(appendAliasPVURL);
            logger.info("Append alias response " + appendAliasStatus.toJSONString());
            Assertions.assertTrue(
                    appendAliasStatus.containsKey("status")
                            && appendAliasStatus.get("status").equals("ok"),
                    "Cannot append and alias PV test_3 to test_4");

            long missingEvents = 2 * 24 * 60; // One per minute for 2 days.
            long eventCountAfterT1 =
                    getEventCountBetween("test_3", TimeUtils.minusDays(TimeUtils.now(), 2 * 365), TimeUtils.now());
            Assertions.assertTrue(
                    Math.abs(eventCountAfterT1 - eventCountBefore) < (missingEvents + 10),
                    "Different event counts Before " + eventCountBefore + " and after " + eventCountAfterT1);
            long eventCountAfterT2 =
                    getEventCountBetween("test_4", TimeUtils.minusDays(TimeUtils.now(), 2 * 365), TimeUtils.now());
            Assertions.assertTrue(
                    Math.abs(eventCountAfterT2 - eventCountBefore) < (missingEvents + 10),
                    "Different event counts Before " + eventCountBefore + " and after " + eventCountAfterT2);
        }
    }
}
