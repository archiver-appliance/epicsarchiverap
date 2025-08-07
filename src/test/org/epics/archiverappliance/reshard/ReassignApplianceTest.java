package org.epics.archiverappliance.reshard;

import static edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin.PB_PLUGIN_IDENTIFIER;
import static org.epics.archiverappliance.config.ConfigServiceForTests.MGMT_URL;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.mgmt.bpl.ReassignAppliance;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONArray;
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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

/**
 * Simple test to test reassigning a PV from one appliance to another.
 * <ul>
 * <li>Bring up a cluster of two appliances.</li>
 * <li>Put a PVTypeInfo assigned to one appliance and resume it. Make sure
 * <ul>
 * <li>ETL jobs are set up</li>
 * <li>CA/PVA connections are made</li>
 * <li>Retrieval works</li>
 * </ul>
 * </li>
 * <li>Reassign the PV to the other appliance. Make sure
 * <ul>
 * <li>Database entries are updated</li>
 * <li>ETL jobs are set up</li>
 * <li>CA/PVA connections are made</li>
 * <li>Retrieval works</li>
 * </ul>
 * </li>
 * </ul>
 *
 * @author mshankar
 *
 */
@Tag("localEpics")
@Tag("integration")
public class ReassignApplianceTest {
    private static final Logger logger = LogManager.getLogger(ReassignApplianceTest.class.getName());
    private final String pvPrefix = ReassignApplianceTest.class.getSimpleName();
    private final String pvName = pvPrefix + "ArchUnitTest:counter10Hz"; // 10 Hz PV
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup(pvPrefix);
    String folderLTS = ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "reassignLTS";
    Instant startOfArchiving = TimeUtils.now();

    private static final String appliance0URLPrefix = "http://localhost:17665/";
    private static final String appliance1URLPrefix = "http://localhost:17666/";

    @BeforeEach
    public void setUp() throws Exception {

        System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER", folderLTS);
        FileUtils.deleteDirectory(new File(folderLTS));
        siocSetup.startSIOCWithDefaultDB();
        System.getProperties().put(ReassignAppliance.class.getCanonicalName(), "true");
        tomcatSetup.setUpClusterWithWebApps(this.getClass().getSimpleName(), 2);
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();

        FileUtils.deleteDirectory(new File(folderLTS));
    }

    @Test
    public void testReassignPV() throws Exception {
        ConfigService configService = new ConfigServiceForTests(-1);
        // Load a sample PVTypeInfo from a prototype file.
        JSONObject typeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(
                "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json")));
        PVTypeInfo typeInfo = new PVTypeInfo();
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
        decoder.decode(typeInfoJSON, typeInfo);

        typeInfo.setPvName(pvName);
        typeInfo.setPaused(false);
        typeInfo.setSamplingPeriod(0.01f);
        typeInfo.setChunkKey(configService.getPVNameToKeyConverter().convertPVNameToKey(pvName));
        typeInfo.setCreationTime(TimeUtils.convertFromISO8601String("2013-11-11T14:49:58.523Z"));
        typeInfo.setModificationTime(TimeUtils.now());
        typeInfo.setApplianceIdentity("appliance0");
        String[] dataStores = {
            PB_PLUGIN_IDENTIFIER
                    + "://localhost?name=STS&rootFolder=${ARCHAPPL_SHORT_TERM_FOLDER}&partitionGranularity=PARTITION_HOUR&consolidateOnShutdown=true",
            PB_PLUGIN_IDENTIFIER
                    + "://localhost?name=MTS&rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&partitionGranularity=PARTITION_DAY&consolidateOnShutdown=true",
            PB_PLUGIN_IDENTIFIER + "://localhost?name=LTS&rootFolder=" + new File(folderLTS).getAbsolutePath()
                    + "/common&partitionGranularity=PARTITION_YEAR"
        };
        typeInfo.setDataStores(dataStores);

        GetUrlContent.postDataAndGetContentAsJSONObject(
                MGMT_URL + "/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                        + "&override=false&createnew=true",
                encoder.encode(typeInfo));
        GetUrlContent.getURLContentAsJSONObject(
                MGMT_URL + "/resumeArchivingPV?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8));

        logger.info("Added " + pvName + " to appliance0");
        Thread.sleep(60 * 1000 + 5 * 1000);

        Assertions.assertTrue(GetUrlContent.getURLContentAsJSONArray(
                                appliance0URLPrefix + "/etl/bpl/getPVDetails?pv="
                                        + URLEncoder.encode(pvName, StandardCharsets.UTF_8),
                                true)
                        .size()
                > 5);
        Assertions.assertTrue(GetUrlContent.getURLContentAsJSONArray(
                                appliance1URLPrefix + "/etl/bpl/getPVDetails?pv="
                                        + URLEncoder.encode(pvName, StandardCharsets.UTF_8),
                                true)
                        .size()
                <= 1);

        Assertions.assertTrue(GetUrlContent.getURLContentAsJSONArray(
                                appliance0URLPrefix + "/engine/bpl/getPVDetails?pv="
                                        + URLEncoder.encode(pvName, StandardCharsets.UTF_8),
                                false)
                        .size()
                > 5);
        Assertions.assertNull(GetUrlContent.getURLContentAsJSONArray(
                appliance1URLPrefix + "/engine/bpl/getPVDetails?pv="
                        + URLEncoder.encode(pvName, StandardCharsets.UTF_8),
                false));

        int expectedSampleCount = 60 * 10;
        List<Integer> valsBefore = getEvents();
        Assertions.assertTrue(
                valsBefore.size() >= expectedSampleCount,
                "Expected at least " + expectedSampleCount + " got " + valsBefore.size());

        GetUrlContent.getURLContentAsJSONObject(
                MGMT_URL + "/reassignAppliance?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                        + "&appliance=appliance1",
                true);

        logger.info("Reassigned " + pvName + " to appliance1");
        Thread.sleep(60 * 1000 + 10 * 1000);

        Assertions.assertTrue(logAndGet(appliance0URLPrefix + "/etl/bpl/getPVDetails?pv="
                                + URLEncoder.encode(pvName, StandardCharsets.UTF_8))
                        .size()
                <= 1);
        Assertions.assertTrue(logAndGet(appliance1URLPrefix + "/etl/bpl/getPVDetails?pv="
                                + URLEncoder.encode(pvName, StandardCharsets.UTF_8))
                        .size()
                > 5);

        Assertions.assertNull(logAndGet(appliance0URLPrefix + "/engine/bpl/getPVDetails?pv="
                + URLEncoder.encode(pvName, StandardCharsets.UTF_8)));
        Assertions.assertTrue(logAndGet(appliance1URLPrefix + "/engine/bpl/getPVDetails?pv="
                                + URLEncoder.encode(pvName, StandardCharsets.UTF_8))
                        .size()
                > 5);

        List<Integer> valsAfter = getEvents();
        expectedSampleCount = 2 * 60 * 10;
        Assertions.assertTrue(
                valsAfter.size() >= expectedSampleCount,
                "Expected at least " + expectedSampleCount + " got " + valsAfter.size());

        // Confirm that every sample in the before made it into the after.
        // This is largely a matter of confirming the setup.
        LinkedList<String> missingVals = new LinkedList<String>();
        for (Integer val : valsBefore) {
            if (!valsAfter.contains(val)) {
                missingVals.add(Integer.toString(val));
            }
        }
        // Depending on your machine's CPU/networking, we may miss one or more samples in the reassignment.
        // In an ideal world, this should be 0; we arbitrarily allow for 0.5 seconds for the reassignment to complete
        Assertions.assertTrue(missingVals.size() <= 5, "Missing previous values " + String.join(",", missingVals));
        if (missingVals.size() > 0) {
            logger.warn("Missing previous values " + String.join(",", missingVals));
        }
    }

    private static JSONArray logAndGet(String url) {
        JSONArray ret = GetUrlContent.getURLContentAsJSONArray(url, false);
        if (ret == null) {
            logger.info("Got a null response for " + url);
        } else {
            logger.info("Got a valid response for " + url);
            logger.info(ret.toJSONString());
        }
        return ret;
    }

    private LinkedList<Integer> getEvents() throws IOException {
        LinkedList<Integer> values = new LinkedList<Integer>();
        Instant start = this.startOfArchiving;
        Instant end = TimeUtils.now();
        RawDataRetrievalAsEventStream rawDataRetrieval =
                new RawDataRetrievalAsEventStream(appliance0URLPrefix + "/retrieval/data/getData.raw");
        try (EventStream stream = rawDataRetrieval.getDataForPVS(new String[] {pvName}, start, end, null)) {
            if (stream != null) {
                for (Event e : stream) {
                    int intValue =
                            ((DBRTimeEvent) e).getSampleValue().getValue().intValue();
                    logger.info("Got value {}", intValue);
                    values.add(intValue);
                }
            } else {
                Assertions.fail("Stream is null when retrieving data.");
            }
        }
        return values;
    }
}
