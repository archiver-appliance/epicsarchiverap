package org.epics.archiverappliance.mgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tag("integration")
@Tag("localEpics")
class MgmtBPLTest {

    private static final Logger logger = LogManager.getLogger(MgmtBPLTest.class.getName());
    static TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();

    @BeforeAll
    static void setUpAll() throws Exception {
        tomcatSetup.setUpWebApps(MgmtBPLTest.class.getSimpleName());
    }

    @AfterAll
    static void teardownAll() throws Exception {
        tomcatSetup.tearDown();
    }

    @AfterEach
    void tearDown() throws Exception {
        siocSetup.stopSIOC();
    }

    @Test
    public void testArchiveWithSamplingPeriod() throws Exception {
        String prefix = "testArchiveWithSamplingPeriod";
        siocSetup.setPrefix(prefix);
        siocSetup.startSIOCWithDefaultDB();
        String pvNameToArchive = prefix + "UnitTestNoNamingConvention:sine";
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
            mgmtURL + "/archivePV",
            GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive, "samplingperiod", "10.0")))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);
        @SuppressWarnings("unchecked")
        Map<String, String> status = (Map<String, String>) GetUrlContent.getURLContentWithQueryParametersAsJSONArray(
                mgmtURL + "getPVStatus", Map.of("pv", pvNameToArchive))
            .get(0);
        Assertions.assertTrue(
            "10.0".equals(status.get("samplingPeriod")),
            "Expecting sampling rate to be 10.0; instead it is " + status.get("samplingPeriod"));
    }

    @Test
    public void testArchiveFieldsPV() throws Exception {
        String prefix = "testArchiveFieldsPV";
        siocSetup.setPrefix(prefix);
        siocSetup.startSIOCWithDefaultDB();
        String[] pvs = new String[] {
            prefix + "UnitTestNoNamingConvention:sine",
            prefix + "UnitTestNoNamingConvention:sine.EOFF",
            prefix + "UnitTestNoNamingConvention:sine.EGU",
            prefix + "UnitTestNoNamingConvention:sine.ALST",
            prefix + "UnitTestNoNamingConvention:sine.HOPR",
            prefix + "UnitTestNoNamingConvention:sine.DESC",
            prefix + "UnitTestNoNamingConvention:sine.YYZ"
        };

        List<JSONObject> arSpecs = Stream.of(pvs)
            .map(x -> new JSONObject(Map.of("pv", x)))
            .toList();
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        logger.info("Archiving multiple PVs");
        GetUrlContent.postDataAndGetContentAsJSONArray(mgmtURL + "archivePV", GetUrlContent.from(arSpecs));
        for (String pv : pvs) {
            logger.info("Checking to see if PV is being archived {}", pv);
            if (!pv.equals("UnitTestNoNamingConvention:sine.YYZ")) {
                PVAccessUtil.waitForStatusChange(pv, "Being archived", 10, mgmtURL, 15);
            } else {
                PVAccessUtil.waitForStatusChange(pv, "Initial sampling", 10, mgmtURL, 15);
            }
        }
    }
    @Test
    public void testArchiveAliasedPV() throws Exception {
        String prefix = "testArchiveAliasedPV";
        siocSetup.setPrefix(prefix);
        siocSetup.startSIOCWithDefaultDB();
        String pvNameToArchive = prefix + "UnitTestNoNamingConvention:sinealias";
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
            mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 20, mgmtURL, 15);

        SIOCSetup.caput(prefix + "UnitTestNoNamingConvention:sine.HIHI", 2.0);
        Thread.sleep(2 * 1000);
        SIOCSetup.caput(prefix + "UnitTestNoNamingConvention:sine.HIHI", 3.0);
        Thread.sleep(2 * 1000);
        SIOCSetup.caput(prefix + "UnitTestNoNamingConvention:sine.HIHI", 4.0);
        Thread.sleep(2 * 1000);
        logger.info("Done updating UnitTestNoNamingConvention:sine.HIHI");
        Thread.sleep(2 * 60 * 1000);

        // Test retrieval of data using the real name and the aliased name
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:sine", true);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:sinealias", true);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:sine.HIHI", true);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:sinealias.HIHI", true);
    }
    @Test
    public void testSimpleArchivePV() throws Exception {
        String prefix = "testSimpleArchivePV";
        siocSetup.setPrefix(prefix);
        siocSetup.startSIOCWithDefaultDB();
        String pvNameToArchive = prefix + "UnitTestNoNamingConvention:sine";
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
            mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);
        JSONArray statuses = GetUrlContent.getURLContentAsJSONArray(mgmtURL + "/getPVStatus?pv=" + pvNameToArchive);
        Assertions.assertNotNull(statuses);
        Assertions.assertTrue(statuses.size() > 0);
        @SuppressWarnings("unchecked")
        Map<String, String> status = (Map<String, String>) statuses.get(0);
        Assertions.assertEquals(
            status.getOrDefault("pvName", ""),
            pvNameToArchive,
            "PV Name is not " + pvNameToArchive + "; instead we get " + status.getOrDefault("pvName", ""));
    }

    @Test
    public void testChangeArchivalParams() throws Exception {
        String prefix = "testChangeArchivalParams";
        siocSetup.setPrefix(prefix);
        siocSetup.startSIOCWithDefaultDB();
        String pvNameToArchive = prefix + "UnitTestNoNamingConvention:sine";
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
            mgmtURL + "archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);
        @SuppressWarnings("unchecked")
        Map<String, String> chst = (Map<String, String>) GetUrlContent.getURLContentWithQueryParametersAsJSONObject(
            mgmtURL + "changeArchivalParameters",
            Map.of("pv", pvNameToArchive, "samplingperiod", "11")); // A sample every 11 seconds
        Assertions.assertEquals(chst.get("status"), "ok");
        @SuppressWarnings("unchecked")
        Map<String, String> pvst = (Map<String, String>) (GetUrlContent.getURLContentWithQueryParametersAsJSONArray(
                mgmtURL + "getPVStatus", Map.of("pv", pvNameToArchive))
            .getFirst());
        Assertions.assertEquals(pvst.get("samplingPeriod"), "11.0");
    }

    @Test
    public void testAddRemoveAlias() throws Exception {
        String prefix = "testAddRemoveAlias";
        siocSetup.setPrefix(prefix);
        siocSetup.startSIOCWithDefaultDB();
        String pvNameToArchive = prefix + "UnitTestNoNamingConvention:sine";
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
            mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);

        SIOCSetup.caput(prefix + "UnitTestNoNamingConvention:sine.HIHI", 2.0);
        Thread.sleep(2 * 1000);
        SIOCSetup.caput(prefix + "UnitTestNoNamingConvention:sine.HIHI", 3.0);
        Thread.sleep(2 * 1000);
        SIOCSetup.caput(prefix + "UnitTestNoNamingConvention:sine.HIHI", 4.0);
        Thread.sleep(2 * 1000);
        logger.info("Done updating UnitTestNoNamingConvention:sine.HIHI");
        Thread.sleep(2 * 60 * 1000);

        // Test retrieval of data using the real name and the aliased name
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:sine", true);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:arandomalias", false);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:sine.HIHI", true);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:arandomalias.HIHI", false);

        JSONAware addAliasStatus = GetUrlContent.getURLContentWithQueryParameters(
            mgmtURL + "addAlias",
            Map.of("pv", pvNameToArchive, "aliasname", prefix + "UnitTestNoNamingConvention:arandomalias"),
            false);

        logger.debug("Add alias response " + addAliasStatus.toJSONString());

        Thread.sleep(2 * 1000);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:sine", true);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:arandomalias", true);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:sine.HIHI", true);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:arandomalias.HIHI", true);

        JSONAware removeAliasStatus = GetUrlContent.getURLContentWithQueryParameters(
            mgmtURL + "removeAlias",
            Map.of("pv", pvNameToArchive, "aliasname", prefix + "UnitTestNoNamingConvention:arandomalias"),
            false);
        logger.debug("Remove alias response " + removeAliasStatus.toJSONString());

        Thread.sleep(2 * 1000);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:sine", true);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:arandomalias", false);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:sine.HIHI", true);
        testRetrievalCount(prefix + "UnitTestNoNamingConvention:arandomalias.HIHI", false);
    }

    /**
     * Make sure we get some data when retriving under the given name
     * @param pvName
     * @param expectingData - true if we are expecting any data at all.
     * @throws IOException
     */
    private void testRetrievalCount(String pvName, boolean expectingData) throws IOException {
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
            "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant end = TimeUtils.plusDays(TimeUtils.now(), 3);
        Instant start = TimeUtils.minusDays(end, 6);
        try (EventStream stream = rawDataRetrieval.getDataForPVS(new String[] {pvName}, start, end, null)) {
            long previousEpochSeconds = 0;
            int eventCount = 0;

            // We are making sure that the stream we get back has times in sequential order...
            if (stream != null) {
                for (Event e : stream) {
                    long actualSeconds = e.getEpochSeconds();
                    Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
                    previousEpochSeconds = actualSeconds;
                    eventCount++;
                }
            }

            logger.info("Got " + eventCount + " event for pv " + pvName);
            if (expectingData) {
                Assertions.assertTrue(
                    eventCount > 0,
                    "When asking for data using " + pvName + ", event count is 0. We got " + eventCount);
            } else {
                Assertions.assertEquals(0, eventCount, "When asking for data using " + pvName + ", event count is 0. We got " + eventCount);
            }
        }
    }
}
