package org.epics.archiverappliance.mgmt;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.RetrievalMetrics;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.waitForStatusChange;

@Tag("integration")
public class MetricsTest {

    private static final Logger logger = LogManager.getLogger(MetricsTest.class.getName());
    private static final TomcatSetup tomcatSetup = new TomcatSetup();
    private static final String pvPrefix = MetricsTest.class.getSimpleName();
    private static final SIOCSetup siocSetup = new SIOCSetup(MetricsTest.class.getSimpleName());
    private static final String mgmtUrl = "http://localhost:17665/mgmt/bpl/";
    private static final String pvName = pvPrefix + "test_1";

    @BeforeEach
    public void setUp() throws Exception {
        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    private static void archivePV() {

        // Archive PV
        String archivePVURL = mgmtUrl + "archivePV?pv=";

        String pvURLName = URLEncoder.encode(pvName, StandardCharsets.UTF_8);

        GetUrlContent.getURLContentAsJSONArray(archivePVURL + pvURLName);
        waitForStatusChange(pvName, "Being archived", 60, mgmtUrl, 10);
    }

    private static void retrievePV() {
        Instant now = Instant.now();
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        rawDataRetrieval.getDataForPVS(
                new String[] {pvName},
                now,
                Instant.now(),
                desc -> logger.info("Getting data for PV " + desc.getPvName()));
    }

    private static void assertApplianceMetricsMatch(Map<String, String> expectedMetrics) {
        JSONArray metricsResponse = GetUrlContent.getURLContentAsJSONArray(MetricsTest.mgmtUrl + "getApplianceMetrics");
        Map<String, String> metricsResult = (Map<String, String>) metricsResponse.get(0);

        partialAssertMap(expectedMetrics, metricsResult);
    }

    private static void partialAssertMap(Map<String, String> expectedMetrics, Map<String, String> metricsResult) {
        logger.info("Actual metrics String hashmap are: " + metricsResult);
        logger.info("Expected metrics String hashmap are: " + expectedMetrics);

        expectedMetrics.forEach((k, v) -> Assertions.assertEquals(
                Objects.toString(v), Objects.toString(metricsResult.get(k)), "Fail check on " + k));
    }

    private static void assertApplianceMetricsDetailsMatch(Map<String, String> expectedMetrics, String appliance) {
        JSONArray metricsResponse = GetUrlContent.getURLContentAsJSONArray(MetricsTest.mgmtUrl
                + "getApplianceMetricsForAppliance?appliance=" + URLEncoder.encode(appliance, StandardCharsets.UTF_8));
        List<Map<String, String>> metricsResult = (List<Map<String, String>>) metricsResponse;

        Map<String, String> actual = convertToStringMap(metricsResult);
        partialAssertMap(expectedMetrics, actual);
    }

    private static void assertPVDetailsMatch(Map<String, String> expectedMetrics) {
        JSONArray metricsResponse = GetUrlContent.getURLContentAsJSONArray(
                MetricsTest.mgmtUrl + "getPVDetails?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8));
        List<Map<String, String>> metricsResult = (List<Map<String, String>>) metricsResponse;

        Map<String, String> actual = convertToStringMap(metricsResult);
        partialAssertMap(expectedMetrics, actual);
    }

    private static Map<String, String> convertToStringMap(List<Map<String, String>> actualMetrics) {
        logger.info("Actual metrics are: " + actualMetrics);
        return actualMetrics.stream()
                .collect((Collectors.toMap(
                        m -> m.get("source") + m.get("name"), m -> StringUtils.defaultString(m.get("value")))));
    }

    String retrievalSource = ConfigService.WAR_FILE.RETRIEVAL.name();
    String engineSource = ConfigService.WAR_FILE.ENGINE.name();
    String mgmtSource = "mgmt";
    String etlSource = ConfigService.WAR_FILE.ETL.name();
    String pvSource = "pv";

    @Test
    void testApplianceMetricsForAppliance() {

        Map<String, String> expectedApplianceMetrics = new HashMap<>(Map.of(
                "connectedPVCount",
                "0",
                "instance",
                "appliance0",
                "pvCount",
                "0",
                "disconnectedPVCount",
                "0",
                "status",
                "Working"));

        Map<String, String> expectedMetricsDetails = new HashMap<>(Map.of(
                "mgmt" + "Appliance Identity",
                "appliance0",
                engineSource + "Total PV count",
                "0",
                engineSource + "Disconnected PV count",
                "0",
                engineSource + "Connected PV count",
                "0",
                engineSource + "Paused PV count",
                "0",
                engineSource + "Total channels",
                "0",
                retrievalSource + RetrievalMetrics.NUMBER_OF_RETRIEVAL_REQUESTS,
                "0",
                retrievalSource + RetrievalMetrics.NUMBER_OF_UNIQUE_USERS,
                "0"));

        assertApplianceMetricsMatch(expectedApplianceMetrics);
        assertApplianceMetricsDetailsMatch(expectedMetricsDetails, "appliance0");

        archivePV();
        expectedApplianceMetrics.put("pvCount", "1");
        expectedApplianceMetrics.put("connectedPVCount", "1");
        expectedMetricsDetails.put(engineSource + "Total PV count", "1");
        expectedMetricsDetails.put(engineSource + "Connected PV count", "1");
        expectedMetricsDetails.put(engineSource + "Total channels", "8"); // All the meta channels

        assertApplianceMetricsMatch(expectedApplianceMetrics);
        assertApplianceMetricsDetailsMatch(expectedMetricsDetails, "appliance0");

        Map<String, String> expectedPVDetails = new HashMap<>(Map.ofEntries(
                entry(mgmtSource + "PV Name", pvName),
                entry(mgmtSource + "Instance archiving PV", "appliance0"),
                entry(mgmtSource + "Archiver DBR type (from typeinfo):", "DBR_SCALAR_DOUBLE"),
                entry(mgmtSource + "Is this a scalar:", "Yes"),
                entry(mgmtSource + "Number of elements:", "1"),
                entry(mgmtSource + "Precision:", "0.0"),
                entry(mgmtSource + "Units:", ""),
                entry(mgmtSource + "Is this PV paused:", "No"),
                entry(mgmtSource + "Sampling method:", "MONITOR"),
                entry(mgmtSource + "Sampling period:", "1.0"),
                entry(mgmtSource + "Are we using PVAccess?", "No"),
                entry(pvSource + "Controlling PV", ""),
                entry(pvSource + "Is engine currently archiving this?", "yes"),
                entry(pvSource + "Sample buffer capacity", "3"),
                entry(etlSource + "Name (from ETL)", pvName),
                entry(retrievalSource + RetrievalMetrics.NUMBER_OF_RETRIEVAL_REQUESTS, "0"),
                entry(retrievalSource + RetrievalMetrics.NUMBER_OF_UNIQUE_USERS, "0")));
        assertPVDetailsMatch(expectedPVDetails);

        retrievePV();

        expectedMetricsDetails.put(retrievalSource + RetrievalMetrics.NUMBER_OF_RETRIEVAL_REQUESTS, "1");
        expectedMetricsDetails.put(retrievalSource + RetrievalMetrics.NUMBER_OF_UNIQUE_USERS, "1");
        expectedPVDetails.put(retrievalSource + RetrievalMetrics.NUMBER_OF_RETRIEVAL_REQUESTS, "1");
        expectedPVDetails.put(retrievalSource + RetrievalMetrics.NUMBER_OF_UNIQUE_USERS, "1");

        assertApplianceMetricsMatch(expectedApplianceMetrics);
        assertApplianceMetricsDetailsMatch(expectedMetricsDetails, "appliance0");
        assertPVDetailsMatch(expectedPVDetails);

        retrievePV();

        expectedMetricsDetails.put(retrievalSource + RetrievalMetrics.NUMBER_OF_RETRIEVAL_REQUESTS, "2");
        expectedMetricsDetails.put(retrievalSource + RetrievalMetrics.NUMBER_OF_UNIQUE_USERS, "1");
        expectedPVDetails.put(retrievalSource + RetrievalMetrics.NUMBER_OF_RETRIEVAL_REQUESTS, "2");
        expectedPVDetails.put(retrievalSource + RetrievalMetrics.NUMBER_OF_UNIQUE_USERS, "1");

        assertApplianceMetricsMatch(expectedApplianceMetrics);
        assertApplianceMetricsDetailsMatch(expectedMetricsDetails, "appliance0");
        assertPVDetailsMatch(expectedPVDetails);
    }
}
