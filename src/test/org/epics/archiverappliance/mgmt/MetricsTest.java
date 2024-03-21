package org.epics.archiverappliance.mgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.waitForStatusChange;

@Tag("integration")
public class MetricsTest {

    private static final Logger logger = LogManager.getLogger(MetricsTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    private static final String pvPrefix = MetricsTest.class.getSimpleName();
    SIOCSetup siocSetup = new SIOCSetup(MetricsTest.class.getSimpleName());

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

    @Test
    void testApplianceMetrics() {
        Map<String, String> expectedMetrics = new HashMap<>(Map.of(
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
        String mgmtUrl = "http://localhost:17665/mgmt/bpl/";

        assertApplianceMetricsMatch(mgmtUrl, expectedMetrics);

        String pvName = pvPrefix + "test_1";

        // Archive PV
        String archivePVURL = mgmtUrl + "archivePV?pv=pva://";

        String pvURLName = URLEncoder.encode(pvName, StandardCharsets.UTF_8);

        GetUrlContent.getURLContentAsJSONArray(archivePVURL + pvURLName);
        waitForStatusChange(pvName, "Being archived", 60, mgmtUrl, 10);

        expectedMetrics.put("pvCount", "1");
        expectedMetrics.put("connectedPVCount", "1");
        assertApplianceMetricsMatch(mgmtUrl, expectedMetrics);
    }

    private static void assertApplianceMetricsMatch(String mgmtUrl, Map<String, String> expectedMetrics) {
        JSONArray metricsResponse = GetUrlContent.getURLContentAsJSONArray(mgmtUrl + "getApplianceMetrics");
        Map<String, String> metricsResult = (Map<String, String>) metricsResponse.get(0);

        expectedMetrics.forEach((k, v) -> Assertions.assertEquals(v, metricsResult.get(k), "Fail check on " + k));
    }

    @Test
    void testApplianceMetricsForAppliance() {

        Map<String, String> expectedMetrics = new HashMap<>(Map.of(
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
        String mgmtUrl = "http://localhost:17665/mgmt/bpl/";

        assertApplianceMetricsMatch(mgmtUrl, expectedMetrics);

        String pvName = pvPrefix + "test_1";

        // Archive PV
        String archivePVURL = mgmtUrl + "archivePV?pv=pva://";

        String pvURLName = URLEncoder.encode(pvName, StandardCharsets.UTF_8);

        GetUrlContent.getURLContentAsJSONArray(archivePVURL + pvURLName);
        waitForStatusChange(pvName, "Being archived", 60, mgmtUrl, 10);

        expectedMetrics.put("pvCount", "1");
        expectedMetrics.put("connectedPVCount", "1");
        assertApplianceMetricsMatch(mgmtUrl, expectedMetrics);
    }
}
