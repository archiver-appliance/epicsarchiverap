package org.epics.archiverappliance.mgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

@Tag("integration")
@Tag("localEpics")
public class ChangeArchivalParamsTest {
    private static Logger logger = LogManager.getLogger(ChangeArchivalParamsTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();

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
    public void testChangeArchivalParams() throws Exception {
        String pvNameToArchive = "UnitTestNoNamingConvention:sine";
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
                .get(0));
        Assertions.assertEquals(pvst.get("samplingPeriod"), "11.0");
    }
}
