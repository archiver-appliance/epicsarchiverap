package org.epics.archiverappliance.engine.V4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.waitForStatusChange;

/**
 * Checks pausing and resuming a pv keeps it using the pvaccess protocol.
 */
@Tag("integration")
@Tag("localEpics")
public class PauseResumeV4Test {
    

    private static final Logger logger = LogManager.getLogger(SampleV4PVAClientTest.class.getName());
    private SIOCSetup ioc;
    TomcatSetup tomcatSetup = new TomcatSetup();

    @BeforeEach
    public void setUp() throws Exception {
        ioc = new SIOCSetup();
        ioc.startSIOCWithDefaultDB();

        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        ioc.stopSIOC();
        tomcatSetup.tearDown();
    }

    @Test
    public void testPauseRestart() throws Exception {
        String pvName = "UnitTestNoNamingConvention:sine:calc";
        String pvURLName = URLEncoder.encode(pvName, StandardCharsets.UTF_8);

        // Archive PV
        String mgmtUrl = "http://localhost:17665/mgmt/bpl/";
        String archivePVURL = mgmtUrl + "archivePV?pv=pva://";

        GetUrlContent
                .getURLContentAsJSONArray(archivePVURL + pvURLName);
        waitForStatusChange(pvName, "Being archived", 100, mgmtUrl);

        usingPvAccessCheck(pvURLName, mgmtUrl);

        // Let's pause the PV.
        String pausePVURL = mgmtUrl + "pauseArchivingPV?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        JSONObject pauseStatus = GetUrlContent.getURLContentAsJSONObject(pausePVURL);
        Assertions.assertTrue(pauseStatus.containsKey("status") && pauseStatus.get("status").equals("ok"), "Pause PV");
        waitForStatusChange(pvName, "Paused", 20, mgmtUrl);

        // Resume PV
        String resumePVURL = mgmtUrl + "resumeArchivingPV?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        JSONObject pvResumeStatus = GetUrlContent.getURLContentAsJSONObject(resumePVURL);
        Assertions.assertTrue(pvResumeStatus.containsKey("status") && pvResumeStatus.get("status").equals("ok"), "Resume PV");
        waitForStatusChange(pvName, "Being archived", 20, mgmtUrl);

        usingPvAccessCheck(pvURLName, mgmtUrl);

    }

    private void usingPvAccessCheck(String pvURLName, String mgmtUrl) {
        // Check using PVAccess
        String pvDetailsURL = mgmtUrl + "getPVDetails?pv=";
        JSONArray pvInfo = GetUrlContent.getURLContentAsJSONArray(pvDetailsURL + pvURLName);
        JSONObject pvAccessInfo = (JSONObject) pvInfo.get(12);
        Assertions.assertEquals("Are we using PVAccess?", pvAccessInfo.get("name"));
        Assertions.assertEquals("Yes", pvAccessInfo.get("value"));
    }

}
