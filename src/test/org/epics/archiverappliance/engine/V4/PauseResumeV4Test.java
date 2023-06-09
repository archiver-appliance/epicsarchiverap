package org.epics.archiverappliance.engine.V4;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.waitForStatusChange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Checks pausing and resuming a pv keeps it using the pvaccess protocol.
 */
@Category({LocalEpicsTests.class , IntegrationTests.class})
public class PauseResumeV4Test {
    

    private static final Logger logger = LogManager.getLogger(SampleV4PVAClientTest.class.getName());
    private SIOCSetup ioc;
    TomcatSetup tomcatSetup = new TomcatSetup();

    @Before
    public void setUp() throws Exception {
        ioc = new SIOCSetup();
        ioc.startSIOCWithDefaultDB();

        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @After
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
        assertTrue("Pause PV", pauseStatus.containsKey("status") && pauseStatus.get("status").equals("ok"));
        waitForStatusChange(pvName, "Paused", 20, mgmtUrl);

        // Resume PV
        String resumePVURL = mgmtUrl + "resumeArchivingPV?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        JSONObject pvResumeStatus = GetUrlContent.getURLContentAsJSONObject(resumePVURL);
        assertTrue("Resume PV", pvResumeStatus.containsKey("status") && pvResumeStatus.get("status").equals("ok"));
        waitForStatusChange(pvName, "Being archived", 20, mgmtUrl);

        usingPvAccessCheck(pvURLName, mgmtUrl);

    }

    private void usingPvAccessCheck(String pvURLName, String mgmtUrl) {
        // Check using PVAccess
        String pvDetailsURL = mgmtUrl + "getPVDetails?pv=";
        JSONArray pvInfo = GetUrlContent.getURLContentAsJSONArray(pvDetailsURL + pvURLName);
        JSONObject pvAccessInfo = (JSONObject) pvInfo.get(12);
        assertEquals("Are we using PVAccess?", pvAccessInfo.get("name"));
        assertEquals("Yes", pvAccessInfo.get("value"));
    }

}
