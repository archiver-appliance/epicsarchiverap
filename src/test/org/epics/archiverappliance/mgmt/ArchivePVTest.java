package org.epics.archiverappliance.mgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Use the firefox driver to test operator's adding a PV to the system.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class ArchivePVTest {
    private static Logger logger = LogManager.getLogger(ArchivePVTest.class.getName());
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
    public void testSimpleArchivePV() throws Exception {
        String pvNameToArchive = "UnitTestNoNamingConvention:sine";
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
}
