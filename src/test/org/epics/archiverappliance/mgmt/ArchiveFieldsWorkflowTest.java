package org.epics.archiverappliance.mgmt;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test archiving of fields - this should include standard field like HIHI and also non-standard fields.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class ArchiveFieldsWorkflowTest {
    private static Logger logger = LogManager.getLogger(ArchiveFieldsWorkflowTest.class.getName());
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
    public void testArchiveFieldsPV() throws Exception {
        String[] pvs = new String[] {
            "UnitTestNoNamingConvention:sine",
            "UnitTestNoNamingConvention:sine.EOFF",
            "UnitTestNoNamingConvention:sine.EGU",
            "UnitTestNoNamingConvention:sine.ALST",
            "UnitTestNoNamingConvention:sine.HOPR",
            "UnitTestNoNamingConvention:sine.DESC",
            "UnitTestNoNamingConvention:sine.YYZ"
        };

        List<JSONObject> arSpecs = List.of(pvs).stream().map((x) -> new JSONObject(Map.of("pv", x))).collect(Collectors.toList());
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        logger.info("Archiving multiple PVs");
        GetUrlContent.postDataAndGetContentAsJSONArray(mgmtURL + "archivePV", GetUrlContent.from(arSpecs));
        for(String pv : pvs) {
            logger.info("Checking to see if PV is being archived {}", pv);
            if(!pv.equals("UnitTestNoNamingConvention:sine.YYZ")) {
                PVAccessUtil.waitForStatusChange(pv, "Being archived", 10, mgmtURL, 15);
            } else {
                PVAccessUtil.waitForStatusChange(pv, "Initial sampling", 10, mgmtURL, 15);
            }
        }
    }
}
