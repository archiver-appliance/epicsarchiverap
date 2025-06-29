package org.epics.archiverappliance.mgmt.pauseresume;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Start an appserver with persistence; start archiving a PV and then pause it and delete it
 * Make sure that the typeinfo disappears from persistence.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class DeleteMultiplePVTest {
    private static Logger logger = LogManager.getLogger(DeleteMultiplePVTest.class.getName());
    private File persistenceFolder =
            new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "DeletePVTest");
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();

    @BeforeEach
    public void setUp() throws Exception {
        if (persistenceFolder.exists()) {
            FileUtils.deleteDirectory(persistenceFolder);
        }
        persistenceFolder.mkdirs();
        System.getProperties()
                .put(
                        ConfigService.ARCHAPPL_PERSISTENCE_LAYER,
                        "org.epics.archiverappliance.config.persistence.JDBM2Persistence");
        System.getProperties()
                .put(
                        JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME,
                        persistenceFolder.getPath() + File.separator + "testconfig.jdbm2");

        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    @Test
    public void testDeleteMultiplePV() throws Exception {
        String[] pvs = { "UnitTestNoNamingConvention:sine", "UnitTestNoNamingConvention:cosine", "test_0", "test_1", "test_2" };
        List<JSONObject> arSpecs = List.of(pvs).stream().map((x) -> new JSONObject(Map.of("pv", x))).collect(Collectors.toList());
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        logger.info("Archiving 5 PV");
        GetUrlContent.postDataAndGetContentAsJSONArray(mgmtURL + "archivePV", GetUrlContent.from(arSpecs));
        for(String pv : pvs) {
            PVAccessUtil.waitForStatusChange(pv, "Being archived", 10, mgmtURL, 15);
        }

        logger.info("Pausing 2 PV");
        List<String> pvsToDelete = List.of("test_0", "test_1");
        for(String pv : pvsToDelete) {
            GetUrlContent.getURLContentWithQueryParameters(mgmtURL + "pauseArchivingPV", Map.of("pv", pv), false);
            Thread.sleep(2 * 1000);
            PVAccessUtil.waitForStatusChange(pv, "Paused", 10, mgmtURL, 15);
        }

        for(String pv : pvsToDelete) {
            GetUrlContent.getURLContentWithQueryParameters(mgmtURL + "deletePV", Map.of("pv", pv), false);
            Thread.sleep(2 * 1000);
            PVAccessUtil.waitForStatusChange(pv, "Not being archived", 10, mgmtURL, 15);
        }

        logger.info("Checking other PV are still there");
        for(String pv : pvs) {
            if(!pvsToDelete.contains(pv)) {
                PVAccessUtil.waitForStatusChange(pv, "Being archived", 10, mgmtURL, 15);
            }
        }
    }
}
