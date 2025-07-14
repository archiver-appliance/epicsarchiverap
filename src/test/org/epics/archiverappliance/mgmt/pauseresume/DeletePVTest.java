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

/**
 * Start an appserver with persistence; start archiving a PV and then pause it and delete it
 * Make sure that the typeinfo disappears from persistence.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class DeletePVTest {
    private static Logger logger = LogManager.getLogger(DeletePVTest.class.getName());
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
    public void testSimpleDeletePV() throws Exception {
        String pvNameToArchive = "UnitTestNoNamingConvention:sine";
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        // Need this delay to make sure the typeinfo is stable in the cluster
        Thread.sleep(90 * 1000);
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);
        logger.info("We are now archiving the PV; let's go into the details page; pause and delete");

        GetUrlContent.getURLContentWithQueryParameters(mgmtURL + "pauseArchivingPV", Map.of("pv", pvNameToArchive), false);
        Thread.sleep(2 * 1000);
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Paused", 10, mgmtURL, 15);

        GetUrlContent.getURLContentWithQueryParameters(mgmtURL + "resumeArchivingPV", Map.of("pv", pvNameToArchive), false);
        Thread.sleep(2 * 1000);
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);

        GetUrlContent.getURLContentWithQueryParameters(mgmtURL + "pauseArchivingPV", Map.of("pv", pvNameToArchive), false);
        Thread.sleep(2 * 1000);
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Paused", 10, mgmtURL, 15);

        GetUrlContent.getURLContentWithQueryParameters(mgmtURL + "deletePV", Map.of("pv", pvNameToArchive), false);
		Thread.sleep(2 * 1000);
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Not being archived", 10, mgmtURL, 15);
    }
}
