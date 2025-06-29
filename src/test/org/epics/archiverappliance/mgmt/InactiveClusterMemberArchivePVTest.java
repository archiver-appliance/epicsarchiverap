package org.epics.archiverappliance.mgmt;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
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
 * Complex unit test for testing what happens when we ask to archive a PV when the member that is currently archiving it is inactive.
 * <ul>
 * <li>Create a dummy config persistence file that has PVs assigned to two instances</li>
 * <li>Start up a tomcat using this file as persistence as one of the two instances</li>
 * <li>Ask to archive PVs that belong to both instances</li>
 * <li>We should expect an already being archived message whether the PV is being archived by the active cluster member or not.</li>
 * <li>Ask to archive a PV that belongs to neither instance.</li>
 * <li>We should expect a Initial Sampling message.</li>
 * </ul>
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class InactiveClusterMemberArchivePVTest {
    private static Logger logger = LogManager.getLogger(InactiveClusterMemberArchivePVTest.class.getName());
    File persistenceFolder = new File(
            ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "InactiveClusterMemberArchivePVTest");
    private String pvNameToArchive1 = "UnitTestNoNamingConvention:inactive1";
    private String pvNameToArchive2 = "UnitTestNoNamingConvention:inactive2";
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
        {
            System.getProperties()
                .put(
                    JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME,
                    persistenceFolder.getPath() + File.separator + "testconfig_appliance0.jdbm2");
            JDBM2Persistence persistenceLayer = new JDBM2Persistence();
            persistenceLayer.putTypeInfo(pvNameToArchive1, generatePVTypeInfo(pvNameToArchive1, "appliance0"));
        }
        {
            System.getProperties()
                .put(
                    JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME,
                    persistenceFolder.getPath() + File.separator + "testconfig_appliance1.jdbm2");
            JDBM2Persistence persistenceLayer = new JDBM2Persistence();
            persistenceLayer.putTypeInfo(pvNameToArchive2, generatePVTypeInfo(pvNameToArchive2, "appliance1"));
        }

        siocSetup.startSIOCWithDefaultDB();
        // Replace the testconfig_appliance0.jdbm2 with testconfig.jdbm2 as TomcatSetup adds this to the JDBM2 file name
        // to make the tests work in a cluster
        System.getProperties()
                .put(
                        JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME,
                        persistenceFolder.getPath() + File.separator + "testconfig.jdbm2");
        tomcatSetup.setUpClusterWithWebApps(this.getClass().getSimpleName(), 2);
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();

        FileUtils.deleteDirectory(persistenceFolder);
    }

    @Test
    public void testRequestForArchivingThatAlreadyExistsOnInactiveMember() throws Exception {
        checkPVStatus(pvNameToArchive1, "Appliance assigned");
        checkPVStatus(pvNameToArchive2, "Appliance assigned");
        tomcatSetup.shutDownAppliance("appliance1");
        checkPVStatus(pvNameToArchive1, "Appliance assigned");
        checkPVStatus(pvNameToArchive2, "Appliance Down");
        archivePV(pvNameToArchive1, "Appliance assigned");
        archivePV(pvNameToArchive2, "Appliance Down");
    }

    private void checkPVStatus(String pvName, String expectedPVStatus) throws Exception {
        String pvNameToArchive = pvName;
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        PVAccessUtil.waitForStatusChange(pvNameToArchive, expectedPVStatus, 10, mgmtURL, 15);
    }

    private void archivePV(String pvName, String expectedPVStatus) throws Exception {
        logger.info("Asking to archive pv " + pvName);
        String pvNameToArchive = pvName;
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, expectedPVStatus, 10, mgmtURL, 15);
    }

    private static PVTypeInfo generatePVTypeInfo(String pvName, String applianceIdentity) {
        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, false, 1);
        typeInfo.setUpperDisplayLimit(Double.valueOf(1.0));
        typeInfo.setLowerDisplayLimit(Double.valueOf(-1.0));
        typeInfo.setHasReducedDataSet(true);
        typeInfo.setComputedEventRate(1.0f);
        typeInfo.setComputedStorageRate(12.0f);
        typeInfo.setUserSpecifiedEventRate(1.0f);
        typeInfo.setApplianceIdentity(applianceIdentity);
        typeInfo.addArchiveField("HIHI");
        typeInfo.addArchiveField("LOLO");
        typeInfo.setPaused(false);
        return typeInfo;
    }
}
