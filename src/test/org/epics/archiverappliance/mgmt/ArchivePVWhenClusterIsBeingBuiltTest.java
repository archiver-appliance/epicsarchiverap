package org.epics.archiverappliance.mgmt;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ArchiveTestUtils;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
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
 * Most sites run with some automation scripts these days that adds PVs to the archiver from IOC info files.
 * If these scripts happen to hit the cluster while the PVTypeInfo's are being loaded, we could potentially end up
 * with situations where a PV might be re-archived to a new cluster instance as a new PV while the instance
 * that was actually archiving it was still coming up. This can also happen in split brain situations.
 * While we cannot prevent this issue completely, we can minimize this a bit more by checking the database
 * to see if the PVTypeInfo for the PV already exists.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class ArchivePVWhenClusterIsBeingBuiltTest {
    private static Logger logger = LogManager.getLogger(ArchivePVWhenClusterIsBeingBuiltTest.class.getName());
    File persistenceFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder()
            + File.separator
            + ArchivePVWhenClusterIsBeingBuiltTest.class.getName());
    private String pvNameToArchive = "UnitTestNoNamingConvention:inactive1";
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

        FileUtils.deleteDirectory(persistenceFolder);
    }

    @Test
    public void testRequestForArchivingThatAlreadyExistsOnInactiveMember() throws Exception {
        // Make sure the PV is not being archived initially.
        checkPVStatus(pvNameToArchive, "Not being archived");
        logger.info("PV " + pvNameToArchive + " is initially not being archived.");

        // Let's first ask the archiver to archive the PV.
        archivePV(pvNameToArchive);
        logger.info("Asked to archive PV " + pvNameToArchive);
        checkPVStatus(pvNameToArchive, "Initial sampling");
        logger.info("PV " + pvNameToArchive + " is now in initial sampling.");

        // We then sneakily add the PV into the database behind the archiver's back.
        {
            JDBM2Persistence persistenceLayer = new JDBM2Persistence();
            persistenceLayer.putTypeInfo(pvNameToArchive, generatePVTypeInfo(pvNameToArchive, "appliance0"));
        }

        // The archive request should have aborted and we should get a "Not being archived" status.
        checkPVStatus(pvNameToArchive, "Not being archived");
        logger.info("PV " + pvNameToArchive + " is no longer being archived.");
    }

    private void checkPVStatus(String pvName, String expectedPVStatus) throws Exception {
        String pvNameToArchive = pvName;
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        ArchiveTestUtils.waitForStatusChange(pvNameToArchive, expectedPVStatus, 10, mgmtURL, 15);
    }

    private void archivePV(String pvName) throws Exception {
        logger.info("Asking to archive pv " + pvName);
        String pvNameToArchive = pvName;
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
                mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
    }

    private static PVTypeInfo generatePVTypeInfo(String pvName, String applianceIdentity) {
        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, false, 1);
        typeInfo.setUpperDisplayLimit(1.0);
        typeInfo.setLowerDisplayLimit(-1.0);
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
