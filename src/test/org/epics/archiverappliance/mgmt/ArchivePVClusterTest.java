package org.epics.archiverappliance.mgmt;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Test archiving using a simple cluster of tomcat servers.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class ArchivePVClusterTest {
    private static Logger logger = LogManager.getLogger(ArchivePVClusterTest.class.getName());
    File persistenceFolder = new File(
            ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "InactiveClusterMemberArchivePVTest");
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();

    @BeforeEach
    public void setUp() throws Exception {
        if (persistenceFolder.exists()) {
            FileUtils.deleteDirectory(persistenceFolder);
        }
        persistenceFolder.mkdirs();
        siocSetup.startSIOCWithDefaultDB();
        System.getProperties()
                .put(
                        ConfigService.ARCHAPPL_PERSISTENCE_LAYER,
                        "org.epics.archiverappliance.config.persistence.JDBM2Persistence");
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
    public void testSimpleArchivePV() throws Exception {
        String pvNameToArchive = "UnitTestNoNamingConvention:sine";
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
                mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);
        String aapl0 = checkInPersistence(pvNameToArchive, 0);
        String aapl1 = checkInPersistence(pvNameToArchive, 1);
        Assertions.assertTrue(
                aapl0.equals(aapl1),
                "Expecting the same appliance indetity in both typeinfos, instead it is " + aapl0
                        + " in cluster member 0 and " + aapl1 + " in cluster member 1");
    }

    private String checkInPersistence(String pvName, int clusterIndex) throws Exception {
        String persistenceFile = persistenceFolder.getPath() + File.separator + "testconfig.jdbm2";
        String persistenceFileForMember = persistenceFile.replace(".jdbm2", "_appliance" + clusterIndex + ".jdbm2");
        logger.info("Checking for pvtype info in persistence for cluster member " + clusterIndex + " in file "
                + persistenceFileForMember);
        System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFileForMember);
        JDBM2Persistence persistenceLayer = new JDBM2Persistence();
        PVTypeInfo typeInfo = persistenceLayer.getTypeInfo(pvName);
        Assertions.assertTrue(
                "Expecting the pv typeinfo to be in persistence for cluster member " + clusterIndex + typeInfo != null);
        return typeInfo.getApplianceIdentity();
    }
}
