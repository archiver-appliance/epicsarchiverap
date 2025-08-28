package org.epics.archiverappliance.mgmt.pauseresume;

import static edu.stanford.slac.archiverappliance.plain.pb.PBPlainFileHandler.PB_PLUGIN_IDENTIFIER;

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
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Create a paused PV in persistence; start the appserver and make sure we can resume
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class ResumePVAfterRestartTest {
    private static Logger logger = LogManager.getLogger(ResumePVAfterRestartTest.class.getName());
    private File persistenceFolder =
            new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "DeletePVTest");
    private String pvNameToArchive = "UnitTestNoNamingConvention:sine";
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
                        persistenceFolder.getPath() + File.separator + "testconfig_appliance0.jdbm2");
        JDBM2Persistence persistenceLayer = new JDBM2Persistence();
        persistenceLayer.putTypeInfo(pvNameToArchive, generatePVTypeInfo(pvNameToArchive, "appliance0"));

        siocSetup.startSIOCWithDefaultDB();
        // Replace the testconfig_appliance0.jdbm2 with testconfig.jdbm2 as TomcatSetup adds this to the JDBM2 file name
        // to make the tests work in a cluster
        System.getProperties()
                .put(
                        JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME,
                        persistenceFolder.getPath() + File.separator + "testconfig.jdbm2");
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    private void assertPVDetails(List<Map<String, String>> pvDetails, String detailName, String expectedValue) {
        for (Map<String, String> pvDetail : pvDetails) {
            if (pvDetail.get("name").equals(detailName)) {
                Assertions.assertEquals(
                        pvDetail.get("value"),
                        expectedValue,
                        "Expecting " + expectedValue + " for detail " + detailName + " instead we got "
                                + pvDetail.get("value"));
                return;
            }
        }
        Assertions.assertTrue(false, "Did not find detail " + detailName + " in getPVDetails");
    }

    @Test
    public void testResumePVAfterRestart() throws Exception {
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Paused", 10, mgmtURL, 15);
        GetUrlContent.getURLContentWithQueryParameters(
                mgmtURL + "resumeArchivingPV", Map.of("pv", pvNameToArchive), false);
        Thread.sleep(2 * 1000);
        logger.debug("Resuming PV");
        @SuppressWarnings("unchecked")
        List<Map<String, String>> pvDetails =
                (List<Map<String, String>>) GetUrlContent.getURLContentWithQueryParametersAsJSONArray(
                        mgmtURL + "getPVDetails", Map.of("pv", pvNameToArchive), false);
        assertPVDetails(pvDetails, "Is this PV paused:", "No");
        assertPVDetails(pvDetails, "Is this PV currently connected?", "yes");
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
        typeInfo.setSamplingPeriod(1.0f);
        typeInfo.setSamplingMethod(SamplingMethod.MONITOR);
        typeInfo.setPaused(true);
        typeInfo.setDataStores(new String[] {
            PB_PLUGIN_IDENTIFIER
                    + "://localhost?name=STS&rootFolder=${ARCHAPPL_SHORT_TERM_FOLDER}&partitionGranularity=PARTITION_HOUR",
            PB_PLUGIN_IDENTIFIER
                    + "://localhost?name=MTS&rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&partitionGranularity=PARTITION_DAY",
            PB_PLUGIN_IDENTIFIER
                    + "://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR"
        });
        return typeInfo;
    }
}
