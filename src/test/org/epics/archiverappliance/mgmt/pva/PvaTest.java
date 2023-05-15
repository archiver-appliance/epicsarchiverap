package org.epics.archiverappliance.mgmt.pva;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * A test suite for the basic pvAccess Archiver service management operations.
 * 
 * @author Kunal Shroff
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
@RunWith(Suite.class)
@Suite.SuiteClasses({ PvaSuiteTstGetAll.class, PvaSuiteTstMgmtServiceStartup.class, PvaSuiteTstGetApplianceInfo.class, PvaSuiteTstArchivePV.class })
public class PvaTest {

	private static final Logger logger = LogManager.getLogger(PvaTest.class.getName());

	static TomcatSetup tomcatSetup = new TomcatSetup();
	static SIOCSetup siocSetup = new SIOCSetup();

	@BeforeClass
	public static void setup() {
		logger.info("Set up for the PVATestSuite");
		try {
			siocSetup.startSIOCWithDefaultDB();
			tomcatSetup.setUpWebApps(PvaTest.class.getSimpleName());
			logger.info(ZonedDateTime.now(ZoneId.systemDefault()) + " Waiting three mins for the service setup to complete");
		} catch (Exception e) {
			logger.log(Level.FATAL, e.getMessage(), e);
		}
	}

	@AfterClass
	public static void tearDown() {
		logger.info("Tear Down for the PVATestSuite");
		try {
			tomcatSetup.tearDown();
			siocSetup.stopSIOC();
		} catch (Exception e) {
			logger.log(Level.FATAL, e.getMessage(), e);
		}
	}

}
