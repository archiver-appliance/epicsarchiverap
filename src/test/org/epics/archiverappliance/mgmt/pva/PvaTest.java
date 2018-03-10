package org.epics.archiverappliance.mgmt.pva;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * A test suite for the basic pvAccess Archiver service management operations.
 * 
 * @author Kunal Shroff
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ PvaSuiteTstGetAll.class, PvaSuiteTstMgmtServiceStartup.class, PvaSuiteTstGetApplianceInfo.class, PvaSuiteTstArchivePV.class })
public class PvaTest {

	private static Logger logger = Logger.getLogger(PvaTest.class.getName());

	static TomcatSetup tomcatSetup = new TomcatSetup();
	static SIOCSetup siocSetup = new SIOCSetup();

	@BeforeClass
	public static void setup() {
		logger.info("Set up for the PVATestSuite");
		try {
			siocSetup.startSIOCWithDefaultDB();
			tomcatSetup.setUpWebApps(PvaTest.class.getSimpleName());
			logger.info(ZonedDateTime.now(ZoneId.systemDefault()) + " Waiting three mins for the service setup to complete");
			Thread.sleep(3 * 60 * 1000);
		} catch (Exception e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	@AfterClass
	public static void tearDown() {
		logger.info("Tear Down for the PVATestSuite");
		try {
			tomcatSetup.tearDown();
			siocSetup.stopSIOC();
		} catch (Exception e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		}
	}

}
