package org.epics.archiverappliance.mgmt;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

/**
 * Test archiving using a simple cluster of tomcat servers.
 * @author mshankar
 *
 */
public class ArchivePVClusterTest {
	private static Logger logger = Logger.getLogger(ArchivePVClusterTest.class.getName());
	File persistenceFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "InactiveClusterMemberArchivePVTest");
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;

	@Before
	public void setUp() throws Exception {
		if(persistenceFolder.exists()) {
			FileUtils.deleteDirectory(persistenceFolder);
		}
		persistenceFolder.mkdirs();
		siocSetup.startSIOCWithDefaultDB();
		System.getProperties().put(ConfigService.ARCHAPPL_PERSISTENCE_LAYER, "org.epics.archiverappliance.config.persistence.JDBM2Persistence");
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFolder.getPath() + File.separator + "testconfig.jdbm2");
		tomcatSetup.setUpClusterWithWebApps(this.getClass().getSimpleName(), 2);
		driver = new FirefoxDriver();
	}

	@After
	public void tearDown() throws Exception {
		driver.quit();
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();
		FileUtils.deleteDirectory(persistenceFolder);
	}

	@Test
	public void testSimpleArchivePV() throws Exception {
		 int port = ConfigServiceForTests.RETRIEVAL_TEST_PORT+1;
		 driver.get("http://localhost:" + port + "/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToArchive = "UnitTestNoNamingConvention:sine";
		 pvstextarea.sendKeys(pvNameToArchive);
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.debug("About to submit");
		 archiveButton.click();
		 // We have to wait for a few minutes here as it does take a while for the workflow to complete.
		 Thread.sleep(4*60*1000);
		 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 WebElement statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(1)"));
		 String pvNameObtainedFromTable = statusPVName.getText();
		 assertTrue("PV Name is not " + pvNameToArchive + "; instead we get " + pvNameObtainedFromTable, pvNameToArchive.equals(pvNameObtainedFromTable));
		 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
		 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
		 String expectedPVStatus = "Being archived";
		 assertTrue("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable, expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));
		 String aapl0 = checkInPersistence(pvNameToArchive, 0);
		 String aapl1 = checkInPersistence(pvNameToArchive, 1);
		 assertTrue("Expecting the same appliance indetity in both typeinfos, instead it is " + aapl0 + " in cluster member 0 and " + aapl1 + " in cluster member 1", aapl0.equals(aapl1));
	}
	
	private String checkInPersistence(String pvName, int clusterIndex) throws Exception {
		String persistenceFile = persistenceFolder.getPath() + File.separator + "testconfig.jdbm2";
		String persistenceFileForMember = persistenceFile.replace(".jdbm2", "_appliance" + clusterIndex + ".jdbm2");
		logger.info("Checking for pvtype info in persistence for cluster member " + clusterIndex + " in file " + persistenceFileForMember);
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFileForMember);
		JDBM2Persistence persistenceLayer = new JDBM2Persistence();
		PVTypeInfo typeInfo = persistenceLayer.getTypeInfo(pvName);
		assertTrue("Expecting the pv typeinfo to be in persistence for cluster member " + clusterIndex + typeInfo != null);
		return typeInfo.getApplianceIdentity();
	}
}
