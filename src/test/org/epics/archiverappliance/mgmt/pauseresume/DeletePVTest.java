package org.epics.archiverappliance.mgmt.pauseresume;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

/**
 * Start an appserver with persistence; start archiving a PV and then pause it and delete it
 * Make sure that the typeinfo disappears from persistence. 
 * @author mshankar
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class DeletePVTest {
	private static Logger logger = LogManager.getLogger(DeletePVTest.class.getName());
	private File persistenceFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "DeletePVTest");
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;

	@BeforeClass
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}

	@Before
	public void setUp() throws Exception {
		if(persistenceFolder.exists()) {
			FileUtils.deleteDirectory(persistenceFolder);
		}
		persistenceFolder.mkdirs();
		System.getProperties().put(ConfigService.ARCHAPPL_PERSISTENCE_LAYER, "org.epics.archiverappliance.config.persistence.JDBM2Persistence");
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFolder.getPath() + File.separator + "testconfig.jdbm2");

		siocSetup.startSIOCWithDefaultDB();
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		driver = new FirefoxDriver();
	}

	@After
	public void tearDown() throws Exception {
		driver.quit();
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();
	}

	@Test
	public void testSimpleDeletePV() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToArchive = "UnitTestNoNamingConvention:sine";
		 pvstextarea.sendKeys(pvNameToArchive);
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.debug("About to submit");
		 archiveButton.click();
		 // We have to wait for about 10 minutes here as it does take a while for the workflow to complete.
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

		 logger.info("We are now archiving the PV; let's go into the details page; pause and delete");
		 driver.get("http://localhost:17665/mgmt/ui/pvdetails.html?pv=" + pvNameToArchive);
		 { 
			 Thread.sleep(2*1000);
			 WebElement pauseArchivingButn = driver.findElement(By.id("pvDetailsPauseArchiving"));
			 logger.info("Clicking on the button to pause archiving the PV");
			 pauseArchivingButn.click();
			 Thread.sleep(10*1000);
			 WebElement pvDetailsTable = driver.findElement(By.id("pvDetailsTable"));
			 List<WebElement> pvDetailsTableRows = pvDetailsTable.findElements(By.cssSelector("tbody tr"));
			 for(WebElement pvDetailsTableRow : pvDetailsTableRows) {
				 WebElement pvDetailsTableFirstCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(1)"));
				 if(pvDetailsTableFirstCol.getText().contains("Is this PV paused:")) {
					 WebElement pvDetailsTableSecondCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(2)"));
					 String obtainedPauseStatus = pvDetailsTableSecondCol.getText();
					 String expectedPauseStatus = "Yes";
					 assertTrue("Expecting paused status to be " + expectedPauseStatus + "; instead it is " + obtainedPauseStatus, expectedPauseStatus.equals(obtainedPauseStatus));
					 break;
				 }
			 }
		 }
		 { 
			 Thread.sleep(2*1000);
			 WebElement resumeArchivingButn = driver.findElement(By.id("pvDetailsResumeArchiving"));
			 logger.info("Clicking on the button to resume archiving the PV");
			 resumeArchivingButn.click();
			 Thread.sleep(10*1000);
			 WebElement pvDetailsTable = driver.findElement(By.id("pvDetailsTable"));
			 List<WebElement> pvDetailsTableRows = pvDetailsTable.findElements(By.cssSelector("tbody tr"));
			 for(WebElement pvDetailsTableRow : pvDetailsTableRows) {
				 WebElement pvDetailsTableFirstCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(1)"));
				 if(pvDetailsTableFirstCol.getText().contains("Is this PV paused:")) {
					 WebElement pvDetailsTableSecondCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(2)"));
					 String obtainedPauseStatus = pvDetailsTableSecondCol.getText();
					 String expectedPauseStatus = "No";
					 assertTrue("Expecting paused status to be " + expectedPauseStatus + "; instead it is " + obtainedPauseStatus, expectedPauseStatus.equals(obtainedPauseStatus));
					 break;
				 }
			 }
		 }
		 { 
			 Thread.sleep(2*1000);
			 WebElement pauseArchivingButn = driver.findElement(By.id("pvDetailsPauseArchiving"));
			 logger.info("Clicking on the button to pause archiving the PV");
			 pauseArchivingButn.click();
			 Thread.sleep(10*1000);
			 WebElement pvDetailsTable = driver.findElement(By.id("pvDetailsTable"));
			 List<WebElement> pvDetailsTableRows = pvDetailsTable.findElements(By.cssSelector("tbody tr"));
			 for(WebElement pvDetailsTableRow : pvDetailsTableRows) {
				 WebElement pvDetailsTableFirstCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(1)"));
				 if(pvDetailsTableFirstCol.getText().contains("Is this PV paused:")) {
					 WebElement pvDetailsTableSecondCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(2)"));
					 String obtainedPauseStatus = pvDetailsTableSecondCol.getText();
					 String expectedPauseStatus = "Yes";
					 assertTrue("Expecting paused status to be " + expectedPauseStatus + "; instead it is " + obtainedPauseStatus, expectedPauseStatus.equals(obtainedPauseStatus));
					 break;
				 }
			 }
		 }
		 { 
			 Thread.sleep(2*1000);
			 WebElement deletePVButn = driver.findElement(By.id("pvDetailsStopArchiving"));
			 logger.info("Clicking on the button to delete/stop archiving the PV");
			 deletePVButn.click();
			 Thread.sleep(2*1000);
			 WebElement dialogOkButton = driver.findElement(By.id("pvStopArchivingOk"));
			 logger.info("About to submit");
			 dialogOkButton.click();
			 Thread.sleep(10*1000);
		 }
		 { 
			 driver.get("http://localhost:17665/mgmt/ui/index.html");
			 checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
			 checkStatusButton.click();
			 Thread.sleep(2*1000);
			 statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(1)"));
			 pvNameObtainedFromTable = statusPVName.getText();
			 assertTrue("PV Name is not " + pvNameToArchive + "; instead we get " + pvNameObtainedFromTable, pvNameToArchive.equals(pvNameObtainedFromTable));
			 statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
			 pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
			 expectedPVStatus = "Not being archived";
			 assertTrue("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable, expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));
		 }
	}
}
