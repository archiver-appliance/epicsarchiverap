package org.epics.archiverappliance.mgmt.pauseresume;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

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
public class DeleteMultiplePVTest {
	private static Logger logger = LogManager.getLogger(DeleteMultiplePVTest.class.getName());
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
	public void testDeleteMultiplePV() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");

		 logger.info("Archiving 5 PV");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToArchive = "UnitTestNoNamingConvention:sine\nUnitTestNoNamingConvention:cosine\ntest_0\ntest_1\ntest_2";
		 pvstextarea.sendKeys(pvNameToArchive);
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 archiveButton.click();
		 // We have to wait for about 4 minutes here as it does take a while for the workflow to complete.
		 Thread.sleep(4*60*1000);
		 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 String pvNameObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(1)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 assertTrue("PV Name is not " + pvNameToArchive + "; instead we get " + pvNameObtainedFromTable, pvNameToArchive.equals(pvNameObtainedFromTable));
		 String pvArchiveStatusObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(2)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 String expectedPVStatus = "Being archived\nBeing archived\nBeing archived\nBeing archived\nBeing archived";
		 assertTrue("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable, expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));

		 logger.info("Pausing 2 PV");
		 pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToPause = "test_0\ntest_1";
		 pvstextarea.clear();
		 pvstextarea.sendKeys(pvNameToPause);
		 WebElement pauseButton = driver.findElement(By.id("pause"));
		 pauseButton.click();
		 Thread.sleep(2*1000);
		 checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 pvNameObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(1)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 assertTrue("PV Name is not " + pvNameToPause + "; instead we get " + pvNameObtainedFromTable, pvNameToPause.equals(pvNameObtainedFromTable));
		 pvArchiveStatusObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(2)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 expectedPVStatus = "Paused\nPaused";
		 assertTrue("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable, expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));

		 logger.info("Deleting 2 PV");
		 pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToDelete = "test_0\ntest_1";
		 pvstextarea.clear();
		 pvstextarea.sendKeys(pvNameToDelete);
		 WebElement deleteButton = driver.findElement(By.id("delete"));
		 deleteButton.click();
		 Thread.sleep(2*1000);
		 checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 pvNameObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(1)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 assertTrue("PV Name is not " + pvNameToDelete + "; instead we get " + pvNameObtainedFromTable, pvNameToDelete.equals(pvNameObtainedFromTable));
		 pvArchiveStatusObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(2)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 expectedPVStatus = "Not being archived\nNot being archived";
		 assertTrue("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable, expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));

		 logger.info("Checking other PV are still there");
		 pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToCheck = "UnitTestNoNamingConvention:sine\nUnitTestNoNamingConvention:cosine\ntest_2";
		 pvstextarea.clear();
		 pvstextarea.sendKeys(pvNameToCheck);
		 checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 pvNameObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(1)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 assertTrue("PV Name is not " + pvNameToCheck + "; instead we get " + pvNameObtainedFromTable, pvNameToCheck.equals(pvNameObtainedFromTable));
		 pvArchiveStatusObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(2)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 expectedPVStatus = "Being archived\nBeing archived\nBeing archived";
		 assertTrue("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable, expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));
	}
}
