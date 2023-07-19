package org.epics.archiverappliance.mgmt.pauseresume;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.stream.Collectors;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
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
@Tag("integration")
@Tag("localEpics")
public class DeleteMultiplePVTest {
	private static Logger logger = LogManager.getLogger(DeleteMultiplePVTest.class.getName());
	private File persistenceFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "DeletePVTest");
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;

	@BeforeAll
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}

	@BeforeEach
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

	@AfterEach
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
		 Assertions.assertTrue(pvNameToArchive.equals(pvNameObtainedFromTable), "PV Name is not " + pvNameToArchive + "; instead we get " + pvNameObtainedFromTable);
		 String pvArchiveStatusObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(2)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 String expectedPVStatus = "Being archived\nBeing archived\nBeing archived\nBeing archived\nBeing archived";
		 Assertions.assertTrue(expectedPVStatus.equals(pvArchiveStatusObtainedFromTable), "Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable);

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
		 Assertions.assertTrue(pvNameToPause.equals(pvNameObtainedFromTable), "PV Name is not " + pvNameToPause + "; instead we get " + pvNameObtainedFromTable);
		 pvArchiveStatusObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(2)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 expectedPVStatus = "Paused\nPaused";
		 Assertions.assertTrue(expectedPVStatus.equals(pvArchiveStatusObtainedFromTable), "Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable);

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
		 Assertions.assertTrue(pvNameToDelete.equals(pvNameObtainedFromTable), "PV Name is not " + pvNameToDelete + "; instead we get " + pvNameObtainedFromTable);
		 pvArchiveStatusObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(2)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 expectedPVStatus = "Not being archived\nNot being archived";
		 Assertions.assertTrue(expectedPVStatus.equals(pvArchiveStatusObtainedFromTable), "Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable);

		 logger.info("Checking other PV are still there");
		 pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToCheck = "UnitTestNoNamingConvention:sine\nUnitTestNoNamingConvention:cosine\ntest_2";
		 pvstextarea.clear();
		 pvstextarea.sendKeys(pvNameToCheck);
		 checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 pvNameObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(1)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 Assertions.assertTrue(pvNameToCheck.equals(pvNameObtainedFromTable), "PV Name is not " + pvNameToCheck + "; instead we get " + pvNameObtainedFromTable);
		 pvArchiveStatusObtainedFromTable = driver.findElements(By.cssSelector("#archstatsdiv_table tr td:nth-child(2)")).stream().map(e->e.getText()).collect(Collectors.joining("\n"));
		 expectedPVStatus = "Being archived\nBeing archived\nBeing archived";
		 Assertions.assertTrue(expectedPVStatus.equals(pvArchiveStatusObtainedFromTable), "Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable);
	}
}
