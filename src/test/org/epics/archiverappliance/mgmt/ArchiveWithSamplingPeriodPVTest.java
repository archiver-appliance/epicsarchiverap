package org.epics.archiverappliance.mgmt;

import static org.junit.Assert.assertTrue;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

@Tag("integration")
@Tag("localEpics")
public class ArchiveWithSamplingPeriodPVTest {
	private static Logger logger = LogManager.getLogger(ArchiveWithSamplingPeriodPVTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;

	@BeforeAll
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}
	@BeforeEach
	public void setUp() throws Exception {
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
	public void testArchiveWithSamplingPeriod() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToArchive = "UnitTestNoNamingConvention:sine";
		 pvstextarea.sendKeys(pvNameToArchive);
		 WebElement archiveWithPeriodButton = driver.findElement(By.id("archstatArchiveWithPeriod"));
		 logger.debug("About to start dialog");
		 archiveWithPeriodButton.click();
		 WebElement samplingPeriodTextBox = driver.findElement(By.id("pvDetailsSamplingPeriod"));
		 samplingPeriodTextBox.sendKeys("10"); // A sample every 10 seconds
		 WebElement dialogOkButton = driver.findElement(By.id("pvDetailsParamsOk"));
		 logger.debug("About to submit");
		 dialogOkButton.click();
		 // We have to wait for about 10 minutes here as it does take a while for the workflow to complete.
		 Thread.sleep(10*60*1000);
		 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 WebElement statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(1)"));
		 String pvNameObtainedFromTable = statusPVName.getText();
		 Assertions.assertTrue(pvNameToArchive.equals(pvNameObtainedFromTable), "PV Name is not " + pvNameToArchive + "; instead we get " + pvNameObtainedFromTable);
		 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
		 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
		 String expectedPVStatus = "Being archived";
		 Assertions.assertTrue(expectedPVStatus.equals(pvArchiveStatusObtainedFromTable), "Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable);
		 WebElement samplingRateTD = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(6)"));
		 String samplingRateStr = samplingRateTD.getText();
		 String expectedsamplingRateStr = "10.0";
		 Assertions.assertTrue(expectedsamplingRateStr.equals(samplingRateStr), "Expecting sampling rate to be " + expectedsamplingRateStr + "; instead it is " + samplingRateStr);
	}
}
