package org.epics.archiverappliance.mgmt;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

public class ChangeArchivalParamsTest {
	private static Logger logger = Logger.getLogger(ChangeArchivalParamsTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;

	@Before
	public void setUp() throws Exception {
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
	public void testChangeArchivalParams() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToArchive = "UnitTestNoNamingConvention:sine";
		 pvstextarea.sendKeys(pvNameToArchive);
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.info("About to submit");
		 archiveButton.click();
		 Thread.sleep(10*1000);
		 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 // We have to wait for a few minutes here as it does take a while for the workflow to complete.
		 Thread.sleep(5*60*1000);
		 driver.get("http://localhost:17665/mgmt/ui/pvdetails.html?pv=" + pvNameToArchive);
		 Thread.sleep(2*1000);
		 WebElement changePVParams = driver.findElement(By.id("pvDetailsParamChange"));
		 logger.info("About to start dialog");
		 changePVParams.click();
		 WebElement samplingPeriodTextBox = driver.findElement(By.id("pvDetailsSamplingPeriod"));
		 samplingPeriodTextBox.clear();
		 samplingPeriodTextBox.sendKeys("11"); // A sample every 11 seconds
		 WebElement dialogOkButton = driver.findElement(By.id("pvDetailsParamsOk"));
		 logger.info("About to submit");
		 dialogOkButton.click();
		 Thread.sleep(10*1000);
		 WebElement pvDetailsTable = driver.findElement(By.id("pvDetailsTable"));
		 List<WebElement> pvDetailsTableRows = pvDetailsTable.findElements(By.cssSelector("tbody tr"));
		 for(WebElement pvDetailsTableRow : pvDetailsTableRows) {
			 WebElement pvDetailsTableFirstCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(1)"));
			 if(pvDetailsTableFirstCol.getText().contains("Sampling period")) {
				 WebElement pvDetailsTableSecondCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(2)"));
				 String obtainedSamplingPeriod = pvDetailsTableSecondCol.getText();
				 String expectedSamplingPeriod = "11.0";
				 assertTrue("Expecting sampling period to be " + expectedSamplingPeriod + "; instead it is " + obtainedSamplingPeriod, expectedSamplingPeriod.equals(obtainedSamplingPeriod));
				 break;
			 }
		 }
	}
}
