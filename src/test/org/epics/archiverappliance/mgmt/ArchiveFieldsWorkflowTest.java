package org.epics.archiverappliance.mgmt;

import static org.junit.Assert.assertTrue;

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

/**
 * Test archiving of fields - this should include standard field like HIHI and also non-standard fields. 
 * @author mshankar
 *
 */
public class ArchiveFieldsWorkflowTest {
	private static Logger logger = Logger.getLogger(ArchiveFieldsWorkflowTest.class.getName());
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
		tomcatSetup.tearDown();
		driver.quit();
		siocSetup.stopSIOC();
	}

	@Test
	public void testArchiveFieldsPV() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String[] fieldsToArchive = new String[] {
				 "UnitTestNoNamingConvention:sine", 
				 "UnitTestNoNamingConvention:sine.EOFF", 
				 "UnitTestNoNamingConvention:sine.EGU",
				 "UnitTestNoNamingConvention:sine.ALST",
				 "UnitTestNoNamingConvention:sine.HOPR",
				 "UnitTestNoNamingConvention:sine.DESC",
				 "UnitTestNoNamingConvention:sine.YYZ"
		 };
		 pvstextarea.sendKeys(String.join("\n", fieldsToArchive));
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.debug("About to submit");
		 archiveButton.click();
		 Thread.sleep(5*60*1000);
		 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 for(int i = 0; i < fieldsToArchive.length; i++) { 
			 int rowWithInfo = i+1;
			 WebElement statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(" + rowWithInfo + ") td:nth-child(1)"));
			 String pvNameObtainedFromTable = statusPVName.getText();
			 assertTrue("PV Name is not " + fieldsToArchive[i] + "; instead we get " + pvNameObtainedFromTable, fieldsToArchive[i].equals(pvNameObtainedFromTable));
			 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(" + rowWithInfo + ") td:nth-child(2)"));
			 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
			 String expectedPVStatus = "Being archived";
			 if(fieldsToArchive[i].equals("UnitTestNoNamingConvention:sine.YYZ")) { 
				 assertTrue("Expecting PV archive status to NOT be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable + " for field " + fieldsToArchive[i], !expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));
			 } else { 
				 assertTrue("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable + " for field " + fieldsToArchive[i], expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));
			 }
		 }
	}
}
