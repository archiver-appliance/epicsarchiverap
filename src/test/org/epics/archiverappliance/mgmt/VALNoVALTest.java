package org.epics.archiverappliance.mgmt;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

/**
 * A common use case is where we archive the .VAL and ask for data either way.
 * This test archives two PVs; one with a .VAL and one without.
 * We then ask for data in all combinations and make sure we get said data. 
 * @author mshankar
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class VALNoVALTest {
	private static Logger logger = LogManager.getLogger(VALNoVALTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;
	String folderSTS = ConfigServiceForTests.getDefaultShortTermFolder() + File.separator + "reshardSTS";
	String folderMTS = ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "reshardMTS";
	String folderLTS = ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "reshardLTS";

	@BeforeClass
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}

	@Before
	public void setUp() throws Exception {
		System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER", folderSTS);
		System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", folderMTS);
		System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER", folderLTS);
		
		FileUtils.deleteDirectory(new File(folderSTS));
		FileUtils.deleteDirectory(new File(folderMTS));
		FileUtils.deleteDirectory(new File(folderLTS));

		siocSetup.startSIOCWithDefaultDB();
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		driver = new FirefoxDriver();
	}

	@After
	public void tearDown() throws Exception {
		driver.quit();
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();

		FileUtils.deleteDirectory(new File(folderSTS));
		FileUtils.deleteDirectory(new File(folderMTS));
		FileUtils.deleteDirectory(new File(folderLTS));
}

	@Test
	public void testVALNoVALTest() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToArchive1 = "UnitTestNoNamingConvention:sine";
		 pvstextarea.sendKeys(pvNameToArchive1);
		 pvstextarea.sendKeys(Keys.RETURN);
		 String pvNameToArchive2 = "UnitTestNoNamingConvention:cosine.VAL";
		 pvstextarea.sendKeys(pvNameToArchive2);
		 pvstextarea.sendKeys(Keys.RETURN);
		 
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.debug("About to submit");
		 archiveButton.click();
		 // We have to wait for a few minutes here as it does take a while for the workflow to complete.
		 Thread.sleep(5*60*1000);
		 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 checkArchiveStatus(pvNameToArchive1, 1);
		 checkArchiveStatus(pvNameToArchive2, 2);
		 Thread.sleep(60*1000);
		 testRetrievalCountOnServer(pvNameToArchive1, 55);
		 testRetrievalCountOnServer(pvNameToArchive1 + ".VAL", 55);
		 testRetrievalCountOnServer(pvNameToArchive1, 55);
		 testRetrievalCountOnServer(pvNameToArchive1 + ".VAL", 55);
	}

	/**
	 * Check to see that we get a Being archived status. We take the pvName and also the row in the table we expect the data.
	 * @param pvNameToArchive
	 * @param rowNum
	 */
	private void checkArchiveStatus(String pvNameToArchive, int rowNum) {
		{
			 WebElement statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(" + rowNum + ") td:nth-child(1)"));
			 String pvNameObtainedFromTable = statusPVName.getText();
			 assertTrue("PV Name is not " + pvNameToArchive + "; instead we get " + pvNameObtainedFromTable, pvNameToArchive.equals(pvNameObtainedFromTable));
			 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(" + rowNum + ") td:nth-child(2)"));
			 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
			 String expectedPVStatus = "Being archived";
			 assertTrue("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable, expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));
		 }
	}
	
	/**
	 * Get data for the PV from the server and make sure we have some data
	 * @param pvName
	 * @param expectedEventCount
	 * @throws IOException
	 */
	private void testRetrievalCountOnServer(String pvName, int expectedEventCount) throws IOException { 
		 RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		 Timestamp end = TimeUtils.plusDays(TimeUtils.now(), 3);
		 Timestamp start = TimeUtils.minusDays(end, 6);
		 try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName}, start, end, null)) {
			 long previousEpochSeconds = 0;
			 int eventCount = 0;

			 // We are making sure that the stream we get back has times in sequential order...
			 if(stream != null) {
				 for(Event e : stream) {
					 long actualSeconds = e.getEpochSeconds();
					 assertTrue(actualSeconds >= previousEpochSeconds);
					 previousEpochSeconds = actualSeconds;
					 eventCount++;
				 }
			 }

			 logger.info("Got " + eventCount + " event for pv " + pvName);
			 assertTrue("When asking for data using " + pvName + ", event count is incorrect We got " + eventCount + " and we were expecting at least " + expectedEventCount, eventCount > expectedEventCount);
		 }
	}

}
