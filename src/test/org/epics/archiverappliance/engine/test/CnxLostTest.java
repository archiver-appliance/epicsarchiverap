package org.epics.archiverappliance.engine.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.epics.archiverappliance.mgmt.ArchiveWorkflowCompleted;
import org.epics.archiverappliance.retrieval.client.EpicsMessage;
import org.epics.archiverappliance.retrieval.client.GenMsgIterator;
import org.epics.archiverappliance.retrieval.client.RawDataRetrieval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

/**
 * Start an appserver with persistence; start archiving a PV; then start and restart the SIOC and make sure we get the expected cnxlost headers.
 * @author mshankar
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class CnxLostTest {
	private static Logger logger = Logger.getLogger(CnxLostTest.class.getName());
	private File persistenceFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "CnxLostTest");
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;

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
		if(persistenceFolder.exists()) {
			FileUtils.deleteDirectory(persistenceFolder);
		}
		
		File mtsFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "UnitTestNoNamingConvention");
		if(mtsFolder.exists()) { 
			FileUtils.deleteDirectory(mtsFolder);
		}
	}
	
	enum ConnectionLossType { 
		STARTUP_OR_PAUSE_RESUME,
		IOC_RESTART,
		NONE
	}
	
	class ExpectedEventType { 
		ConnectionLossType lossType;
		int numberOfEvents;
		public ExpectedEventType(ConnectionLossType lossType, int numberOfEvents) {
			this.lossType = lossType;
			this.numberOfEvents = numberOfEvents;
		}
	}

	@Test
	public void testConnectionLossHeaders() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToArchive = "UnitTestNoNamingConvention:inactive1";
		 pvstextarea.sendKeys(pvNameToArchive);
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.debug("About to submit");
		 archiveButton.click();
		 ArchiveWorkflowCompleted.isArchiveRequestComplete(pvNameToArchive);
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
		 
		 // UnitTestNoNamingConvention:inactive1 is SCAN passive without autosave so it should have an invalid timestamp.
		 // We caput something to generate a valid timestamp..
		 siocSetup.caput(pvNameToArchive, "1.0");
		 Thread.sleep(60*1000);
		 siocSetup.caput(pvNameToArchive, "2.0");
		 Thread.sleep(60*1000);
		 
		 checkRetrieval(pvNameToArchive, new ExpectedEventType[] { 
			new ExpectedEventType(ConnectionLossType.STARTUP_OR_PAUSE_RESUME, 1),
			new ExpectedEventType(ConnectionLossType.NONE, 1)
		 });

		 logger.info("We are now archiving the PV; let's go into the details page; pause and resume");
		 driver.get("http://localhost:17665/mgmt/ui/pvdetails.html?pv=" + pvNameToArchive);
		 { 
			 Thread.sleep(20*1000);
			 WebElement pauseArchivingButn = driver.findElement(By.id("pvDetailsPauseArchiving"));
			 logger.info("Clicking on the button to pause archiving the PV");
			 pauseArchivingButn.click();
			 Thread.sleep(20*1000);
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
		 siocSetup.caput(pvNameToArchive, "3.0"); // We are paused; so we should miss this event
		 Thread.sleep(60*1000);
		 siocSetup.caput(pvNameToArchive, "4.0");
		 Thread.sleep(60*1000);
		 { 
			 Thread.sleep(20*1000);
			 WebElement resumeArchivingButn = driver.findElement(By.id("pvDetailsResumeArchiving"));
			 logger.info("Clicking on the button to resume archiving the PV");
			 resumeArchivingButn.click();
			 Thread.sleep(20*1000);
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
		 
		 checkRetrieval(pvNameToArchive, new ExpectedEventType[] { 
			new ExpectedEventType(ConnectionLossType.STARTUP_OR_PAUSE_RESUME, 1),
			new ExpectedEventType(ConnectionLossType.NONE, 1),
			new ExpectedEventType(ConnectionLossType.STARTUP_OR_PAUSE_RESUME, 1)
		 });

		 siocSetup.stopSIOC();
		 Thread.sleep(20*1000);
		 
		 siocSetup = new SIOCSetup();
		 siocSetup.startSIOCWithDefaultDB();
		 Thread.sleep(20*1000);

		 siocSetup.caput(pvNameToArchive, "5.0");
		 Thread.sleep(60*1000);
		 siocSetup.caput(pvNameToArchive, "6.0");
		 Thread.sleep(60*1000);

		 checkRetrieval(pvNameToArchive, new ExpectedEventType[] { 
			new ExpectedEventType(ConnectionLossType.STARTUP_OR_PAUSE_RESUME, 1),
			new ExpectedEventType(ConnectionLossType.NONE, 1),
			new ExpectedEventType(ConnectionLossType.STARTUP_OR_PAUSE_RESUME, 1),
			new ExpectedEventType(ConnectionLossType.IOC_RESTART, 1),
			new ExpectedEventType(ConnectionLossType.NONE, 1),
		 });
		 
	}

	
	private void checkRetrieval(String retrievalPVName, ExpectedEventType[] expectedEvents) throws IOException {
		RawDataRetrieval rawDataRetrieval = new RawDataRetrieval("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		Timestamp now = TimeUtils.now();
		Timestamp start = TimeUtils.minusDays(now, 366);
		Timestamp end = now;

		LinkedList<EpicsMessage> retrievedData = new LinkedList<EpicsMessage>();
		try(GenMsgIterator strm = rawDataRetrieval.getDataForPV(retrievalPVName, start, end, false, null)) { 
			int eventCount = 0;
			assertTrue("We should get some data, we are getting a null stream back", strm != null);
				for(EpicsMessage dbrevent : strm) {
					logger.info("Adding event with value " + dbrevent.getNumberValue().doubleValue()
							+ " at time " + TimeUtils.convertToHumanReadableString(dbrevent.getTimestamp()));
					retrievedData.add(dbrevent);
					eventCount++;
				}
				assertTrue("Expecting at least one event. We got " + eventCount, eventCount >= 1);
		}
		int eventIndex = 0;
		for(ExpectedEventType expectedEvent : expectedEvents) {
			for(int i = 0; i < expectedEvent.numberOfEvents; i++) {
				assertTrue("Ran out of events at " + eventIndex + " processed " + i + " expecting " + (expectedEvent.numberOfEvents-i) + "more", !retrievedData.isEmpty());
				EpicsMessage message = retrievedData.poll();
				assertTrue("Expecting event at " + eventIndex + " to be of type " + expectedEvent.lossType, expectedEvent.lossType == determineConnectionLossType(message));
				eventIndex++;
			}
		}
	}

	private static ConnectionLossType determineConnectionLossType(EpicsMessage dbrevent) throws IOException { 
		ConnectionLossType retVal = ConnectionLossType.NONE;
		HashMap<String, String> extraFields = dbrevent.getFieldValues();
		if(!extraFields.keySet().contains("cnxlostepsecs")) { 
			retVal = ConnectionLossType.NONE;
		} else { 
			String connectionLostSecs = extraFields.get("cnxlostepsecs");
			if(Long.parseLong(connectionLostSecs) == 0) { 
				assertTrue("At least for now, we should have a startup field as well", extraFields.keySet().contains("startup"));
				retVal = ConnectionLossType.STARTUP_OR_PAUSE_RESUME;
			} else { 
				retVal = ConnectionLossType.IOC_RESTART;
			}
		}
		
		logger.info("Event with value " + dbrevent.getNumberValue().doubleValue() + " is of type " + retVal);
		return retVal;
	}		

}
