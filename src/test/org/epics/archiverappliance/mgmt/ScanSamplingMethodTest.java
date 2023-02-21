package org.epics.archiverappliance.mgmt;

import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.PVCaPut;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.retrieval.client.RetrievalEventProcessor;
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
import org.openqa.selenium.support.ui.Select;

/**
 * Test the SCAN sampling method. These are the test cases
 * <ol>
 * <li>PVs changing at 10Hz/1Hz/0.1Hz; we archive at SCAN/1.0. We should get a sample every second.</li>
 * <li>Second test is to change a PV rapidly and then stop. Wait a bit and then get the data; we should get the final value that was set and not something that was set earlier. 
 * </ol>
 * @author mshankar
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class ScanSamplingMethodTest {
	private static Logger logger = Logger.getLogger(ScanSamplingMethodTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;

	@BeforeClass
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}
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
	public void testScanPV() throws Exception {
		String[] pvNames = new String[] {"ArchUnitTest:counter10Hz", "ArchUnitTest:counter1Hz", "ArchUnitTest:counter1By10thHz", "ArchUnitTest:manual"};

		int port = ConfigServiceForTests.RETRIEVAL_TEST_PORT;
		driver.get("http://localhost:" + port + "/mgmt/ui/index.html");
		WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		for(String pvName : pvNames) { 
			pvstextarea.sendKeys(pvName);
			pvstextarea.sendKeys(Keys.RETURN);
		}
		WebElement archiveButton = driver.findElement(By.id("archstatArchiveWithPeriod"));
		logger.debug("About to submit");
		archiveButton.click();
		Thread.sleep(5000);
		Select samplingMethodButton = new Select(driver.findElement(By.id("pvDetailsSamplingMethod")));
		samplingMethodButton.selectByVisibleText("Scan");
		WebElement samplingPeriod = driver.findElement(By.id("pvDetailsSamplingPeriod"));
		samplingPeriod.sendKeys("1.0");
		WebElement okButton = driver.findElement(By.id("pvDetailsParamsOk"));
		okButton.click();
		checkIfAllPVsAreArchived(pvNames);

		Thread.sleep(60*1000);
		double lastValue = rapidlyChangeManualPV(pvNames[3]);
		Thread.sleep(20*1000);

		testDataRetrieval(pvNames[0], 60, 1100,  false);
		testDataRetrieval(pvNames[1], 60, 1100,  true);
		testDataRetrieval(pvNames[2], 8,  10100, true);
		
		testLastSampleOfManualPV(pvNames[3], lastValue);
		
	}

	private void testDataRetrieval(String pvName, int expectedCount, long expectedGapBetweenSamples, boolean consecutiveValuesExpected) {
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		Timestamp end = TimeUtils.now();
		Timestamp start = TimeUtils.minusHours(end, 1);

		EventStream stream = null;
		try {
			stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, start, end, new RetrievalEventProcessor() {
				@Override
				public void newPVOnStream(EventStreamDesc desc) {
					logger.info("Getting data for PV " + desc.getPvName());
				}
			});

			// We are making sure that the stream we get back has a sample every second.
			long eventCount = 0;
			if(stream != null) {
				long previousEventMillis = -1;
				long previousValue = -1;
				for(Event e : stream) {
					long currentMillis = e.getEventTimeStamp().getTime();
					assertTrue("Gap between samples " + (currentMillis - previousEventMillis) + " is more than expected " + expectedGapBetweenSamples + " for PV " + pvName, previousEventMillis == -1 || ((currentMillis - previousEventMillis) <= expectedGapBetweenSamples));
					previousEventMillis = currentMillis;
					eventCount++;
					if(consecutiveValuesExpected) { 
						long currentValue = e.getSampleValue().getValue().longValue();
						assertTrue("We expect not to miss any value. Current " + currentValue + " and previous " + previousValue + " for pv " + pvName, 
								previousValue == -1 || (currentValue == (previousValue + 1)));
						previousValue = currentValue;
					}
				}
			}
			assertTrue("Event count is not what we expect. We got " + eventCount + " and we expected at least " + expectedCount + " for pv " + pvName, eventCount >= expectedCount);
		} finally {
			if(stream != null) try { stream.close(); stream = null; } catch(Throwable t) { }
		}
	}	
	
	private boolean checkIfAllPVsAreArchived(String[] pvNames) throws Exception { 
		for(int i = 0; i < 3600; i++) { 
			Thread.sleep(8000);
			WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
			checkStatusButton.click();
			Thread.sleep(2*1000);
			boolean allArchived = true;
			for(int p = 0; p < pvNames.length; p++) { 
				WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(" + (p+1) + ") td:nth-child(2)"));
				String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
				String expectedPVStatus = "Being archived";
				if(!pvArchiveStatusObtainedFromTable.equals(expectedPVStatus)) { 
					allArchived = false;
					break;
				}
			}
			if (allArchived) { 
				return true;
			}
		}
		return false;
	}
	
	private double rapidlyChangeManualPV(String pvName) throws Exception {
		double lastValue = -1000.0;
		new PVCaPut().caPut(pvName, 1.0);
		Thread.sleep(2000);
		new PVCaPut().caPutValues(pvName, new double[] { 1.1, 1.2, 1.3, lastValue}, 100);
		return lastValue;
	}
	
	
	private void testLastSampleOfManualPV(String pvName, double lastValue) {
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		Timestamp end = TimeUtils.plusHours(TimeUtils.now(), 10);
		Timestamp start = TimeUtils.minusHours(end, 10);

		EventStream stream = null;
		try {
			stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, start, end, new RetrievalEventProcessor() {
				@Override
				public void newPVOnStream(EventStreamDesc desc) {
					logger.info("Getting data for PV " + desc.getPvName());
				}
			});

			// We want to make sure that the last sample we get is what we expect.
			long eventCount = 0;
			if(stream != null) {
				double eventValue = 0.0;
				for(Event e : stream) {
					eventValue = e.getSampleValue().getValue().doubleValue();
					eventCount++;
				}
				assertTrue("We expected the last value to be " + lastValue + ". Instead it is " + eventValue, eventValue == lastValue);
			}
			assertTrue("Event count is not what we expect. We got " + eventCount + " and we expected at least one event", eventCount >= 1);
		} finally {
			if(stream != null) try { stream.close(); stream = null; } catch(Throwable t) { }
		}
	}	

}