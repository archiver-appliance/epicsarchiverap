package org.epics.archiverappliance.retrieval;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.PVCaPut;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

/**
 * This tests various post processors using sample data generated from within Matlab
 * We change an ai record using various values generated from within Matlab and then make various postprocessor calls and compare with values we got from Matlab
 * @author mshankar
 *
 */
public class VariousPostProcessorTest {
	private static Logger logger = Logger.getLogger(VariousPostProcessorTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;
	String pvName = "UnitTestNoNamingConvention:inactive1";

	// These values were generated from Matlab.
	double[] sampleValues = new double[] { 0.1349, -2.3312, 1.2507, 1.5754, -1.2929, 3.3818, 3.3783, 0.9247, 1.6546, 1.3493, 0.6266, 2.4516, -0.1766, 5.3664, 0.7272, 1.2279, 3.1335, 1.1186, 0.8087, -0.6647, 1.5888, -1.6724, 2.4286, 4.2471, -0.3836, 2.7160, 3.5080, -2.1875, -1.8819, 2.1423, 0.2002, 2.3800, 2.6312, 2.4238, 3.5805, 2.3372, 3.3817, -1.4049, 0.9604, 0.6866, -2.2082, 1.5146, -1.1129, 3.8303, -0.6102, 2.0575, 1.4386, -0.8438, -3.3413, 0.8816, -1.0213, 2.2289, 2.0155, 4.3849, 2.1826, -0.2872, 1.7607, -1.0182, 0.9610, 0.9036, 1.0001, 0.3643, 3.1900, -2.7480, 1.8564, 2.7913, 2.4619, 2.1557, 1.0806, 2.3542, 2.1378, 0.4887, 0.2451, 0.4082, -1.9503, 0.5320, 1.2369, 1.6296, 3.8870, 0.2981, 2.2465, 2.5981, 2.8818, -0.9842, 1.4241, 1.4758, -1.0155, -0.4841, 3.1646, 0.7370, 1.7798, 1.1760, -0.2709, -0.1191, 1.8873, -0.8998, 2.5624, 2.1379, -0.6434, 0.4688, Float.NaN };
	double expectedMean = 1.0959;
	double expectedMedian = 1.2324;
	double expectedStd = 1.7370;
	double expectedJitter = 1.5851;
	double expectedVariance = 3.0172;
	
	@Before
	public void setUp() throws Exception {
		FileUtils.deleteDirectory(new File(ConfigServiceForTests.getDefaultShortTermFolder() + File.separator + "UnitTestNoNamingConvention"));
		FileUtils.deleteDirectory(new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "UnitTestNoNamingConvention"));
		if(!System.getenv().containsKey("ARCHAPPL_SHORT_TERM_FOLDER") && !System.getProperties().containsKey("ARCHAPPL_SHORT_TERM_FOLDER")) { 
			System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER", ConfigServiceForTests.getDefaultShortTermFolder());
		}
		if(!System.getenv().containsKey("ARCHAPPL_MEDIUM_TERM_FOLDER") && !System.getProperties().containsKey("ARCHAPPL_MEDIUM_TERM_FOLDER")) { 
			System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", ConfigServiceForTests.getDefaultPBTestFolder());
		}
		siocSetup.startSIOCWithDefaultDB();
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		driver = new FirefoxDriver();
	}

	@After
	public void tearDown() throws Exception {
		driver.quit();
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();
		FileUtils.deleteDirectory(new File(ConfigServiceForTests.getDefaultShortTermFolder() + File.separator + "UnitTestNoNamingConvention"));
		FileUtils.deleteDirectory(new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "UnitTestNoNamingConvention"));
	}

	@Test
	public void testPostProcessors() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToArchive = pvName;
		 pvstextarea.sendKeys(pvNameToArchive);
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.debug("About to submit");
		 archiveButton.click();
		 // We have to wait for a few minutes here as it does take a while for the workflow to complete.
		 Thread.sleep(5*60*1000);
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
		 
		 // Change the values for the PV
		 logger.info("Changing pv " + pvName + " and generating " + sampleValues.length + " values");
		 new PVCaPut().caPutValues(pvName,sampleValues);

		 Thread.sleep(3*1000);
		 
		 testRetrievalCount(pvName, sampleValues);
		 testRetrievalCount("mean_432000(" + pvName + ")", new double[] {expectedMean});
		 testRetrievalCount("median_432000(" + pvName + ")", new double[] {expectedMedian});
		 testRetrievalCount("std_432000(" + pvName + ")", new double[] {expectedStd});
		 testRetrievalCount("jitter_432000(" + pvName + ")", new double[] {expectedJitter});
		 testRetrievalCount("variance_432000(" + pvName + ")", new double[] {expectedVariance});
	}
	
	
	
	private void testRetrievalCount(String pvName, double[] expectedValues) throws IOException {
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		Timestamp end = TimeUtils.plusDays(TimeUtils.now(), 1);
		Timestamp start = TimeUtils.minusDays(end, 2);
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName}, start, end, null)) {
			long previousEpochSeconds = 0;
			int eventCount = 0;

			// We are making sure that the stream we get back has times in sequential order...
			if(stream != null) {
				double previousValue = 0.0;
				for(Event e : stream) {
					long actualSeconds = e.getEpochSeconds();
					assertTrue(actualSeconds >= previousEpochSeconds);
					previousEpochSeconds = actualSeconds;
					if(eventCount == expectedValues.length) { 
						assertTrue("We are probably running this test on a day that generates two values for the bin size. In this case, we make sure the two values are the same", 
								e.getSampleValue().getValue().doubleValue() == previousValue);
						continue;
					}
					assertTrue("Expecting " + expectedValues.length + " got " + eventCount + " for pv " + pvName, eventCount < expectedValues.length);
					// We check for approx values only.
					if(Double.isNaN(expectedValues[eventCount])) { 
						assertTrue("Got " + e.getSampleValue().getValue().doubleValue() + " expecting " +  expectedValues[eventCount] + " at " + eventCount, 
								Double.isNaN(e.getSampleValue().getValue().doubleValue()));
					} else { 
						assertTrue("Got " + e.getSampleValue().getValue().doubleValue() + " expecting " +  expectedValues[eventCount] + " at " + eventCount, 
								Math.abs(Math.abs(e.getSampleValue().getValue().doubleValue()) -  Math.abs(expectedValues[eventCount])) < 0.001);
					}
					previousValue = e.getSampleValue().getValue().doubleValue();
					eventCount++;
				}
			}

			assertTrue("Expecting " + expectedValues.length + " got " + eventCount + " for pv " + pvName, eventCount == expectedValues.length);
		}
	}

}