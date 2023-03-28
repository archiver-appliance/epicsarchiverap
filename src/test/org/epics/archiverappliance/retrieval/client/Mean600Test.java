package org.epics.archiverappliance.retrieval.client;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;

/**
 * Testing mean_600 and making sure we get expected results in the client.
 * Generate a few years worth of data for a PV and then get the results using mean_600 and raw results.
 * 
 * @author mshankar
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class Mean600Test {
	private static Logger logger = Logger.getLogger(Mean600Test.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;
	private String pvName = "UnitTestNoNamingConvention:inactive1";
	private short currentYear = TimeUtils.getCurrentYear();
	private File ltsFolder = new File(System.getenv("ARCHAPPL_LONG_TERM_FOLDER") + "/UnitTestNoNamingConvention");
	StoragePlugin storageplugin;
	private ConfigServiceForTests configService;

	@BeforeClass
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		storageplugin = StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR", configService);
		siocSetup.startSIOCWithDefaultDB();
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		driver = new FirefoxDriver();
		
		if(ltsFolder.exists()) { 
			FileUtils.deleteDirectory(ltsFolder);
		}
		try(BasicContext context = new BasicContext()) { 
			for(short y = 3; y >= 0; y--) { 
				short year = (short)(currentYear - y);
				for(int day = 0; day < 366; day++) {
					ArrayListEventStream testData = new ArrayListEventStream(24*60*60, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));
					int startofdayinseconds = day*24*60*60;
					for(int secondintoday = 0; secondintoday < 24*60*60; secondintoday++) {
						// The value should be the secondsIntoYear integer divided by 600.
						testData.add(new SimulationEvent(startofdayinseconds + secondintoday, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>((double) (((int)(startofdayinseconds + secondintoday)/600)))));
					}
					storageplugin.appendData(context, pvName, testData);
				}
			}
		}
	}

	@After
	public void tearDown() throws Exception {
		driver.quit();
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();

		if(ltsFolder.exists()) { 
			FileUtils.deleteDirectory(ltsFolder);
		}
	}

	@Test
	public void testSimpleArchivePV() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 pvstextarea.sendKeys(pvName);
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.debug("About to submit");
		 archiveButton.click();
		 // We have to wait for a few minutes here here as it does take a while for the workflow to complete.
		 Thread.sleep(5*60*1000);
		 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 WebElement statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(1)"));
		 String pvNameObtainedFromTable = statusPVName.getText();
		 assertTrue("PV Name is not " + pvName + "; instead we get " + pvNameObtainedFromTable, pvName.equals(pvNameObtainedFromTable));
		 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
		 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
		 String expectedPVStatus = "Being archived";
		 assertTrue("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable, expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));
		 Thread.sleep(1*60*1000);
		 
		 // We have now archived this PV, get some data and validate we got the expected number of events
		 checkRetrieval(pvName, 366*86400);
		 checkRetrieval("mean_600(" + pvName + ")", 366*24*6);
	}

	private void checkRetrieval(String retrievalPVName, int expectedAtLeastEvents) throws IOException {
		long startTimeMillis = System.currentTimeMillis();
		RawDataRetrieval rawDataRetrieval = new RawDataRetrieval("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		 Timestamp now = TimeUtils.now();
		 Timestamp start = TimeUtils.minusDays(now, 366);
		 Timestamp end = now;
		 int eventCount = 0;

		 final HashMap<String, String> metaFields = new HashMap<String, String>(); 
		 // Make sure we get the EGU as part of a regular VAL call.
		 try(GenMsgIterator strm = rawDataRetrieval.getDataForPV(retrievalPVName, start, end, false, null)) { 
			 PayloadInfo info = null;
			 assertTrue("We should get some data, we are getting a null stream back", strm != null); 
			 info =  strm.getPayLoadInfo();
			 assertTrue("Stream has no payload info", info != null);
			 mergeHeaders(info, metaFields);
			 strm.onInfoChange(new InfoChangeHandler() {
				 @Override
				 public void handleInfoChange(PayloadInfo info) {
					 mergeHeaders(info, metaFields);
				 }
			 });

			 long endTimeMillis =  System.currentTimeMillis();

			 
			 for(@SuppressWarnings("unused") EpicsMessage dbrevent : strm) {
				 eventCount++;
			 }
			 
			 logger.info("Retrival for " + retrievalPVName + "=" + (endTimeMillis - startTimeMillis) + "(ms)");
		 }

		 assertTrue("Expecting " + expectedAtLeastEvents + "events. We got " + eventCount, eventCount >= expectedAtLeastEvents);
	}
	
	private static void mergeHeaders(PayloadInfo info, HashMap<String, String> headers) { 
		 int headerCount = info.getHeadersCount();
		 for(int i = 0; i < headerCount; i++) { 
			 String headerName = info.getHeaders(i).getName();
			 String headerValue = info.getHeaders(i).getVal();
			 logger.info("Adding header " + headerName + " = " + headerValue);
			 headers.put(headerName, headerValue);
		 }
	}		
}
