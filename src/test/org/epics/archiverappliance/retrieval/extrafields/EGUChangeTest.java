package org.epics.archiverappliance.retrieval.extrafields;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.HashMap;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.client.EpicsMessage;
import org.epics.archiverappliance.retrieval.client.GenMsgIterator;
import org.epics.archiverappliance.retrieval.client.InfoChangeHandler;
import org.epics.archiverappliance.retrieval.client.RawDataRetrieval;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
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
 * We want to make sure we capture changes in EGU and return them as part of the retrieval request.
 * We archive a PV, then get data (and thus the EGU).
 * We then caput the EGU and then fetch the data again...
 * @author mshankar
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class EGUChangeTest {
	private static Logger logger = LogManager.getLogger(EGUChangeTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;
	private String pvName = "UnitTestNoNamingConvention:sine";

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
		 
		 // We have now archived this PV, get some data and make sure the EGU is as expected.
		 checkEGU("apples");
		 SIOCSetup.caput(pvName + ".EGU", "oranges");
		 Thread.sleep(5*1000);
		 // Pause and resume the PV to reget the meta data
		 String pausePVURL = "http://localhost:17665/mgmt/bpl/pauseArchivingPV?pv=" + URLEncoder.encode(pvName, "UTF-8");
		 JSONObject pauseStatus = GetUrlContent.getURLContentAsJSONObject(pausePVURL);
		 assertTrue("Cannot pause PV", pauseStatus.containsKey("status") && pauseStatus.get("status").equals("ok"));
		 logger.info("Done pausing PV " + pvName);
		 Thread.sleep(5*1000);
		 String resumePVURL = "http://localhost:17665/mgmt/bpl/resumeArchivingPV?pv=" + URLEncoder.encode(pvName, "UTF-8");
		 JSONObject resumeStatus = GetUrlContent.getURLContentAsJSONObject(resumePVURL);
		 assertTrue("Cannot resume PV", resumeStatus.containsKey("status") && resumeStatus.get("status").equals("ok"));
		 logger.info("Done resuming PV " + pvName);

		 // Now check the EGU again...
		 Thread.sleep(1*60*1000);
		 checkEGU("oranges");		 
	}

	private void checkEGU(String expectedEGUValue) throws IOException {
		RawDataRetrieval rawDataRetrieval = new RawDataRetrieval("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		 Timestamp now = TimeUtils.now();
		 Timestamp start = TimeUtils.minusDays(now, 100);
		 Timestamp end = TimeUtils.plusDays(now, 10);
		 int eventCount = 0;

		 final HashMap<String, String> metaFields = new HashMap<String, String>(); 
		 // Make sure we get the EGU as part of a regular VAL call.
		 try(GenMsgIterator strm = rawDataRetrieval.getDataForPV(pvName, start, end, false, null)) { 
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

			 for(@SuppressWarnings("unused") EpicsMessage dbrevent : strm) {
				 eventCount++;
			 }
		 }

		 assertTrue("We should have gotten some data back in retrieval. We got " + eventCount, eventCount > 0);
		 assertTrue("The final value of EGU is " + metaFields.get("EGU") + ". We expected " + expectedEGUValue, expectedEGUValue.equals(metaFields.get("EGU")));
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
