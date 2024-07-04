package org.epics.archiverappliance.retrieval.extrafields;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import java.io.IOException;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;

/**
 * We want to make sure we capture changes in EGU and return them as part of the retrieval request.
 * We archive a PV, then get data (and thus the EGU).
 * We then caput the EGU and then fetch the data again...
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class EGUChangeTest {
	private static Logger logger = LogManager.getLogger(EGUChangeTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;
	private String pvName = "UnitTestNoNamingConvention:sine";

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
		 Assertions.assertTrue(pvName.equals(pvNameObtainedFromTable), "PV Name is not " + pvName + "; instead we get " + pvNameObtainedFromTable);
		 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
		 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
		 String expectedPVStatus = "Being archived";
		 Assertions.assertTrue(expectedPVStatus.equals(pvArchiveStatusObtainedFromTable), "Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable);
		 Thread.sleep(1*60*1000);
		 
		 // We have now archived this PV, get some data and make sure the EGU is as expected.
		 checkEGU("apples");
		 SIOCSetup.caput(pvName + ".EGU", "oranges");
		 Thread.sleep(5*1000);
		 // Pause and resume the PV to reget the meta data
		 String pausePVURL = "http://localhost:17665/mgmt/bpl/pauseArchivingPV?pv=" + URLEncoder.encode(pvName, "UTF-8");
		 JSONObject pauseStatus = GetUrlContent.getURLContentAsJSONObject(pausePVURL);
		 Assertions.assertTrue(pauseStatus.containsKey("status") && pauseStatus.get("status").equals("ok"), "Cannot pause PV");
		 logger.info("Done pausing PV " + pvName);
		 Thread.sleep(5*1000);
		 String resumePVURL = "http://localhost:17665/mgmt/bpl/resumeArchivingPV?pv=" + URLEncoder.encode(pvName, "UTF-8");
		 JSONObject resumeStatus = GetUrlContent.getURLContentAsJSONObject(resumePVURL);
		 Assertions.assertTrue(resumeStatus.containsKey("status") && resumeStatus.get("status").equals("ok"), "Cannot resume PV");
		 logger.info("Done resuming PV " + pvName);

		 // Now check the EGU again...
		 Thread.sleep(1*60*1000);
		 checkEGU("oranges");		 
	}

	private void checkEGU(String expectedEGUValue) throws IOException {
		RawDataRetrieval rawDataRetrieval = new RawDataRetrieval("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
        Instant now = TimeUtils.now();
        Instant start = TimeUtils.minusDays(now, 100);
        Instant end = TimeUtils.plusDays(now, 10);
		 int eventCount = 0;

		 final HashMap<String, String> metaFields = new HashMap<String, String>(); 
		 // Make sure we get the EGU as part of a regular VAL call.
        try (GenMsgIterator strm = rawDataRetrieval.getDataForPVs(Arrays.asList(pvName), TimeUtils.toSQLTimeStamp(start), TimeUtils.toSQLTimeStamp(end), false, null)) {
			 PayloadInfo info = null;
			 Assertions.assertTrue(strm != null, "We should get some data, we are getting a null stream back");
			 info =  strm.getPayLoadInfo();
			 Assertions.assertTrue(info != null, "Stream has no payload info");
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

		 Assertions.assertTrue(eventCount > 0, "We should have gotten some data back in retrieval. We got " + eventCount);
		 Assertions.assertTrue(expectedEGUValue.equals(metaFields.get("EGU")), "The final value of EGU is " + metaFields.get("EGU") + ". We expected " + expectedEGUValue);
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
