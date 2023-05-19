package org.epics.archiverappliance.mgmt;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import io.github.bonigarcia.wdm.WebDriverManager;
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
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

/**
 * This relates to issue - https://github.com/slacmshankar/epicsarchiverap/issues/69
 * We want to create a policy that has extra fields that are not in getFieldsArchivedAsPartOfStream
 * When archiving PV's with this policy, we should check
 * <ol>
 * <li>Fields that are part of getFieldsArchivedAsPartOfStream should be in the .VAL's archiveFields</li>
 * <li>Fields that are NOT part of getFieldsArchivedAsPartOfStream should NOT be in the .VAL's archiveFields</li>
 * <li>Fields that are NOT part of getFieldsArchivedAsPartOfStream should have separate PVTypeInfo's.</li>
 * <li>Just to make things interesting, let's throw in EPICS aliases as well.</li>
 * </ol>
 * 
 * The best RTYP to use test this is the MOTOR record; however this has a lot of dependencies. 
 * So, we approximate this using a couple of CALC records in the UnitTestPVs
 * If we <code>caput ArchUnitTest:fieldtst:cnt 0.0</code>, we should see...
 * <pre><code>
 * $ camonitor ArchUnitTest:fieldtst ArchUnitTest:fieldtst.C
 * ArchUnitTest:fieldtst          2018-11-14 15:36:26.730758 0  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:26.730758 3.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:26.730758 0  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:57.730752 0  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:57.730752 0.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:58.730694 0.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:58.730694 1  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:59.730766 1  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:59.730766 1.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:00.730725 1.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:00.730725 2  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:01.730730 2  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:01.730730 2.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:02.730723 2.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:02.730723 3  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:03.730723 3  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:03.730723 3.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:04.730761 3.5  
 * </code></pre>
 * Rather unfortunate about the duplicate timestamps; but we should at least be able to test to make sure we capture all the data.
 * 
 * So, we archive ArchUnitTest:fieldtstalias from within a special policies file which add the .C archiveField
 * Make sure all the listed conditions are true for ArchUnitTest:fieldtst and ArchUnitTest:fieldtst.C. 
 * Make sure we can get the data for ArchUnitTest:fieldtst and ArchUnitTest:fieldtst.C (and for the alias as well).
 * 
 *   
 * @author mshankar
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class ArchiveFieldsNotInStreamTest {
	private static Logger logger = LogManager.getLogger(ArchiveFieldsNotInStreamTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;

	@BeforeClass
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}
	@Before
	public void setUp() throws Exception {
		System.getProperties().put("ARCHAPPL_POLICIES", System.getProperty("user.dir") + "/src/test/org/epics/archiverappliance/mgmt/ArchiveFieldsNotInStream.py");
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
	public void testArchiveFieldsPV() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 ((JavascriptExecutor)driver).executeScript("window.skipAutoRefresh = true;");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String[] fieldsToArchive = new String[] {
				 "ArchUnitTest:fieldtstalias"
		 };
		 pvstextarea.sendKeys(String.join("\n", fieldsToArchive));
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.debug("About to submit");
		 archiveButton.click();
		 Thread.sleep(4*60*1000);
		 logger.debug("Checking for archive status");
		 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(17*1000);
		 for(int i = 0; i < fieldsToArchive.length; i++) { 
			 int rowWithInfo = i+1;
			 WebElement statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(" + rowWithInfo + ") td:nth-child(1)"));
			 String pvNameObtainedFromTable = statusPVName.getText();
			 assertTrue("PV Name is not " + fieldsToArchive[i] + "; instead we get " + pvNameObtainedFromTable, fieldsToArchive[i].equals(pvNameObtainedFromTable));
			 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(" + rowWithInfo + ") td:nth-child(2)"));
			 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
			 String expectedPVStatus = "Being archived";
			 assertTrue("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable + " for field " + fieldsToArchive[i], expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));
		 }
		 
		 // Check that we have PVTypeInfo's for the main PV. Also check the archiveFields.
		 JSONObject valInfo = GetUrlContent.getURLContentAsJSONObject("http://localhost:17665/mgmt/bpl/getPVTypeInfo?pv=ArchUnitTest:fieldtst", true);
		 logger.debug(valInfo.toJSONString());
		 @SuppressWarnings("unchecked")
		 List<String> archiveFields = (List<String>) valInfo.get("archiveFields");
		 assertTrue("TypeInfo should contain the HIHI field but it does not",   archiveFields.contains("HIHI"));
		 assertTrue("TypeInfo should contain the LOLO field but it does not",   archiveFields.contains("LOLO"));
		 assertTrue("TypeInfo should not contain the DESC field but it does",   !archiveFields.contains("DESC"));
		 assertTrue("TypeInfo should not contain the C field but it does",      !archiveFields.contains("C"));

		 JSONObject C_Info = GetUrlContent.getURLContentAsJSONObject("http://localhost:17665/mgmt/bpl/getPVTypeInfo?pv=ArchUnitTest:fieldtst.C", true);
		 assertTrue("Did not find a typeinfo for ArchUnitTest:fieldtst.C",      C_Info != null);
		 logger.debug(C_Info.toJSONString());
		 
		 testRetrievalCount("ArchUnitTest:fieldtst", new double[] { 0.0 } );
		 siocSetup.caput("ArchUnitTest:fieldtst:cnt", "0.0");
		 Thread.sleep(2*60*1000);
		 testRetrievalCount("ArchUnitTest:fieldtst", new double[] { 0.0 } );
		 testRetrievalCount("ArchUnitTest:fieldtst.C", new double[] { 3.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5 } );
		 testRetrievalCount("ArchUnitTest:fieldtstalias", new double[] { 0.0 } );
		 testRetrievalCount("ArchUnitTest:fieldtstalias.C", new double[] { 3.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5 } );
	}
	
	private void testRetrievalCount(String pvName, double[] expectedValues) throws IOException {
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		Timestamp end = TimeUtils.plusDays(TimeUtils.now(), 1);
		Timestamp start = TimeUtils.minusDays(end, 2);
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, start, end, null)) {
			long previousEpochSeconds = 0;
			int eventCount = 0;
			assertTrue("Got a null event stream for PV " + pvName,   stream != null);
			for(Event e : stream) {
				long actualSeconds = e.getEpochSeconds();
				logger.debug("For " + pvName + " got value " + e.getSampleValue().getValue().doubleValue());
				assertTrue("Got a sample at or before the previous sample " + actualSeconds + " ! >= " + previousEpochSeconds, actualSeconds > previousEpochSeconds);
				previousEpochSeconds = actualSeconds;
				assertTrue("Got " + e.getSampleValue().getValue().doubleValue() + " expecting " +  expectedValues[eventCount] + " at " + eventCount, 
						Math.abs(Math.abs(e.getSampleValue().getValue().doubleValue()) -  Math.abs(expectedValues[eventCount])) < 0.001);
				eventCount++;
			}

			assertTrue("Expecting " + expectedValues.length + " got " + eventCount + " for pv " + pvName, eventCount == expectedValues.length);
		}
	}
}
