package org.epics.archiverappliance.retrieval.cluster;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
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

import java.io.File;
import java.io.IOException;
import java.time.Instant;

/**
 * Test data retrieval with aliasing and clustering.
 * We archive a PV with an alias and then test retrieval using the real name and the alias name.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class ClusterAliasTest {
	private static final Logger logger = LogManager.getLogger(ClusterAliasTest.class.getName());
	File persistenceFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ClusterAliasTest");
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;

	@BeforeAll
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}

	@BeforeEach
	public void setUp() throws Exception {
		if(persistenceFolder.exists()) {
			FileUtils.deleteDirectory(persistenceFolder);
		}
		persistenceFolder.mkdirs();
		siocSetup.startSIOCWithDefaultDB();
		System.getProperties().put(ConfigService.ARCHAPPL_PERSISTENCE_LAYER, "org.epics.archiverappliance.config.persistence.JDBM2Persistence");
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFolder.getPath() + File.separator + "testconfig.jdbm2");
		tomcatSetup.setUpClusterWithWebApps(this.getClass().getSimpleName(), 2);
		driver = new FirefoxDriver();
	}

	@AfterEach
	public void tearDown() throws Exception {
		driver.quit();
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();
		FileUtils.deleteDirectory(persistenceFolder);
	}

	@Test
	public void testSimpleArchivePV() throws Exception {
		 int port = ConfigServiceForTests.RETRIEVAL_TEST_PORT+1;
		 driver.get("http://localhost:" + port + "/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToArchive = "UnitTestNoNamingConvention:sinealias";
		 pvstextarea.sendKeys(pvNameToArchive);
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.debug("About to submit");
		 archiveButton.click();
		 // We have to wait for a few minutes here as it does take a while for the workflow to complete.
		 // In addition, we are also getting .HIHI etc the monitors for which get established many minutes after the beginning of archiving 
		 Thread.sleep(15*60*1000);
		 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 WebElement statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(1)"));
		 String pvNameObtainedFromTable = statusPVName.getText();
		 Assertions.assertTrue(pvNameToArchive.equals(pvNameObtainedFromTable), "PV Name is not " + pvNameToArchive + "; instead we get " + pvNameObtainedFromTable);
		 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
		 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
		 String expectedPVStatus = "Being archived";
		 Assertions.assertTrue(expectedPVStatus.equals(pvArchiveStatusObtainedFromTable), "Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable);
		 SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 2.0);
		 Thread.sleep(2*1000);
		 SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 3.0);
		 Thread.sleep(2*1000);
		 SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 4.0);
		 Thread.sleep(2*1000);
		 logger.info("Done updating UnitTestNoNamingConvention:sine.HIHI");
		 Thread.sleep(2*60*1000);

		 String aapl0 = checkInPersistence("UnitTestNoNamingConvention:sine", 0);
		 String aapl1 = checkInPersistence("UnitTestNoNamingConvention:sine", 1);
		 Assertions.assertTrue(aapl0.equals(aapl1), "Expecting the same appliance identity in both typeinfos, instead it is " + aapl0 + " in cluster member 0 and " + aapl1 + " in cluster member 1");
		 testRetrievalCount("UnitTestNoNamingConvention:sinealias");
		 testRetrievalCount("UnitTestNoNamingConvention:sine");		 
		 testRetrievalCount("UnitTestNoNamingConvention:sinealias.HIHI");
		 testRetrievalCount("UnitTestNoNamingConvention:sine.HIHI");		 
	}
	
	private String checkInPersistence(String pvName, int clusterIndex) throws Exception {
		logger.info("Checking for pvtype info in persistence for cluster member " + clusterIndex);
		String persistenceFile = persistenceFolder.getPath() + File.separator + "testconfig.jdbm2";
		String persistenceFileForMember = persistenceFile.replace(".jdbm2", "_appliance" + clusterIndex + ".jdbm2");
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFileForMember);
		JDBM2Persistence persistenceLayer = new JDBM2Persistence();
		PVTypeInfo typeInfo = persistenceLayer.getTypeInfo(pvName);
		Assertions.assertTrue("Expecting the pv typeinfo to be in persistence for cluster member " + clusterIndex + typeInfo != null);
		return typeInfo.getApplianceIdentity();
	}
	
	
	/**
	 * Make sure we get some data when retriving under the given name
	 * @throws IOException
	 */
	private void testRetrievalCount(String pvName) throws IOException {
		testRetrievalCountOnServer(pvName, "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		testRetrievalCountOnServer(pvName, "http://localhost:" + (ConfigServiceForTests.RETRIEVAL_TEST_PORT+1) + "/retrieval/data/getData.raw");
	}
	
	
	private void testRetrievalCountOnServer(String pvName, String serverRetrievalURL) throws IOException { 
		 RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(serverRetrievalURL);
        Instant end = TimeUtils.plusDays(TimeUtils.now(), 3);
        Instant start = TimeUtils.minusDays(end, 6);
		 try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName}, start, end, null)) {
			 long previousEpochSeconds = 0;
			 int eventCount = 0;

			 // We are making sure that the stream we get back has times in sequential order...
			 if(stream != null) {
				 for(Event e : stream) {
					 long actualSeconds = e.getEpochSeconds();
					 Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
					 previousEpochSeconds = actualSeconds;
					 eventCount++;
				 }
			 }

			 logger.info("Got " + eventCount + " event for pv " + pvName);
			 Assertions.assertTrue(eventCount > 0, "When asking for data using " + pvName + ", event count is 0. We got " + eventCount);
		 }
	}

}
