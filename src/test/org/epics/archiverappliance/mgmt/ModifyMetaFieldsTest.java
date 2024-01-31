package org.epics.archiverappliance.mgmt;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
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

import java.io.File;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;

/**
 * Modify the metafields across the cluster. We archive and then send the modifyMetaFields to the other appliance.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class ModifyMetaFieldsTest {
	private static Logger logger = LogManager.getLogger(ModifyMetaFieldsTest.class.getName());
	File persistenceFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ModifyMetaFieldsTest");
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
	public void testChangeMetaFields() throws Exception {
		 int port = ConfigServiceForTests.RETRIEVAL_TEST_PORT+1;
		 driver.get("http://localhost:" + port + "/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvName = "UnitTestNoNamingConvention:sine";
		 pvstextarea.sendKeys(pvName);
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
		 Assertions.assertTrue(pvName.equals(pvNameObtainedFromTable), "PV Name is not " + pvName + "; instead we get " + pvNameObtainedFromTable);
		 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
		 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
		 String expectedPVStatus = "Being archived";
		 Assertions.assertTrue(expectedPVStatus.equals(pvArchiveStatusObtainedFromTable), "Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable);
		 String aapl0 = checkInPersistence(pvName, 0);
		 String aapl1 = checkInPersistence(pvName, 1);
		 Assertions.assertTrue(aapl0.equals(aapl1), "Expecting the same appliance identity in both typeinfos, instead it is " + aapl0 + " in cluster member 0 and " + aapl1 + " in cluster member 1");
		 
		 // Let's pause the PV.
		 String pausePVURL = "http://localhost:17665/mgmt/bpl/pauseArchivingPV?pv=" + URLEncoder.encode(pvName, "UTF-8");
		 JSONObject pauseStatus = GetUrlContent.getURLContentAsJSONObject(pausePVURL);
		 Assertions.assertTrue(pauseStatus.containsKey("status") && pauseStatus.get("status").equals("ok"), "Cannot pause PV");
		 Thread.sleep(5000);
		 logger.info("Successfully paused the PV " + pvName);
		 

		 // Make the modifyMetaFields call to the other appliance.
		 int clusterIndex = aapl0.equals("appliance0") ? 1 : 0;
		 port = ConfigServiceForTests.RETRIEVAL_TEST_PORT + clusterIndex;
		 String modifyMetaFieldsURL = "http://localhost:" + port + "/mgmt/bpl/modifyMetaFields?pv=" 
				 + URLEncoder.encode(pvName, "UTF-8") 
				 + "&command=clear"
				 + "&command=add,HIHI,HIGH,LOLO,LOW"
				 + "&command=add,DESC"
				 + "&command=remove,HIHI,LOLO";
		 JSONObject status = GetUrlContent.getURLContentAsJSONObject(modifyMetaFieldsURL);
		 logger.info(status.toJSONString());

		 Thread.sleep(5000);

		 String[] expectedFields = new String[] {"HIGH", "LOW", "DESC"};
		 String[] fields0 = getFieldsFromPersistence(pvName, 0);
		 String[] fields1 = getFieldsFromPersistence(pvName, 1);
		 Assertions.assertTrue(arrayEquals(fields0, expectedFields), "Expecting the fields to be " + fieldsToCSV(expectedFields) + " in cluster member 0 instead it is " + fieldsToCSV(fields0));
		 Assertions.assertTrue(arrayEquals(fields1, expectedFields), "Expecting the fields to be " + fieldsToCSV(expectedFields) + " in cluster member 1 instead it is " + fieldsToCSV(fields1));

	}
	
	/**
	 * Do these two arrays contain the same set of fields
	 * @param actualFields
	 * @param expectedFields
	 * @return
	 */
	private boolean arrayEquals(String[] actualFields, String[] expectedFields) {
		if(actualFields.length != expectedFields.length) return false;
		List<String> actualFieldsList = Arrays.asList(actualFields);
		List<String> expectedFieldsList = Arrays.asList(expectedFields);
		
		if(!expectedFieldsList.containsAll(actualFieldsList)) return false;
		if(!actualFieldsList.containsAll(expectedFieldsList)) return false;
		return true;
	}

	private String fieldsToCSV(String[] fields) {
		StringWriter buf = new StringWriter();
		boolean isFirst = true;
		for(String field : fields) { 
			if(isFirst) { isFirst = false; } else { buf.append(","); }
			buf.append(field);
		}
		return buf.toString();
	}

	private String checkInPersistence(String pvName, int clusterIndex) throws Exception {
		logger.info("Checking for pvtype info in persistence for cluster member " + clusterIndex);
		String persistenceFile = persistenceFolder.getPath() + File.separator + "testconfig.jdbm2";
		String persistenceFileForMember = persistenceFile.replace(".jdbm2", "_appliance" + clusterIndex + ".jdbm2");
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFileForMember);
		JDBM2Persistence persistenceLayer = new JDBM2Persistence();
		PVTypeInfo typeInfo = persistenceLayer.getTypeInfo(pvName);
		Assertions.assertTrue(typeInfo != null, "Expecting the pv typeinfo to be in persistence for cluster member " + clusterIndex);
		return typeInfo.getApplianceIdentity();
	}
	
	private String[] getFieldsFromPersistence(String pvName, int clusterIndex) throws Exception {
		logger.info("Checking for pvtype info in persistence for cluster member " + clusterIndex);
		String persistenceFile = persistenceFolder.getPath() + File.separator + "testconfig.jdbm2";
		String persistenceFileForMember = persistenceFile.replace(".jdbm2", "_appliance" + clusterIndex + ".jdbm2");
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFileForMember);
		JDBM2Persistence persistenceLayer = new JDBM2Persistence();
		PVTypeInfo typeInfo = persistenceLayer.getTypeInfo(pvName);
		Assertions.assertTrue(typeInfo != null, "Expecting the pv typeinfo to be in persistence for cluster member " + clusterIndex);
		return typeInfo.getArchiveFields();
	}


}
