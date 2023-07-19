package org.epics.archiverappliance.reshard;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.List;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

/**
 * Simple test to test resharding a PV from one appliance to another...
 * <ul>
 * <li>Bring up a cluster of two appliances.</li>
 * <li>Archive the PV and wait for it to connect etc.</li>
 * <li>Determine the appliance for the PV.</li>
 * <li>Generate data for a PV making sure we have more than one data source and more than one chunk.</li>
 * <li>Pause the PV.</li>
 * <li>Reshard to the other appliance. </li>
 * <li>Resume the PV.</li>
 * <li>Check for data loss and resumption of archiving etc,</li>
 * </ul>
 * 
 * This test will probably fail at the beginning of the year; we generate data into MTS and LTS and if there is an overlap we get an incorrect number of events.
 * 
 * @author mshankar
 *
 */
@Tag("localEpics")
public class BasicReshardingTest {
	private static Logger logger = LogManager.getLogger(BasicReshardingTest.class.getName());
	private String pvName = "UnitTestNoNamingConvention:sine";
	private ConfigServiceForTests configService;
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;
	String folderSTS = ConfigServiceForTests.getDefaultShortTermFolder() + File.separator + "reshardSTS";
	String folderMTS = ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "reshardMTS";
	String folderLTS = ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "reshardLTS";

	@BeforeAll
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}

	@BeforeEach
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));

		System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER", folderSTS);
		System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", folderMTS);
		System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER", folderLTS);
		
		FileUtils.deleteDirectory(new File(folderSTS));
		FileUtils.deleteDirectory(new File(folderMTS));
		FileUtils.deleteDirectory(new File(folderLTS));
		
		siocSetup.startSIOCWithDefaultDB();
		tomcatSetup.setUpClusterWithWebApps(this.getClass().getSimpleName(), 2);
		driver = new FirefoxDriver();
	}

	@AfterEach
	public void tearDown() throws Exception {
		driver.quit();
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();

		FileUtils.deleteDirectory(new File(folderSTS));
		FileUtils.deleteDirectory(new File(folderMTS));
		FileUtils.deleteDirectory(new File(folderLTS));
	}
	
	
	@Test
	public void testReshardPV() throws Exception {
		// This section is straight from the ArchivePVTest
		// Let's archive the PV and wait for it to connect.
		driver.get("http://localhost:17665/mgmt/ui/index.html");
		WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		pvstextarea.sendKeys(pvName);
		WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		logger.debug("About to submit");
		archiveButton.click();
		// We have to wait for some time here as it does take a while for the workflow to complete.
		Thread.sleep(4*60*1000);
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
		
		PVTypeInfo typeInfoBeforePausing = getPVTypeInfo();
		// We determine the appliance for the PV by getting it's typeInfo.
		String applianceIdentity = typeInfoBeforePausing.getApplianceIdentity();
		Assertions.assertTrue(applianceIdentity != null, "Cannot determine appliance identity for pv from typeinfo ");
		
		// We use the PV's PVTypeInfo creation date for moving data. This PVTypeInfo was just created. 
		// We need to fake this to an old value so that the data is moved correctly.
		// The LTS data spans 2 years, so we set a creation time of about 4 years ago.
		typeInfoBeforePausing.setCreationTime(TimeUtils.getStartOfYear(TimeUtils.getCurrentYear() - 4));
		String updatePVTypeInfoURL = "http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8") + "&override=true";
		GetUrlContent.postObjectAndGetContentAsJSONObject(updatePVTypeInfoURL, JSONEncoder.getEncoder(PVTypeInfo.class).encode(typeInfoBeforePausing));
		
		Timestamp beforeReshardingCreationTimedstamp = typeInfoBeforePausing.getCreationTime();

		// Generate some data into the MTS and LTS
		String[] dataStores = typeInfoBeforePausing.getDataStores();
		Assertions.assertTrue(dataStores != null && dataStores.length > 1, "Data stores is null or empty for pv from typeinfo ");
		for(String dataStore : dataStores) { 
			logger.info("Data store for pv " + dataStore);
			StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(dataStore, configService);
			String name = plugin.getName();
			if(name.equals("MTS")) {
				// For the MTS we generate a couple of days worth of data
				Timestamp startOfMtsData = TimeUtils.minusDays(TimeUtils.now(), 3);
				long startOfMtsDataSecs = TimeUtils.convertToEpochSeconds(startOfMtsData);
				ArrayListEventStream strm = new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.convertToYearSecondTimestamp(startOfMtsDataSecs).getYear()));
				for(long offsetSecs = 0; offsetSecs < 2*24*60*60; offsetSecs += 60) { 
					strm.add(new SimulationEvent(TimeUtils.convertToYearSecondTimestamp(startOfMtsDataSecs + offsetSecs), ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>((double)offsetSecs)));
				}
				try(BasicContext context = new BasicContext()) {
					plugin.appendData(context, pvName, strm);
				}
			} else if(name.equals("LTS")) {
				// For the LTS we generate a couple of years worth of data
				long startofLtsDataSecs = TimeUtils.getStartOfYearInSeconds(TimeUtils.getCurrentYear() - 2);
				ArrayListEventStream strm = new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.convertToYearSecondTimestamp(startofLtsDataSecs).getYear()));
				for(long offsetSecs = 0; offsetSecs < 2*365*24*60*60; offsetSecs += 24*60*60) { 
					strm.add(new SimulationEvent(TimeUtils.convertToYearSecondTimestamp(startofLtsDataSecs + offsetSecs), ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>((double)offsetSecs)));
				}
				try(BasicContext context = new BasicContext()) {
					plugin.appendData(context, pvName, strm);
				}
			}
		}
		logger.info("Done generating data. Now making sure the setup is correct by fetching some data.");
		
		
		// Get the number of events before resharding...
		long eventCount = getNumberOfEvents();
		long expectedMinEventCount = 2*24*60 + 2*365;
		logger.info("Got " + eventCount + " events");
		Assertions.assertTrue(eventCount >= expectedMinEventCount, "Expecting at least " + expectedMinEventCount  + " got " + eventCount + " for ");

		String otherAppliance = "appliance1";
		if(applianceIdentity.equals(otherAppliance)) { 
			otherAppliance = "appliance0";
		}
		
		// Let's pause the PV.
		String pausePVURL = "http://localhost:17665/mgmt/bpl/pauseArchivingPV?pv=" + URLEncoder.encode(pvName, "UTF-8");
		JSONObject pauseStatus = GetUrlContent.getURLContentAsJSONObject(pausePVURL);
		Assertions.assertTrue(pauseStatus.containsKey("status") && pauseStatus.get("status").equals("ok"), "Cannot pause PV");
		Thread.sleep(1000);
		logger.info("Successfully paused the PV; other appliance is " + otherAppliance);
		
		driver.get("http://localhost:17665/mgmt/ui/pvdetails.html?pv=" + pvName);
		Thread.sleep(2*1000);
		WebElement reshardPVButton = driver.findElement(By.id("pvDetailsReshardPV"));
		logger.info("About to click on reshard button.");
		reshardPVButton.click();
		WebElement dialogOkButton = driver.findElement(By.id("pvReshardOk"));
		logger.info("About to click on reshard ok button");
		dialogOkButton.click();
		Thread.sleep(5*60*1000);
		WebElement pvDetailsTable = driver.findElement(By.id("pvDetailsTable"));
		List<WebElement> pvDetailsTableRows = pvDetailsTable.findElements(By.cssSelector("tbody tr"));
		for(WebElement pvDetailsTableRow : pvDetailsTableRows) {
			WebElement pvDetailsTableFirstCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(1)"));
			if(pvDetailsTableFirstCol.getText().contains("Instance archiving PV")) {
				WebElement pvDetailsTableSecondCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(2)"));
				String obtainedAppliance = pvDetailsTableSecondCol.getText();
				String expectedAppliance = otherAppliance;
				Assertions.assertTrue(expectedAppliance.equals(obtainedAppliance), "Expecting appliance to be " + expectedAppliance + "; instead it is " + obtainedAppliance);
				break;
			}
		}
		
		logger.info("Resharding UI is done.");


		PVTypeInfo typeInfoAfterResharding = getPVTypeInfo();
		String afterReshardingAppliance = typeInfoAfterResharding.getApplianceIdentity();
		Assertions.assertTrue(afterReshardingAppliance != null && afterReshardingAppliance.equals(otherAppliance), "Invalid appliance identity after resharding " + afterReshardingAppliance);
		Timestamp afterReshardingCreationTimedstamp = typeInfoAfterResharding.getCreationTime();
		
		// Let's resume the PV.
		String resumePVURL = "http://localhost:17665/mgmt/bpl/resumeArchivingPV?pv=" + URLEncoder.encode(pvName, "UTF-8");
		JSONObject resumeStatus = GetUrlContent.getURLContentAsJSONObject(resumePVURL);
		Assertions.assertTrue(resumeStatus.containsKey("status") && resumeStatus.get("status").equals("ok"), "Cannot resume PV");

		long postReshardEventCount = getNumberOfEvents();
		logger.info("After resharding, got " + postReshardEventCount + " events");
		Assertions.assertTrue(postReshardEventCount >= expectedMinEventCount, "Expecting at least " + expectedMinEventCount  + " got " + postReshardEventCount + " for ");
		
		checkRemnantShardPVs();

		// Make sure the creation timestamps are ok. If we have external integration, these play a part and you can not serve data because the creation timestamp is off
		Assertions.assertTrue(beforeReshardingCreationTimedstamp.equals(afterReshardingCreationTimedstamp), "Creation timestamps before "
				+ TimeUtils.convertToHumanReadableString(beforeReshardingCreationTimedstamp)
				+ " and after "
				+ TimeUtils.convertToHumanReadableString(afterReshardingCreationTimedstamp)
				+ " should be the same");

		
	}

	private void checkRemnantShardPVs() {
		// Make sure we do not have any temporary PV's present.
		String tempReshardPVs = "http://localhost:17665/mgmt/bpl/getAllPVs?pv=*_reshard_*";
		JSONArray reshardPVs = GetUrlContent.getURLContentAsJSONArray(tempReshardPVs);
		StringWriter buf = new StringWriter();
		for(Object reshardPV : reshardPVs) {
			buf.append(reshardPV.toString());
			buf.append(",");
		}
		Assertions.assertTrue(reshardPVs.size() == 0, "We seem to have some reshard temporary PV's present " + buf.toString());
	}
	
	private PVTypeInfo getPVTypeInfo() throws Exception { 
		String getPVTypeInfoURL = "http://localhost:17665/mgmt/bpl/getPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8");
		JSONObject typeInfoJSON = GetUrlContent.getURLContentAsJSONObject(getPVTypeInfoURL);
		Assertions.assertTrue(typeInfoJSON != null, "Cannot get typeinfo for pv using " + getPVTypeInfoURL);
        PVTypeInfo unmarshalledTypeInfo = new PVTypeInfo();
        JSONDecoder<PVTypeInfo> typeInfoDecoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        typeInfoDecoder.decode((JSONObject) typeInfoJSON, unmarshalledTypeInfo);
        return unmarshalledTypeInfo;
	}
	
	private long getNumberOfEvents() throws Exception { 
		Timestamp start = TimeUtils.convertFromEpochSeconds(TimeUtils.getStartOfYearInSeconds(TimeUtils.getCurrentYear() - 2), 0);
		Timestamp end = TimeUtils.now();
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		Timestamp obtainedFirstSample = null;
		long eventCount = 0;
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, start, end, null)) {
			if(stream != null) {
				for(Event e : stream) {
					if(obtainedFirstSample == null) { 
						obtainedFirstSample = e.getEventTimeStamp();
					}
					logger.debug("Sample from " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
					eventCount++;
				}
			} else { 
				Assertions.fail("Stream is null when retrieving data.");
			}
		}
		return eventCount;
	}
}