package org.epics.archiverappliance.mgmt;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.EpicsMessage;
import org.epics.archiverappliance.retrieval.client.GenMsgIterator;
import org.epics.archiverappliance.retrieval.client.InfoChangeHandler;
import org.epics.archiverappliance.retrieval.client.RawDataRetrieval;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
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
import java.io.IOException;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;

import static org.epics.archiverappliance.config.ConfigServiceForTests.MGMT_INDEX_URL;
import static org.epics.archiverappliance.config.ConfigServiceForTests.MGMT_URL;

/**
 * Test rename PV with data at the backend.
 * We create data in the LTS and then pause, rename and check to make sure we have the same number of samples before and after.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class RenamePVBPLTest {
    private static Logger logger = LogManager.getLogger(RenamePVBPLTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();
    WebDriver driver;
    StoragePlugin storageplugin;
    private String pvName = "UnitTestNoNamingConvention:inactive1";
    private short currentYear = TimeUtils.getCurrentYear();
    private File ltsFolder = new File(System.getenv("ARCHAPPL_LONG_TERM_FOLDER") + "/UnitTestNoNamingConvention");
    private File ltsFolderForNewPVName =
            new File(System.getenv("ARCHAPPL_LONG_TERM_FOLDER") + "/NewName_UnitTestNoNamingConvention");
    private ConfigServiceForTests configService;

    @BeforeAll
    public static void setupClass() {
        WebDriverManager.firefoxdriver().setup();
    }

    private static void mergeHeaders(PayloadInfo info, HashMap<String, String> headers) {
        int headerCount = info.getHeadersCount();
        for (int i = 0; i < headerCount; i++) {
            String headerName = info.getHeaders(i).getName();
            String headerValue = info.getHeaders(i).getVal();
            logger.info("Adding header " + headerName + " = " + headerValue);
            headers.put(headerName, headerValue);
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
        if (ltsFolderForNewPVName.exists()) {
            FileUtils.deleteDirectory(ltsFolderForNewPVName);
        }

        configService = new ConfigServiceForTests(-1);
        storageplugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR",
                configService);
        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
        driver = new FirefoxDriver();

        try (BasicContext context = new BasicContext()) {
            // Create three years worth of data in the LTS
            for (short y = 3; y >= 0; y--) {
                short year = (short) (currentYear - y);
                for (int day = 0; day < 366; day++) {
                    ArrayListEventStream testData = new ArrayListEventStream(
                            PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                            new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));
                    int startofdayinseconds = day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                    for (int secondintoday = 0;
                            secondintoday < PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                            secondintoday++) {
                        // The value should be the secondsIntoYear integer divided by 600.
                        testData.add(new SimulationEvent(
                                startofdayinseconds + secondintoday,
                                year,
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                new ScalarValue<Double>(
                                        (double) (((int) (startofdayinseconds + secondintoday) / 600)))));
                    }
                    storageplugin.appendData(context, pvName, testData);
                }
            }
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        driver.quit();
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();

        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
        if (ltsFolderForNewPVName.exists()) {
            FileUtils.deleteDirectory(ltsFolderForNewPVName);
        }
    }

    @Test
    public void testSimpleArchivePV() throws Exception {
        driver.get(MGMT_INDEX_URL);
        WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
        pvstextarea.sendKeys(pvName);
        WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
        logger.debug("About to submit");
        archiveButton.click();
        // We have to wait for a few minutes here here as it does take a while for the workflow to complete.
        Thread.sleep(5 * 60 * 1000);
        WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
        checkStatusButton.click();
        Thread.sleep(2 * 1000);
        WebElement statusPVName =
                driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(1)"));
        String pvNameObtainedFromTable = statusPVName.getText();
        Assertions.assertTrue(
                pvName.equals(pvNameObtainedFromTable),
                "PV Name is not " + pvName + "; instead we get " + pvNameObtainedFromTable);
        WebElement statusPVStatus =
                driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
        String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
        String expectedPVStatus = "Being archived";
        Assertions.assertTrue(
                expectedPVStatus.equals(pvArchiveStatusObtainedFromTable),
                "Expecting PV archive status to be " + expectedPVStatus + "; instead it is "
                        + pvArchiveStatusObtainedFromTable);
        Thread.sleep(1 * 60 * 1000);

        // We have now archived this PV, get some data and validate we got the expected number of events
        long beforeRenameCount =
                checkRetrieval(pvName, 3 * 365 * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk());
        logger.info("Before renaming, we had this many events from retrieval" + beforeRenameCount);

        // Let's pause the PV.
        String pausePVURL = MGMT_URL + "/pauseArchivingPV?pv=" + URLEncoder.encode(pvName, "UTF-8");
        JSONObject pauseStatus = GetUrlContent.getURLContentAsJSONObject(pausePVURL);
        Assertions.assertTrue(
                pauseStatus.containsKey("status") && pauseStatus.get("status").equals("ok"), "Cannot pause PV");
        Thread.sleep(5000);
        logger.info("Successfully paused the PV " + pvName);

        // Let's rename the PV.
        String newPVName = "NewName_" + pvName;
        String renamePVURL = MGMT_URL + "/renamePV?pv=" + URLEncoder.encode(pvName, "UTF-8") + "&newname="
                + URLEncoder.encode(newPVName, "UTF-8");
        JSONObject renameStatus = GetUrlContent.getURLContentAsJSONObject(renamePVURL);
        Assertions.assertTrue(
                renameStatus.containsKey("status") && renameStatus.get("status").equals("ok"), "Cannot rename PV");
        Thread.sleep(5000);

        long afterRenameCount =
                checkRetrieval(newPVName, 3 * 365 * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk());
        logger.info("After renaming, we had this many events from retrieval" + beforeRenameCount);
        // The  Math.abs(beforeRenameCount-afterRenameCount) < 2 is to cater to the engine not sending data after rename
        // as the PV is still paused.
        Assertions.assertTrue(
                Math.abs(beforeRenameCount - afterRenameCount) < 2,
                "Different event counts before and after renaming. Before " + beforeRenameCount + " and after "
                        + afterRenameCount);

        // Make sure the old PV still exists
        long afterRenameOldPVCount = checkRetrieval(pvName, 3 * 365 * 86400);
        Assertions.assertTrue(
                Math.abs(beforeRenameCount - afterRenameOldPVCount) < 2,
                "After the rename, we were still expecting data for the old PV " + afterRenameOldPVCount);

        // Delete the old PV
        String deletePVURL = MGMT_URL + "/deletePV?pv=" + URLEncoder.encode(pvName, "UTF-8") + "&deleteData=true";
        JSONObject deletePVtatus = GetUrlContent.getURLContentAsJSONObject(deletePVURL);
        Assertions.assertTrue(
                deletePVtatus.containsKey("status")
                        && deletePVtatus.get("status").equals("ok"),
                "Cannot delete old PV");
        logger.info("Done with deleting the old PV....." + pvName);
        Thread.sleep(30000);

        // Let's rename the PV back to its original name
        String renamePVBackURL = MGMT_URL + "/renamePV?pv=" + URLEncoder.encode(newPVName, "UTF-8") + "&newname="
                + URLEncoder.encode(pvName, "UTF-8");
        JSONObject renameBackStatus = GetUrlContent.getURLContentAsJSONObject(renamePVBackURL);
        Assertions.assertTrue(
                renameBackStatus.containsKey("status")
                        && renameBackStatus.get("status").equals("ok"),
                "Cannot rename PV");
        Thread.sleep(5000);

        long afterRenamingBackCount =
                checkRetrieval(pvName, 3 * 365 * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk());
        logger.info("After renaming back to original, we had this many events from retrieval" + afterRenamingBackCount);
        Assertions.assertTrue(
                Math.abs(beforeRenameCount - afterRenamingBackCount) < 2,
                "Different event counts before and after renaming back. Before " + beforeRenameCount + " and after "
                        + afterRenamingBackCount);
    }

    private int checkRetrieval(String retrievalPVName, int expectedAtLeastEvents) throws IOException {
        long startTimeMillis = System.currentTimeMillis();
        RawDataRetrieval rawDataRetrieval = new RawDataRetrieval(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant now = TimeUtils.now();
        Instant start = TimeUtils.minusDays(now, 3 * 366);
        Instant end = now;
        int eventCount = 0;

		 final HashMap<String, String> metaFields = new HashMap<String, String>(); 
		 // Make sure we get the EGU as part of a regular VAL call.
        try (GenMsgIterator strm = rawDataRetrieval.getDataForPVs(Arrays.asList(retrievalPVName), TimeUtils.toSQLTimeStamp(start), TimeUtils.toSQLTimeStamp(end), false, null)) {
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

            long endTimeMillis = System.currentTimeMillis();

            for (@SuppressWarnings("unused") EpicsMessage dbrevent : strm) {
                eventCount++;
            }

            logger.info("Retrival for " + retrievalPVName + "=" + (endTimeMillis - startTimeMillis) + "(ms)");
        }

        Assertions.assertTrue(
                eventCount >= expectedAtLeastEvents,
                "Expecting " + expectedAtLeastEvents + "events. We got " + eventCount);
        return eventCount;
    }
}
