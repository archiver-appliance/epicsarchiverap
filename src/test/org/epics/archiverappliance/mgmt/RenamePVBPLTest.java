package org.epics.archiverappliance.mgmt;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test rename PV with data at the backend.
 * We create data in the LTS and then pause, rename and check to make sure we have the same number of samples before and after.
 * @author mshankar
 *
 */
@Tag("integration")@Tag("localEpics")
public class RenamePVBPLTest {
	private static Logger logger = LogManager.getLogger(RenamePVBPLTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	private String pvName = "UnitTestNoNamingConvention:inactive1";
	private short currentYear = TimeUtils.getCurrentYear();
	private File ltsFolder = new File(System.getenv("ARCHAPPL_LONG_TERM_FOLDER") + "/UnitTestNoNamingConvention");
	private File ltsFolderForNewPVName = new File(System.getenv("ARCHAPPL_LONG_TERM_FOLDER") + "/NewName_UnitTestNoNamingConvention");
	StoragePlugin storageplugin;
	private ConfigServiceForTests configService;

	@BeforeEach
	public void setUp() throws Exception {
		if(ltsFolder.exists()) { 
			FileUtils.deleteDirectory(ltsFolder);
		}
		if(ltsFolderForNewPVName.exists()) { 
			FileUtils.deleteDirectory(ltsFolderForNewPVName);
		}

		configService = new ConfigServiceForTests(-1);
		storageplugin = StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR", configService);
		siocSetup.startSIOCWithDefaultDB();
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		

		try(BasicContext context = new BasicContext()) {
			// Create three years worth of data in the LTS
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

	@AfterEach
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();

		if(ltsFolder.exists()) { 
			FileUtils.deleteDirectory(ltsFolder);
		}
		if(ltsFolderForNewPVName.exists()) { 
			FileUtils.deleteDirectory(ltsFolderForNewPVName);
		}
	}

	@Test
	public void testSimpleArchivePV() throws Exception {
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvName)))));
        PVAccessUtil.waitForStatusChange(pvName, "Being archived", 10, mgmtURL, 15);		 
		// We have now archived this PV, get some data and validate we got the expected number of events
		long beforeRenameCount = checkRetrieval(pvName, 3*365*86400);
		logger.info("Before renaming, we had this many events from retrieval" +  beforeRenameCount);
		Assertions.assertTrue(beforeRenameCount > 0, "We should see at least a few event before renaming the PV");
		 
		// Let's pause the PV.
        GetUrlContent.getURLContentWithQueryParameters(mgmtURL + "pauseArchivingPV", Map.of("pv", pvName), false);
        Thread.sleep(2 * 1000);
        PVAccessUtil.waitForStatusChange(pvName, "Paused", 10, mgmtURL, 15);
		logger.info("Successfully paused the PV " + pvName);

		// Let's rename the PV.
		String newPVName = "NewName_" + pvName;
        JSONObject renameStatus = GetUrlContent.getURLContentWithQueryParametersAsJSONObject(mgmtURL + "renamePV", Map.of("pv", pvName, "newname", newPVName), false);
		Assertions.assertTrue(renameStatus.containsKey("status") && renameStatus.get("status").equals("ok"), "Cannot rename PV");
        Thread.sleep(2 * 1000);

		long afterRenameCount = checkRetrieval(newPVName, 3*365*86400);
		logger.info("After renaming, we had this many events from retrieval" +  beforeRenameCount);
		// The  Math.abs(beforeRenameCount-afterRenameCount) < 2 is to cater to the engine not sending data after rename as the PV is still paused.
		Assertions.assertTrue(Math.abs(beforeRenameCount-afterRenameCount) < 2, "Different event counts before and after renaming. Before " + beforeRenameCount + " and after " + afterRenameCount);

		// Make sure the old PV still exists
		long afterRenameOldPVCount = checkRetrieval(pvName, 3*365*86400);
		Assertions.assertTrue(Math.abs(beforeRenameCount-afterRenameOldPVCount) < 2, "After the rename, we were still expecting data for the old PV " + afterRenameOldPVCount);
		 
		// Delete the old PV
        JSONObject deletePVtatus = GetUrlContent.getURLContentWithQueryParametersAsJSONObject(mgmtURL + "deletePV", Map.of("pv", pvName, "deleteData", "true"), false);
		Assertions.assertTrue(deletePVtatus.containsKey("status") && deletePVtatus.get("status").equals("ok"), "Cannot delete old PV");
		Thread.sleep(2 * 1000);
        PVAccessUtil.waitForStatusChange(pvName, "Not being archived", 10, mgmtURL, 15);
		logger.info("Done with deleting the old PV....." + pvName);

		 // Let's rename the PV back to its original name
        JSONObject renameBackStatus = GetUrlContent.getURLContentWithQueryParametersAsJSONObject(mgmtURL + "renamePV", Map.of("pv", newPVName, "newname", pvName), false);
		Assertions.assertTrue(renameBackStatus.containsKey("status") && renameBackStatus.get("status").equals("ok"), "Cannot rename PV");
		Thread.sleep(5000);

		 long afterRenamingBackCount = checkRetrieval(pvName, 3*365*86400);
		 logger.info("After renaming back to original, we had this many events from retrieval" +  afterRenamingBackCount);
		 Assertions.assertTrue(Math.abs(beforeRenameCount-afterRenamingBackCount) < 2, "Different event counts before and after renaming back. Before " + beforeRenameCount + " and after " + afterRenamingBackCount);
		 
	}

	private int checkRetrieval(String retrievalPVName, int expectedAtLeastEvents) throws IOException {
		long startTimeMillis = System.currentTimeMillis();
		RawDataRetrieval rawDataRetrieval = new RawDataRetrieval("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
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

			 long endTimeMillis =  System.currentTimeMillis();

			 
			 for(@SuppressWarnings("unused") EpicsMessage dbrevent : strm) {
				 eventCount++;
			 }
			 
			 logger.info("Retrival for " + retrievalPVName + "=" + (endTimeMillis - startTimeMillis) + "(ms)");
		 }

		 Assertions.assertTrue(eventCount >= expectedAtLeastEvents, "Expecting " + expectedAtLeastEvents + "events. We got " + eventCount);
		 return eventCount;
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
