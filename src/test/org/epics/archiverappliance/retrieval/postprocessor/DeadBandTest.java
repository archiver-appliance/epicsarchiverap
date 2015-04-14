package org.epics.archiverappliance.retrieval.postprocessor;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.retrieval.client.EpicsMessage;
import org.epics.archiverappliance.retrieval.client.GenMsgIterator;
import org.epics.archiverappliance.retrieval.client.InfoChangeHandler;
import org.epics.archiverappliance.retrieval.client.RawDataRetrieval;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;

/**
 * Michael DavidSaver supplied the data for this test.
 * We have raw data and data from another PV that applies the ADEL.
 * <ol>
 * <li>sig1-wo-adel.pb - the raw result of TST-CT{}Sig:1-I which has ADEL=MDEL=0.</li>
 * <li>sig2-w-adel.pb - TST-CT{}Sig:2-I with ADEL=MDEL=2</li>
 * <li>The time range in question is Dec/10/2014 14:12:42 through 14:12:55 EST.</li>
 * </ol>
 * @author mshankar
 *
 */
public class DeadBandTest {
	private static Logger logger = Logger.getLogger(DeadBandTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	private String pvName = "UnitTestNoNamingConvention:inactive1";
	private String ltsFolderName = System.getenv("ARCHAPPL_LONG_TERM_FOLDER"); 
	private File ltsFolder = new File(ltsFolderName);
	
	@Before
	public void setUp() throws Exception {
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		
		if(ltsFolder.exists()) { 
			FileUtils.deleteDirectory(ltsFolder);
		}
	}

	@After
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();

		if(ltsFolder.exists()) { 
			FileUtils.deleteDirectory(ltsFolder);
		}
	}

	@Test
	public void testRetrievalPV1() throws Exception {
		File destFile = new File(ltsFolder + "/TST-CT{}Sig/1-I:2014.pb");
		String srcFile = "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/deadband/sig1-wo-adel.pb";
		destFile.getParentFile().mkdirs();
		FileUtils.copyFile(new File(srcFile), destFile);
		assertTrue(destFile.getAbsolutePath() + "does not exist", destFile.exists());
		 
		// Load a sample PVTypeInfo from a prototype file.
		JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File("src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
		PVTypeInfo srcPVTypeInfo = new PVTypeInfo();
		JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
		decoder.decode(srcPVTypeInfoJSON, srcPVTypeInfo);
		assertTrue("Expecting PV typeInfo for " + pvName + "; instead it is " + srcPVTypeInfo.getPvName(), srcPVTypeInfo.getPvName().equals(pvName));
		String newPVName = "TST-CT{}Sig:1-I";
		PVTypeInfo newPVTypeInfo = new PVTypeInfo(newPVName, srcPVTypeInfo);
		newPVTypeInfo.setPaused(true);
		newPVTypeInfo.setChunkKey("TST-CT{}Sig/1-I:");
		JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
		GetUrlContent.postObjectAndGetContentAsJSONObject("http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(newPVName, "UTF-8"), encoder.encode(newPVTypeInfo));

		logger.info("Sample file copied to " + destFile.getAbsolutePath());

		Timestamp start = TimeUtils.convertFromISO8601String("2014-12-10T19:10:00.000Z");
		Timestamp end   = TimeUtils.convertFromISO8601String("2014-12-10T19:15:55.000Z");
		
		checkRetrieval(newPVName, start, end, 37, true);
		try(FileBackedPBEventStream compareStream = new FileBackedPBEventStream("TST-CT{}Sig:2-I", Paths.get("src/test/org/epics/archiverappliance/retrieval/postprocessor/data/deadband/sig2-w-adel.pb"), ArchDBRTypes.DBR_SCALAR_DOUBLE)) { 
			compareStreams("deadBand_2.0(" + newPVName + ")", start, end, compareStream);
		}
	}
	
	private int checkRetrieval(String retrievalPVName, Timestamp start, Timestamp end, int expectedAtLeastEvents, boolean exactMatch) throws IOException {
		long startTimeMillis = System.currentTimeMillis();
		RawDataRetrieval rawDataRetrieval = new RawDataRetrieval("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		int eventCount = 0;

		final HashMap<String, String> metaFields = new HashMap<String, String>(); 
		// Make sure we get the EGU as part of a regular VAL call.
		try(GenMsgIterator strm = rawDataRetrieval.getDataForPV(retrievalPVName, start, end, false, null)) { 
			PayloadInfo info = null;
			assertTrue("We should get some data for " + retrievalPVName + " , we are getting a null stream back", strm != null); 
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

		logger.info("For " + retrievalPVName + "we were expecting " + expectedAtLeastEvents + "events. We got " + eventCount);
		assertTrue("Expecting " + expectedAtLeastEvents + "events for " + retrievalPVName + ". We got " + eventCount, eventCount >= expectedAtLeastEvents);
		if(exactMatch) { 
			assertTrue("Expecting " + expectedAtLeastEvents + "events for " + retrievalPVName + ". We got " + eventCount, eventCount == expectedAtLeastEvents);
		}
		return eventCount;
	}
	
	private static void mergeHeaders(PayloadInfo info, HashMap<String, String> headers) { 
		 int headerCount = info.getHeadersCount();
		 for(int i = 0; i < headerCount; i++) { 
			 String headerName = info.getHeaders(i).getName();
			 String headerValue = info.getHeaders(i).getVal();
			 logger.debug("Adding header " + headerName + " = " + headerValue);
			 headers.put(headerName, headerValue);
		 }
	}
	
	private void compareStreams(String retrievalPVName, Timestamp start, Timestamp end, FileBackedPBEventStream compareStream) throws IOException {
		long startTimeMillis = System.currentTimeMillis();
		RawDataRetrieval rawDataRetrieval = new RawDataRetrieval("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		int eventCount = 0;

		final HashMap<String, String> metaFields = new HashMap<String, String>(); 
		// Make sure we get the EGU as part of a regular VAL call.
		try(GenMsgIterator strm = rawDataRetrieval.getDataForPV(retrievalPVName, start, end, false, null)) { 
			PayloadInfo info = null;
			assertTrue("We should get some data for " + retrievalPVName + " , we are getting a null stream back", strm != null); 
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

			Iterator<Event> compareIt = compareStream.iterator();

			for(EpicsMessage dbrEvent : strm) {
				assertTrue("We seem to have run out of events at " + eventCount, compareIt.hasNext());
				Event compareEvent = compareIt.next();
				assertTrue("At event " + eventCount + ", from the operator we have an event at " 
						+ TimeUtils.convertToISO8601String(dbrEvent.getTimestamp())
						+ " and from the compare stream, we have an event at "
						+ TimeUtils.convertToISO8601String(compareEvent.getEventTimeStamp()),
						dbrEvent.getTimestamp().equals(compareEvent.getEventTimeStamp()));
				
				eventCount++;
			}

			logger.info("Retrival for " + retrievalPVName + "=" + (endTimeMillis - startTimeMillis) + "(ms)");
		}
	}
}
