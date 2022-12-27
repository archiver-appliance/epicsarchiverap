package org.epics.archiverappliance.retrieval.postprocessor;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
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
import org.junit.experimental.categories.Category;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;

/**
 * This is a framework for various data driven post processor tests from Michael.
 * Some variation of getting raw PB files, loading it into the system and then running retrieval on it to check the results.
 * @author mshankar
 *
 */
@Category(IntegrationTests.class)
public class DataDrivenPostProcessorTest {
	private static Logger logger = Logger.getLogger(DataDrivenPostProcessorTest.class.getName());
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
		File destFile = new File(ltsFolder + "/LN/AM/RadMon/2/DoseRate/I:2014.pb");
		String srcFile = "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/test1/lrm01_raw.pb";
		destFile.getParentFile().mkdirs();
		FileUtils.copyFile(new File(srcFile), destFile);
		assertTrue(destFile.getAbsolutePath() + "does not exist", destFile.exists());
		 
		// Load a sample PVTypeInfo from a prototype file.
		JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File("src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
		PVTypeInfo srcPVTypeInfo = new PVTypeInfo();
		JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
		decoder.decode(srcPVTypeInfoJSON, srcPVTypeInfo);
		assertTrue("Expecting PV typeInfo for " + pvName + "; instead it is " + srcPVTypeInfo.getPvName(), srcPVTypeInfo.getPvName().equals(pvName));
		String newPVName = "LN-AM{RadMon:2}DoseRate-I";
		PVTypeInfo newPVTypeInfo = new PVTypeInfo(newPVName, srcPVTypeInfo);
		newPVTypeInfo.setPaused(true);
		newPVTypeInfo.setChunkKey("LN/AM/RadMon/2/DoseRate/I:");
		JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
		GetUrlContent.postObjectAndGetContentAsJSONObject("http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(newPVName, "UTF-8") + "&createnew=true", encoder.encode(newPVTypeInfo));

		logger.info("Sample file copied to " + destFile.getAbsolutePath());

		Timestamp start = TimeUtils.convertFromISO8601String("2014-12-08T07:03:55.000Z");
		Timestamp end   = TimeUtils.convertFromISO8601String("2014-12-08T08:04:00.000Z");
		
		checkRetrieval(newPVName, start, end, 1, true);
		checkRetrieval("firstSample_7(" + newPVName + ")", start, end, 1, true);
		checkRetrieval("lastSample_7(" + newPVName + ")", start, end, 1, true);
		checkRetrieval("meanSample_7(" + newPVName + ")", start, end, 1, true);
	}
	
	private int checkRetrieval(String retrievalPVName, Timestamp start, Timestamp end, int expectedAtLeastEvents, boolean exactMatch) throws IOException {
		long startTimeMillis = System.currentTimeMillis();
		RawDataRetrieval rawDataRetrieval = new RawDataRetrieval("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw?param1=abc123&param2=def456");
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
		
}
