package org.epics.archiverappliance.retrieval;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

public class MultiPVClusterRetrievalTest {
	private static Logger logger = Logger.getLogger(MultiPVClusterRetrievalTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
	PBCommonSetup pbSetup = new PBCommonSetup();
	short year = (short) 2015;

	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "_dataretrieval";
	String pvName2 = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "_dataretrieval2";

	static {
        System.setProperty("ARCHAPPL_SHORT_TERM_FOLDER", "/scratch/LargeDisk/sts/ArchiverStore");
        System.setProperty("ARCHAPPL_MEDIUM_TERM_FOLDER", "/scratch/LargeDisk/mts/ArchiverStore");
        System.setProperty("ARCHAPPL_LONG_TERM_FOLDER", "/scratch/LargeDisk/lts/ArchiverStore");
    }
	
	@Before
	public void setUp() throws Exception {	
		tomcatSetup.setUpClusterWithWebApps(this.getClass().getSimpleName(), 2);
	}

	@After
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
	}
	
	/**
	 * Test to make sure that data is retrieved from across clusters.
	 * @throws Exception 
	 * @throws UnsupportedEncodingException 
	 */
	@Test
	public void multiplePvsAcrossCluster() throws Exception {
		ConfigService configService = new ConfigServiceForTests(new File("./bin"));
		
		// Set up pbplugin so that data can be retrieved using the instance
		pbplugin.initialize("pb://localhost?name=STS&rootFolder=" + System.getProperty("ARCHAPPL_SHORT_TERM_FOLDER") 
				+ "&partitionGranularity=PARTITION_YEAR&pp=pb", configService);
		
		// Delete any data that currently exists
		Files.deleteIfExists(PlainPBPathNameUtility.getPathNameForTime(pbplugin, pvName, TimeUtils.getStartOfYearInSeconds(year), 
				new ArchPaths(), configService.getPVNameToKeyConverter()));
		
		// Generate an event stream to populate the PB files
		SimulationEventStream simstream = new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0), year);
		try(BasicContext context = new BasicContext()) {
			pbplugin.appendData(context, pvName, simstream);
			pbplugin.appendData(context, pvName2, simstream);
		}
		
		// Load a sample PVTypeInfo from a prototype file.
		JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File("src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
		
		// Create target for decoded type info from JSON
		PVTypeInfo srcPVTypeInfo = new PVTypeInfo();
		
		// Decoder for PVTypeInfo
		JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
		
		// Create type info from the data
		decoder.decode(srcPVTypeInfoJSON, srcPVTypeInfo);
		
		PVTypeInfo pvTypeInfo1 = new PVTypeInfo(pvName, srcPVTypeInfo);
		assertTrue("Expecting PV typeInfo for " + pvName + "; instead it is " + pvTypeInfo1.getPvName(), pvTypeInfo1.getPvName().equals(pvName));
		PVTypeInfo pvTypeInfo2 = new PVTypeInfo(pvName2, srcPVTypeInfo);
		assertTrue("Expecting PV typeInfo for " + pvName2 + "; instead it is " + pvTypeInfo2.getPvName(), pvTypeInfo2.getPvName().equals(pvName2));

		JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
		
		pvTypeInfo1.setPaused(true);
		pvTypeInfo1.setChunkKey(pvName.substring(2, pvName.length()) + ":");
		pvTypeInfo1.setCreationTime(TimeUtils.convertFromISO8601String("2013-11-11T14:49:58.523Z"));
		pvTypeInfo1.setModificationTime(TimeUtils.now());
		pvTypeInfo1.setApplianceIdentity("appliance0");
		GetUrlContent.postObjectAndGetContentAsJSONObject("http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8") + "&override=true", encoder.encode(pvTypeInfo1));

		logger.info("Added " + pvName + " to appliance0");
		
		pvTypeInfo2.setPaused(true);
		pvTypeInfo2.setChunkKey(pvName2.substring(2, pvName2.length()) + ":");
		pvTypeInfo2.setCreationTime(TimeUtils.convertFromISO8601String("2013-11-11T14:49:58.523Z"));
		pvTypeInfo2.setModificationTime(TimeUtils.now());
		pvTypeInfo2.setApplianceIdentity("appliance1");
		GetUrlContent.postObjectAndGetContentAsJSONObject("http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName2, "UTF-8") + "&override=true", encoder.encode(pvTypeInfo2));
		logger.info("Added " + pvName + " to appliance1");
		
		logger.info("Finished loading " + pvName + " and " + pvName2 + " into their appliances.");
		
		String startString = "2015-11-17T16:00:00.000Z";
		String endString = "2015-11-17T16:01:00.000Z";
		
		Timestamp start = TimeUtils.convertFromISO8601String(startString);
		Timestamp end = TimeUtils.convertFromISO8601String(endString);
		
		Map<String, List<JSONObject>> pvToData = retrieveJsonResults(startString, endString);
		
		logger.info("Received response from server; now retrieving data using PBStoragePlugin");
		
		try (BasicContext context = new BasicContext(); 
				EventStream pv1ResultsStream = new CurrentThreadWorkerEventStream(pvName, pbplugin.getDataForPV(context, pvName, start, end));
				EventStream pv2ResultsStream = new CurrentThreadWorkerEventStream(pvName2, pbplugin.getDataForPV(context, pvName2, start, end))) {
			
			compareDataAndTimestamps(pvName, pvToData.get(pvName), pv1ResultsStream);
			compareDataAndTimestamps(pvName2, pvToData.get(pvName2), pv2ResultsStream);
		}
	}

	private Map<String, List<JSONObject>> retrieveJsonResults(String startString, String endString) throws IOException {
		logger.info("Retrieving data using JSON/HTTP and comparing it to retrieval over PBStoragePlugin");
		
		// Establish a connection with appliance0 -- borrowed from http://www.mkyong.com/java/how-to-send-http-request-getpost-in-java/
		URL obj = new URL("http://localhost:17665/retrieval/data/getDataForPVs.json?pv=" 
				+ URLEncoder.encode(pvName, "UTF-8") + "&pv=" + URLEncoder.encode(pvName2, "UTF-8") + "&from=" + URLEncoder.encode(startString, "UTF-8")
				+ "&to=" + URLEncoder.encode(endString, "UTF-8") + "&pp=pb");
		logger.info("Opening this URL: " + obj.toString());
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("GET");
		
		// Get response code
		int responseCode = con.getResponseCode();
		if (responseCode != 200) {
			logger.error("Response code was " + responseCode + "; exiting.");
			logger.error("Message: " + con.getResponseMessage());
			return null;
		}
		
		// Retrieve JSON response
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer content = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			content.append(inputLine);
		}
		in.close();
		Object json = JSONValue.parse(content.toString());
		
		JSONArray finalResult = (JSONArray) json;
		
		Map<String, List<JSONObject>> pvToData = new HashMap<>();
		int sizeOfResponse = finalResult.size();
		
		logger.debug("Size of response: " + sizeOfResponse);
		
		logger.debug("First part: " + finalResult.get(0).toString());
		logger.debug("Second part: " + finalResult.get(1).toString());
		
		for (int i = 0; i < sizeOfResponse; i++) {
			logger.debug("Count on the for-loop: " + i);
			
			// Three explicit castings because Java
			String pvName = ((JSONObject) ((JSONObject) ((JSONObject) finalResult.get(i))).get("meta")).get("name").toString();
			
			logger.debug("Extracted PV name: " + pvName);
			
			// Data for PV
			JSONArray pvDataJson = (JSONArray) ((JSONObject) finalResult.get(i)).get("data");
			List<JSONObject> pvData = new ArrayList<>();
			for (int j = 0; j < pvDataJson.size(); j++) 
				pvData.add((JSONObject) pvDataJson.get(j));
			
			pvToData.put(pvName, pvData);
			
			logger.debug("Grabbed PV " + pvName + " with data " + pvData);
		}
		
		return pvToData;
	}

	private void compareDataAndTimestamps(String pvName, List<JSONObject> pvJsonData, EventStream pv1ResultsStream) throws NoSuchElementException {
		int counter = 0;
		try {
			for (Event pluginEvent : pv1ResultsStream) {
				JSONObject jsonEvent = pvJsonData.get(counter++);
				
				// Get values
				double jsonValue = Double.valueOf(jsonEvent.get("val").toString());
				double pluginValue = Double.valueOf(pluginEvent.getSampleValue().toString());
				
				// Get seconds and nanoseconds for JSON
				String jsonSecondsPart = jsonEvent.get("secs").toString();
				String jsonNanosPart = jsonEvent.get("nanos").toString();
				String jsonTimestamp = jsonSecondsPart + ("000000000" + jsonNanosPart).substring(jsonNanosPart.length());
				
				// Get seconds and nanoseconds for plugin event
				String pluginSecondsPart = Long.toString(pluginEvent.getEpochSeconds());
				String pluginNanosPart = Integer.toString(pluginEvent.getEventTimeStamp().getNanos());
				String pluginTimestamp = pluginSecondsPart + ("000000000" + pluginNanosPart).substring(pluginNanosPart.length());
				
				assertTrue("JSON value, " + jsonValue + ", and plugin event value, " + pluginValue + ", are unequal.", jsonValue == pluginValue);
				assertTrue("JSON timestamp, " + jsonTimestamp + ", and plugin event timestamp, " + pluginTimestamp + ", are unequal.", jsonTimestamp.equals(pluginTimestamp));
			}
		} catch (IndexOutOfBoundsException e) {
			throw new IndexOutOfBoundsException("The data obtained from JSON and the plugin class for PV " + pvName + " are unequal in length.");
		}
	}
}
