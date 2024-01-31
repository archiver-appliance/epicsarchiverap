package org.epics.archiverappliance.retrieval;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

@Tag("integration")
public class MultiPVClusterRetrievalTest {
	private static final Logger logger = LogManager.getLogger(MultiPVClusterRetrievalTest.class.getName());
	private final TomcatSetup tomcatSetup = new TomcatSetup();
	private final PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
	short year = TimeUtils.getCurrentYear();

	private final String pvName = "MultiPVClusterRetrievalTest:dataretrieval";
	private final String pvName2 = "MultiPVClusterRetrievalTest:dataretrieval2";
	private final String ltsFolder = System.getenv("ARCHAPPL_LONG_TERM_FOLDER");
	private final File ltsPVFolder = new File(ltsFolder + File.separator + "MultiPVClusterRetrievalTest");

	@BeforeEach
	public void setUp() throws Exception {	
		tomcatSetup.setUpClusterWithWebApps(this.getClass().getSimpleName(), 2);
		
		if(ltsPVFolder.exists()) { 
			FileUtils.deleteDirectory(ltsPVFolder);
		}
	}

	@AfterEach
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
		if(ltsPVFolder.exists()) { 
			FileUtils.deleteDirectory(ltsPVFolder);
		}
	}
	
	/**
	 * Test to make sure that data is retrieved from across clusters.
	 * @throws Exception 
	 * @throws UnsupportedEncodingException 
	 */
	@Test
	public void multiplePvsAcrossCluster() throws Exception {
		ConfigService configService = new ConfigServiceForTests(-1);
		
		// Set up pbplugin so that data can be retrieved using the instance
		pbplugin.initialize("pb://localhost?name=LTS&rootFolder=" + ltsFolder + "&partitionGranularity=PARTITION_YEAR", configService);
				
		// Generate an event stream to populate the PB files
        SimulationEventStream simstream = new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0), TimeUtils.getStartOfYear(year), TimeUtils.getEndOfYear(year), 1);
		try(BasicContext context = new BasicContext()) {
			pbplugin.appendData(context, pvName, simstream);
			pbplugin.appendData(context, pvName2, simstream);
		}
		logger.info("Done generating data for PV in " + ltsPVFolder.getAbsolutePath());
		
		// Load a sample PVTypeInfo from a prototype file.
		JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File("src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
		
		// Create target for decoded type info from JSON
		PVTypeInfo srcPVTypeInfo = new PVTypeInfo();
		
		// Decoder for PVTypeInfo
		JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
		
		// Create type info from the data
		decoder.decode(srcPVTypeInfoJSON, srcPVTypeInfo);
		
		PVTypeInfo pvTypeInfo1 = new PVTypeInfo(pvName, srcPVTypeInfo);
		Assertions.assertEquals(pvName, pvTypeInfo1.getPvName(), "Expecting PV typeInfo for " + pvName + "; instead it is " + pvTypeInfo1.getPvName());
		PVTypeInfo pvTypeInfo2 = new PVTypeInfo(pvName2, srcPVTypeInfo);
		Assertions.assertEquals(pvName2, pvTypeInfo2.getPvName(), "Expecting PV typeInfo for " + pvName2 + "; instead it is " + pvTypeInfo2.getPvName());

		JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
		
		pvTypeInfo1.setPaused(true);
		pvTypeInfo1.setChunkKey(configService.getPVNameToKeyConverter().convertPVNameToKey(pvName));
		pvTypeInfo1.setCreationTime(TimeUtils.convertFromISO8601String("2013-11-11T14:49:58.523Z"));
		pvTypeInfo1.setModificationTime(TimeUtils.now());
		pvTypeInfo1.setApplianceIdentity("appliance0");
		GetUrlContent.postObjectAndGetContentAsJSONObject("http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8) + "&override=false&createnew=true", encoder.encode(pvTypeInfo1));

		logger.info("Added " + pvName + " to appliance0");
		
		pvTypeInfo2.setPaused(true);
		pvTypeInfo2.setChunkKey(configService.getPVNameToKeyConverter().convertPVNameToKey(pvName2));
		pvTypeInfo2.setCreationTime(TimeUtils.convertFromISO8601String("2013-11-11T14:49:58.523Z"));
		pvTypeInfo2.setModificationTime(TimeUtils.now());
		pvTypeInfo2.setApplianceIdentity("appliance1");
		GetUrlContent.postObjectAndGetContentAsJSONObject("http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName2, StandardCharsets.UTF_8) + "&override=false&createnew=true", encoder.encode(pvTypeInfo2));
		logger.info("Added " + pvName + " to appliance1");
		
		logger.info("Finished loading " + pvName + " and " + pvName2 + " into their appliances.");
		try { Thread.sleep(5*1000); } catch(Exception ex) {}
		short currentYear = TimeUtils.getCurrentYear();
		String startString = currentYear + "-11-17T16:00:00.000Z";
		String endString = currentYear + "-11-17T16:01:00.000Z";

        Instant start = TimeUtils.convertFromISO8601String(startString);
        Instant end = TimeUtils.convertFromISO8601String(endString);

        Map<String, List<JSONObject>> pvToData = retrieveJsonResults(startString, endString);
		
		logger.info("Received response from server; now retrieving data using PBStoragePlugin Start: " + TimeUtils.convertToISO8601String(start) + " End: " + TimeUtils.convertToISO8601String(end));

        try (BasicContext context = new BasicContext();
             EventStream pv1ResultsStream = new CurrentThreadWorkerEventStream(pvName, pbplugin.getDataForPV(context, pvName, start, end));
             EventStream pv2ResultsStream = new CurrentThreadWorkerEventStream(pvName2, pbplugin.getDataForPV(context, pvName2, start, end))) {
			assert pvToData != null;
			compareDataAndTimestamps(pvName, pvToData.get(pvName), pv1ResultsStream);
			compareDataAndTimestamps(pvName2, pvToData.get(pvName2), pv2ResultsStream);
		}
	}

	private Map<String, List<JSONObject>> retrieveJsonResults(String startString, String endString) throws IOException {
		logger.info("Retrieving data using JSON/HTTP and comparing it to retrieval over PBStoragePlugin");
		
		// Establish a connection with appliance0 -- borrowed from http://www.mkyong.com/java/how-to-send-http-request-getpost-in-java/
		URL obj = new URL("http://localhost:17665/retrieval/data/getDataForPVs.json?pv=" 
				+ URLEncoder.encode(pvName, StandardCharsets.UTF_8) + "&pv=" + URLEncoder.encode(pvName2, StandardCharsets.UTF_8) + "&from=" + URLEncoder.encode(startString, StandardCharsets.UTF_8)
				+ "&to=" + URLEncoder.encode(endString, StandardCharsets.UTF_8) + "&pp=pb");
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
		StringBuilder content = new StringBuilder();

		while ((inputLine = in.readLine()) != null) {
			content.append(inputLine);
		}
		in.close();
		Object json = JSONValue.parse(content.toString());
		
		JSONArray finalResult = (JSONArray) json;
		
		Map<String, List<JSONObject>> pvToData = new HashMap<>();
		int sizeOfResponse = finalResult.size();
		
		logger.info("Size of response: " + sizeOfResponse);
		
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
			for (Object o : pvDataJson) pvData.add((JSONObject) o);
			
			pvToData.put(pvName, pvData);
			
			logger.debug("Grabbed PV " + pvName + " with data " + pvData);
		}
		
		return pvToData;
	}

	private void compareDataAndTimestamps(String pvName, List<JSONObject> pvJsonData, EventStream pv1ResultsStream) throws NoSuchElementException {
		logger.info("Comparing data for pv " + pvName);
		int counter = 0;
		try {
			for (Event pluginEvent : pv1ResultsStream) {
				JSONObject jsonEvent = pvJsonData.get(counter++);
				
				// Get values
				double jsonValue = Double.parseDouble(jsonEvent.get("val").toString());
				double pluginValue = Double.parseDouble(pluginEvent.getSampleValue().toString());
				
				// Get seconds and nanoseconds for JSON
				String jsonSecondsPart = jsonEvent.get("secs").toString();
				String jsonNanosPart = jsonEvent.get("nanos").toString();
				String jsonTimestamp = jsonSecondsPart + ("000000000" + jsonNanosPart).substring(jsonNanosPart.length());
				
				// Get seconds and nanoseconds for plugin event
				String pluginSecondsPart = Long.toString(pluginEvent.getEpochSeconds());
                String pluginNanosPart = Integer.toString(pluginEvent.getEventTimeStamp().getNano());
				String pluginTimestamp = pluginSecondsPart + ("000000000" + pluginNanosPart).substring(pluginNanosPart.length());

				Assertions.assertEquals(jsonTimestamp, pluginTimestamp, "JSON timestamp, " + jsonTimestamp + ", and plugin event timestamp, " + pluginTimestamp + ", are unequal.");
				Assertions.assertEquals(jsonValue, pluginValue, 0.0, "JSON value, " + jsonValue + ", and plugin event value, " + pluginValue + ", are unequal.");
			}
		} catch (IndexOutOfBoundsException e) {
			throw new IndexOutOfBoundsException("The data obtained from JSON and the plugin class for PV " + pvName + " are unequal in length.");
		}
	}
}
