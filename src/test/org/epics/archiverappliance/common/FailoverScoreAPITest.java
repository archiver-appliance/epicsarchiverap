/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the getDataAtTime API when using the merge dedup plugin.
 * Generate data such that each failover cluster has one of known data on the morning or afternoon.
 * @author mshankar
 *
 */
@Category(IntegrationTests.class)
public class FailoverScoreAPITest {
	private static Logger logger = Logger.getLogger(FailoverScoreAPITest.class.getName());
	private ConfigServiceForTests configService;
	String pvName = "FailoverRetrievalTest";
	ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
	TomcatSetup tomcatSetup = new TomcatSetup();
	
	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		tomcatSetup.setUpFailoverWithWebApps(this.getClass().getSimpleName());		
	}

	/**
	 * Generate a months worth of data for the given appserver; one per day, a boolean indicating if the sample is in the morning or afternoon.
	 * @param applURL - The URL for the appliance.
	 * @param applianceName - The name of the appliance
	 * @param theMonth - The month we generate data for. We generate a month's worth of MTS data.
	 * @param morningp - If true; data is generated for the morning else afternoon.
	 * @throws Exception
	 */
	private long generateMTSData(String applURL, String applianceName, Timestamp theMonth, boolean morningp)
			throws Exception {
		int genEventCount = 0;
		StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=LTS&rootFolder=" + "tomcat_"+ this.getClass().getSimpleName() + "/" + applianceName + "/mts" + "&partitionGranularity=PARTITION_DAY", configService);
		try(BasicContext context = new BasicContext()) {
			for(long s = TimeUtils.getPreviousPartitionLastSecond(TimeUtils.convertToEpochSeconds(theMonth), PartitionGranularity.PARTITION_DAY) + 1;
					s < TimeUtils.convertToEpochSeconds(TimeUtils.now()); 
					s = s + 86400) {
				ArrayListEventStream strm = new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.convertToYearSecondTimestamp(s).getYear()));
				POJOEvent pojoEvent = new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, TimeUtils.convertFromEpochSeconds(s + (morningp ? 10*60*60 : 20*60*60 ), 0), new ScalarValue<Double>((double)(morningp ? 10 : 20 )), 0, 0);
				logger.debug("Generating event at " + TimeUtils.convertToHumanReadableString(pojoEvent.getEventTimeStamp()));
				strm.add(pojoEvent);
				genEventCount++;
				plugin.appendData(context, pvName, strm);
			}			
		}		
		logger.info("Done generating data for appliance " + applianceName);
		
		JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File("src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
		PVTypeInfo destPVTypeInfo = new PVTypeInfo();
		JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
		JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
		decoder.decode(srcPVTypeInfoJSON, destPVTypeInfo);
		
		destPVTypeInfo.setPaused(true);
		destPVTypeInfo.setPvName(pvName);
		destPVTypeInfo.setApplianceIdentity(applianceName);
		destPVTypeInfo.setChunkKey(configService.getPVNameToKeyConverter().convertPVNameToKey(pvName));
		destPVTypeInfo.setCreationTime(TimeUtils.convertFromISO8601String("2020-11-11T14:49:58.523Z"));
		destPVTypeInfo.setModificationTime(TimeUtils.now());
		GetUrlContent.postObjectAndGetContentAsJSONObject(applURL + "/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8") + "&override=true&createnew=true", encoder.encode(destPVTypeInfo));
		logger.info("Added " + pvName + " to the appliance " + applianceName);
		
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(applURL + "/retrieval/data/getData.raw");
		long rtvlEventCount = 0;
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, TimeUtils.minusDays(TimeUtils.now(), 90), TimeUtils.plusDays(TimeUtils.now(), 31), null)) {
			long lastEvEpoch = 0;
			if(stream != null) {
				for(Event e : stream) {
					long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
					if(lastEvEpoch != 0) {
						assertTrue("We got events more than " + 86400 + " seconds apart " + TimeUtils.convertToHumanReadableString(lastEvEpoch) + " and  " +  TimeUtils.convertToHumanReadableString(evEpoch), (evEpoch - lastEvEpoch) == 86400);
					}
					lastEvEpoch = evEpoch;
					rtvlEventCount++;
				}
			} else { 
				fail("Stream is null when retrieving data.");
			}
		}		
		assertTrue("We expected event count  " + genEventCount + " but got  " + rtvlEventCount, genEventCount == rtvlEventCount);
		return rtvlEventCount;
	}
	
	@After
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
	}
	
	private void changeMTSForDest() throws Exception {
		JSONObject srcPVTypeInfoJSON = GetUrlContent.getURLContentAsJSONObject("http://localhost:17665/mgmt/bpl/getPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8"));
		JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
		JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
		PVTypeInfo destPVTypeInfo = new PVTypeInfo();
		decoder.decode(srcPVTypeInfoJSON, destPVTypeInfo);
		String otherURL = "pbraw://localhost?name=MTS&rawURL=" + URLEncoder.encode("http://localhost:17669/retrieval/data/getData.raw", "UTF-8");
		destPVTypeInfo.getDataStores()[1] = "merge://localhost?name=MTS&dest="
				+ URLEncoder.encode(destPVTypeInfo.getDataStores()[1], "UTF-8") 
				+ "&other=" + URLEncoder.encode(otherURL, "UTF-8");
		logger.info("Data store is " + destPVTypeInfo.getDataStores()[1]);
		
		GetUrlContent.postObjectAndGetContentAsJSONObject("http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8") + "&override=true&createnew=true", encoder.encode(destPVTypeInfo));
		logger.info("Changed " + pvName + " to a merge dedup plugin");

	}
	
	@SuppressWarnings("unchecked")
	private void testDataAtTime(long epochSecs, boolean morningp) throws Exception {
		String scoreURL = "http://localhost:17665/retrieval/data/getDataAtTime.json?at=" + TimeUtils.convertToISO8601String(epochSecs);
		JSONArray array = new JSONArray();
		array.add(pvName);
		Map<String, Map<String, Object>> ret = (Map<String, Map<String, Object>>) GetUrlContent.postDataAndGetContentAsJSONArray(scoreURL, array);
		assertTrue("We expected some data back from getDataAtTime", ret.size() > 0);
		for(String retpvName : ret.keySet()) {
			Map<String, Object> val = ret.get(retpvName);
			if(retpvName.equals(pvName)) {
				logger.info("Asking for value at " + TimeUtils.convertToISO8601String(epochSecs) + " got value at " + TimeUtils.convertToISO8601String((long)val.get("secs")));
				assertTrue("We expected a morning value for " + TimeUtils.convertToISO8601String(epochSecs) 
				+ " instead we got " + TimeUtils.convertToISO8601String((long)val.get("secs")), (double) val.get("val") == (morningp ? 10 : 20 ));
				return;
			}
		}
		
		assertTrue("We did not receive a value for PV ", false);
	}

	@Test
	public void testRetrieval() throws Exception {
		// Register the PV with both appliances and generate data.
		Timestamp lastMonth = TimeUtils.minusDays(TimeUtils.now(), 31);		
		generateMTSData("http://localhost:17665", "dest_appliance", lastMonth, true);
		generateMTSData("http://localhost:17669", "other_appliance", lastMonth, false);
		
		changeMTSForDest();

		for(int i = 0; i < 20; i++) {
			long startOfDay = (TimeUtils.convertToEpochSeconds(TimeUtils.minusDays(TimeUtils.now(), -1*(i-25)))/86400)*86400;
			for(int h = 0; h < 24; h++) {
				logger.info("Looking for value of PV at  " + TimeUtils.convertToHumanReadableString(startOfDay + h*60*60));
				testDataAtTime(startOfDay + h*60*60, ( (h >=10 && h < 20) ? true : false));
			}			
		}
	}	
}
