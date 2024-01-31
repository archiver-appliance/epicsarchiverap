/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
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
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;

/**
 * Test retrieval after the dest appserver has been upgraded.
 * Assume that only the MTS has a mergededup plugin for failover. 
 * What happens when we restart dest? 
 * Let's assume this is the sample sequence; T0 is older and T10 is the latest
 * <pre>
 *    Day    Dest                           Other
 *     1      T0                             T0
 *     2      T1                             T1
 *     3      T2                             T2
 * -------------- MTS above STS below  -----------      
 *     4      T3                             T3
 *     5                                     T4
 *     6                                     T5
 *     7                                     T6
 *     8                                     T7
 *     9      T8                             T8
 *     10     T9                             T9
 *     11     T10                            T10
 * </pre>
 * @author mshankar
 *  Ideally, regardless of what the start time and end time are we should get all 10 data points.
 *  We generate data for the start of the year one per day; the first three samples are in the MTS and then rest are in the STS. 
 *  If we only mergededup the MTS, then there is a small window where the results from "others" are not included.
 *  To cater to this, either use a mergededup for the STS as well or simply ask for data that is certain to start from the MTS.  
 *
 */
@Tag("integration")
public class FailoverUpgradeTest {
	private static Logger logger = LogManager.getLogger(FailoverUpgradeTest.class.getName());
	private ConfigServiceForTests configService;
    String pvName = "FailoverUpgradeTest";
	ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
	TomcatSetup tomcatSetup = new TomcatSetup();
	
	@BeforeEach
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(-1);
		tomcatSetup.setUpFailoverWithWebApps(this.getClass().getSimpleName());		
	}

	/**
	 * Generate one sample per day; T0-T3 in the STS and the rest in the MTS. 
	 * Skip sample T4-T7 for the dest appliance.
	 * @param applURL - The URL for the appliance.
	 * @param applianceName - The name of the appliance
	 * @param sampleStart - The month we generate data for. We generate a month's worth of MTS data.
	 * @throws Exception
	 */
	private long generateData(String applURL, String applianceName, long sampleStart)
			throws Exception {
		int genEventCount = 0;
		StoragePlugin mts = StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + "build/tomcats/tomcat_"+ this.getClass().getSimpleName() + "/" + applianceName + "/mts" + "&partitionGranularity=PARTITION_DAY", configService);
		try(BasicContext context = new BasicContext()) {
			ArrayListEventStream strm = new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.convertToYearSecondTimestamp(sampleStart).getYear()));
			for(int i = 0; i < 3; i++) {
				long sampleTime = sampleStart + i*86400 + 10*3600; // 10AM into the day
				POJOEvent pojoEvent = new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, TimeUtils.convertFromEpochSeconds(sampleTime, 0), new ScalarValue<Double>((double)i), 0, 0);
				logger.debug("Generating event at " + TimeUtils.convertToHumanReadableString(pojoEvent.getEventTimeStamp()));
				strm.add(pojoEvent);
				genEventCount++;
			}
			mts.appendData(context, pvName, strm);
		}
		
		StoragePlugin sts = StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + "build/tomcats/tomcat_"+ this.getClass().getSimpleName() + "/" + applianceName + "/sts" + "&partitionGranularity=PARTITION_HOUR", configService);
		try(BasicContext context = new BasicContext()) {
			ArrayListEventStream strm = new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.convertToYearSecondTimestamp(sampleStart).getYear()));
			for(int i = 3; i <= 10; i++) {
				if(applianceName.equals("dest_appliance") && (i >= 4 && i <= 7)) { continue; }
				long sampleTime = sampleStart + i*86400 + 10*3600; // 10AM into the day
				POJOEvent pojoEvent = new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, TimeUtils.convertFromEpochSeconds(sampleTime, 0), new ScalarValue<Double>((double)i), 0, 0);
				logger.debug("Generating event at " + TimeUtils.convertToHumanReadableString(pojoEvent.getEventTimeStamp()));
				strm.add(pojoEvent);
				genEventCount++;
			}
			sts.appendData(context, pvName, strm);
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
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, TimeUtils.minusDays(TimeUtils.now(), 2*365), TimeUtils.plusDays(TimeUtils.now(), 31), null)) {
			long lastEvEpoch = 0;
			if(stream != null) {
				for(Event e : stream) {
					long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
					if(lastEvEpoch != 0) {
						Assertions.assertTrue(evEpoch > lastEvEpoch, "We got events out of order " + TimeUtils.convertToHumanReadableString(lastEvEpoch) + " and  " +  TimeUtils.convertToHumanReadableString(evEpoch) + " at event count " + rtvlEventCount);
					}
					lastEvEpoch = evEpoch;
					rtvlEventCount++;
				}
			} else { 
				Assertions.fail("Stream is null when retrieving data.");
			}
		}		
		Assertions.assertTrue(genEventCount == rtvlEventCount, "We expected event count  " + genEventCount + " but got  " + rtvlEventCount);
		return genEventCount;
	}
	
	@AfterEach
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
	}
	
	private void changeMTSForDest() throws Exception {
		JSONObject srcPVTypeInfoJSON = GetUrlContent.getURLContentAsJSONObject("http://localhost:17665/mgmt/bpl/getPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8"));
		JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
		JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
		PVTypeInfo destPVTypeInfo = new PVTypeInfo();
		decoder.decode(srcPVTypeInfoJSON, destPVTypeInfo);
		{
			String otherURL = "pbraw://localhost?name=MTS&rawURL=" + URLEncoder.encode("http://localhost:17669/retrieval/data/getData.raw", "UTF-8");
			destPVTypeInfo.getDataStores()[1] = "merge://localhost?name=MTS&dest="
					+ URLEncoder.encode(destPVTypeInfo.getDataStores()[1], "UTF-8") 
					+ "&other=" + URLEncoder.encode(otherURL, "UTF-8");
			logger.info("Data store is " + destPVTypeInfo.getDataStores()[1]);
		}
		
		GetUrlContent.postObjectAndGetContentAsJSONObject("http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8") + "&override=true&createnew=true", encoder.encode(destPVTypeInfo));
		logger.info("Changed " + pvName + " to a merge dedup plugin");

	}

	private void changeSTSForDest() throws Exception {
		JSONObject srcPVTypeInfoJSON = GetUrlContent.getURLContentAsJSONObject("http://localhost:17665/mgmt/bpl/getPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8"));
		JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
		JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
		PVTypeInfo destPVTypeInfo = new PVTypeInfo();
		decoder.decode(srcPVTypeInfoJSON, destPVTypeInfo);
		{
			String otherURL = "pbraw://localhost?name=STS&rawURL=" + URLEncoder.encode("http://localhost:17669/retrieval/data/getData.raw", "UTF-8");
			destPVTypeInfo.getDataStores()[0] = "merge://localhost?name=STS&dest="
					+ URLEncoder.encode(destPVTypeInfo.getDataStores()[0], "UTF-8") 
					+ "&other=" + URLEncoder.encode(otherURL, "UTF-8");
		}
		GetUrlContent.postObjectAndGetContentAsJSONObject("http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8") + "&override=true&createnew=true", encoder.encode(destPVTypeInfo));
		logger.info("Changed " + pvName + " to a merge dedup plugin");

	}
	
	private void testDataForRange(long start, long end, int expectedCount) throws Exception {
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:17665/retrieval/data/getData.raw");
		long rtvlEventCount = 0;
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, TimeUtils.convertFromEpochSeconds(start, 0), TimeUtils.convertFromEpochSeconds(end, 0), null)) {
			long lastEvEpoch = 0;
			if(stream != null) {
				for(Event e : stream) {
					long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
					logger.info("Current event " + TimeUtils.convertToISO8601String(evEpoch)
							+ " for start " + TimeUtils.convertToISO8601String(start) + " and end " + TimeUtils.convertToISO8601String(end));
					if(lastEvEpoch != 0) {
						Assertions.assertTrue(evEpoch > lastEvEpoch, "We got events out of order " + TimeUtils.convertToHumanReadableString(lastEvEpoch) + " and  " +  TimeUtils.convertToHumanReadableString(evEpoch) + " at event count " + rtvlEventCount);
					}
					lastEvEpoch = evEpoch;
					rtvlEventCount++;
				}
			} else { 
				Assertions.fail("Stream is null when retrieving data.");
			}
		}		
		Assertions.assertTrue(expectedCount == rtvlEventCount, "We expected event count  " + expectedCount + " but got  " + rtvlEventCount
				+ " for start " + TimeUtils.convertToISO8601String(start) + " and end " + TimeUtils.convertToISO8601String(end));
	}

	@Test
	public void testRetrieval() throws Exception {
		// Register the PV with both appliances and generate data.
		long sampleStart = TimeUtils.getStartOfCurrentYearInSeconds();		
		generateData("http://localhost:17665", "dest_appliance", sampleStart);
		generateData("http://localhost:17669", "other_appliance", sampleStart);
		
		changeMTSForDest();
		
		for(int i = 0; i <=10; i++) {
			testDataForRange(sampleStart, sampleStart+i*86400, i);
		}
		changeSTSForDest();
		for(int i = 0; i <=10; i++) {
			testDataForRange(sampleStart+i*86400, sampleStart+11*86400, (i == 0 ? 11 : 12-i));
		}
	}	
}
