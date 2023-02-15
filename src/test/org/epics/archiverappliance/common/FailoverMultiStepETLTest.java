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
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.Callable;

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
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A more complex test for testing ETL for failover.
 * "Other" generates even data for multiple months
 * "dest" generates odd data for multiple months. 
 * We run ETL multiple times... 
 * @author mshankar
 *
 */
@Category(IntegrationTests.class)
public class FailoverMultiStepETLTest {
	private static Logger logger = Logger.getLogger(FailoverMultiStepETLTest.class.getName());
	private ConfigServiceForTests configService;
	String pvName = "FailoverETLTest";
	ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
	TomcatSetup tomcatSetup = new TomcatSetup();
	long stepSeconds = 3600;
	
	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER",  "../sts"); 
		System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", "../mts"); 
		System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER",   "../lts"); 
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());		
	}
	
	@After
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
	}

	/**
	 * Generate a months of data for the other appliance.
	 * @param applURL - The URL for the appliance.
	 * @param applianceName - The name of the appliance
	 * @param startTime - The query start time
	 * @param endTime - The query end time
	 * @param genEventCount - The expected event count.
	 * @throws Exception
	 */
	private long registerPVForOther(String applURL, String applianceName, Timestamp startTime, Timestamp endTime, long genEventCount)
			throws Exception {
		
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
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, startTime, endTime, null)) {
			long lastEvEpoch = 0;
			if(stream != null) {
				for(Event e : stream) {
					long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
					if(lastEvEpoch != 0) {
						assertTrue("We got events more than " + stepSeconds + " seconds apart " + TimeUtils.convertToHumanReadableString(lastEvEpoch) + " and  " +  TimeUtils.convertToHumanReadableString(evEpoch), (evEpoch - lastEvEpoch) == stepSeconds);
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

	private int generateData(String applianceName, Timestamp ts, int startingOffset) throws IOException {
		int genEventCount = 0;
		StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + "tomcat_"+ this.getClass().getSimpleName() + "/" + applianceName + "/mts" + "&partitionGranularity=PARTITION_DAY", configService);
		try(BasicContext context = new BasicContext()) {
			for(long s = TimeUtils.getPreviousPartitionLastSecond(TimeUtils.convertToEpochSeconds(ts), PartitionGranularity.PARTITION_DAY) + 1 + startingOffset;
					s < TimeUtils.getNextPartitionFirstSecond(TimeUtils.convertToEpochSeconds(ts), PartitionGranularity.PARTITION_DAY); 
					s = s + stepSeconds) {
				ArrayListEventStream strm = new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.convertToYearSecondTimestamp(s).getYear()));
				POJOEvent genEvent = new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, TimeUtils.convertFromEpochSeconds(s, 0), new ScalarValue<Double>((double)s), 0, 0);
				strm.add(genEvent);
				genEventCount++;
				plugin.appendData(context, pvName, strm);
			}			
		}		
		return genEventCount;
	}
	
	
	private void changeMTSForDest() throws Exception {
		JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File("src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
		PVTypeInfo destPVTypeInfo = new PVTypeInfo();
		JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
		decoder.decode(srcPVTypeInfoJSON, destPVTypeInfo);
		
		destPVTypeInfo.setPaused(false);
		destPVTypeInfo.setPvName(pvName);
		destPVTypeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
		destPVTypeInfo.setChunkKey(configService.getPVNameToKeyConverter().convertPVNameToKey(pvName));
		destPVTypeInfo.setCreationTime(TimeUtils.convertFromISO8601String("2020-11-11T14:49:58.523Z"));
		destPVTypeInfo.setModificationTime(TimeUtils.now());
		String otherURL = "pbraw://localhost?name=MTS&rawURL=" + URLEncoder.encode("http://localhost:17665/retrieval/data/getData.raw", "UTF-8");
		destPVTypeInfo.getDataStores()[1] = "merge://localhost?name=MTS&dest="
				+ URLEncoder.encode(destPVTypeInfo.getDataStores()[1], "UTF-8") 
				+ "&other=" + URLEncoder.encode(otherURL, "UTF-8");
		configService.updateTypeInfoForPV(pvName, destPVTypeInfo);
		configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
	}
	
	private long testMergedRetrieval(String applianceName, Timestamp startTime, Timestamp endTime, boolean expectContinous) throws Exception {
		long rtvlEventCount = 0;
		long lastEvEpoch = 0;
		StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=LTS&rootFolder=" + "tomcat_"+ this.getClass().getSimpleName() + "/" + applianceName + "/lts" + "&partitionGranularity=PARTITION_YEAR", configService);
		try(BasicContext context = new BasicContext()) {
			logger.info("Looking for data " + plugin.getDescription() + " from " + TimeUtils.convertToHumanReadableString(startTime) + " and " + TimeUtils.convertToHumanReadableString(endTime));
			List<Callable<EventStream>> callables = plugin.getDataForPV(context, pvName, startTime, endTime, new DefaultRawPostProcessor());
			for(Callable<EventStream> callable : callables) {
				EventStream ev = callable.call();
				for(Event e : ev) {
					long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
					logger.debug("Current event " + TimeUtils.convertToHumanReadableString(evEpoch) + " Previous: " + TimeUtils.convertToHumanReadableString(lastEvEpoch));
					if(lastEvEpoch != 0) {
						assertTrue("We got events out of order " + TimeUtils.convertToHumanReadableString(lastEvEpoch) + " and  " +  TimeUtils.convertToHumanReadableString(evEpoch) + " at event count " + rtvlEventCount, evEpoch > lastEvEpoch);
					}
					lastEvEpoch = evEpoch;
					rtvlEventCount++;
				}
			}
		}		
		return rtvlEventCount;
	}

	@Test
	public void testETL() throws Exception {
		configService.getETLLookup().manualControlForUnitTests();
		// Register the PV with both appliances and generate data.
		Timestamp startTime = TimeUtils.minusDays(TimeUtils.now(), 365);		
		Timestamp endTime = TimeUtils.now();		

		long oCount = 0;
		for(Timestamp ts = startTime; ts.before(endTime); ts=TimeUtils.plusDays(ts, 1)) {
			oCount = oCount + generateData(ConfigServiceForTests.TESTAPPLIANCE0, ts, 0);
		}
		registerPVForOther("http://localhost:17665", ConfigServiceForTests.TESTAPPLIANCE0, TimeUtils.minusDays(TimeUtils.now(), 5*365), TimeUtils.plusDays(TimeUtils.now(), 10), oCount);

		System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER",  "tomcat_"+ this.getClass().getSimpleName() + "/" + "dest_appliance" + "/sts"); 
		System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", "tomcat_"+ this.getClass().getSimpleName() + "/" + "dest_appliance" + "/mts"); 
		System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER",   "tomcat_"+ this.getClass().getSimpleName() + "/" + "dest_appliance" + "/lts"); 

		long dCount = 0;
		for(Timestamp ts = startTime; ts.before(endTime); ts=TimeUtils.plusDays(ts, 1)) {
			dCount = dCount + generateData("dest_appliance", ts, 1);
		}
		testMergedRetrieval("dest_appliance", TimeUtils.minusDays(TimeUtils.now(), 5*365), TimeUtils.plusDays(TimeUtils.now(), 10), false);
		long totCount = dCount + oCount;
		
		changeMTSForDest();
		long lastCount = 0;
		for(Timestamp ts = startTime; ts.before(endTime); ts=TimeUtils.plusDays(ts, 1)) {
			Timestamp queryStart = TimeUtils.minusDays(TimeUtils.now(), 5*365), queryEnd = TimeUtils.plusDays(ts, 10);
			Timestamp timeETLruns = TimeUtils.convertFromEpochSeconds(TimeUtils.getNextPartitionFirstSecond(TimeUtils.convertToEpochSeconds(ts), PartitionGranularity.PARTITION_DAY) + 60, 0);
			// Add 3 days to take care of the hold and gather
			timeETLruns = TimeUtils.plusDays(timeETLruns, 3);
	    	logger.info("Running ETL now as if it is " + TimeUtils.convertToHumanReadableString(timeETLruns));
	    	ETLExecutor.runETLs(configService, timeETLruns);
			long rCount = testMergedRetrieval("dest_appliance", queryStart, queryEnd, false);
			logger.info("Got " + rCount + " events between " + TimeUtils.convertToHumanReadableString(queryStart) + " and " + TimeUtils.convertToHumanReadableString(queryEnd));
			assertTrue("We expected more than what we got last time " + lastCount + " . This time we got " + rCount, (rCount >= lastCount));
			lastCount = rCount;			
		}
		assertTrue("We expected event count  " + totCount + " but got  " + lastCount, lastCount == totCount);
	}	
}
