/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import static org.junit.Assert.assertTrue;

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
import org.epics.archiverappliance.SlowTests;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test basic failover - test the ETL side of things when the other server is down...
 * @author mshankar
 *
 */
public class FailoverETLServerDownTest {
	private static Logger logger = Logger.getLogger(FailoverETLServerDownTest.class.getName());
	private ConfigServiceForTests configService;
	String pvName = "FailoverETLTest";
	ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
	long tCount = 0;
	long stepSeconds = 2;
	
	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
	}

	private int generateData(String applianceName, Timestamp lastMonth, int startingOffset) throws IOException {
		int genEventCount = 0;
		StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + "tomcat_"+ this.getClass().getSimpleName() + "/" + applianceName + "/mts" + "&partitionGranularity=PARTITION_DAY", configService);
		try(BasicContext context = new BasicContext()) {
			for(long s = TimeUtils.getPreviousPartitionLastSecond(TimeUtils.convertToEpochSeconds(lastMonth), PartitionGranularity.PARTITION_MONTH) + 1 + startingOffset; // We generate a months worth of data.
					s < TimeUtils.getNextPartitionFirstSecond(TimeUtils.convertToEpochSeconds(lastMonth), PartitionGranularity.PARTITION_MONTH); 
					s = s + stepSeconds) {
				ArrayListEventStream strm = new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.convertToYearSecondTimestamp(s).getYear()));
				strm.add(new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, TimeUtils.convertFromEpochSeconds(s, 0), new ScalarValue<Double>((double)s), 0, 0));
				genEventCount++;
				plugin.appendData(context, pvName, strm);
			}			
		}		
		logger.info("Done generating dest data");
		return genEventCount;
	}
	
	@After
	public void tearDown() throws Exception {
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
	
	private long testMergedRetrieval(String pluginURL, Timestamp startTime, Timestamp endTime) throws Exception {
		long rtvlEventCount = 0;
		long lastEvEpoch = 0;
		StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(pluginURL, configService);
		try(BasicContext context = new BasicContext()) {
			List<Callable<EventStream>> callables = plugin.getDataForPV(context, pvName, startTime, endTime, new DefaultRawPostProcessor());
			for(Callable<EventStream> callable : callables) {
				EventStream ev = callable.call();
				logger.error("Event Stream " + ev.getDescription());
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
	@Category(SlowTests.class)
	public void testETL() throws Exception {
		configService.getETLLookup().manualControlForUnitTests();
		// Register the PV with both appliances and generate data.
		Timestamp lastMonth = TimeUtils.minusDays(TimeUtils.now(), 31);		

		System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER",  "tomcat_"+ this.getClass().getSimpleName() + "/" + "dest_appliance" + "/sts"); 
		System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", "tomcat_"+ this.getClass().getSimpleName() + "/" + "dest_appliance" + "/mts"); 
		System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER",   "tomcat_"+ this.getClass().getSimpleName() + "/" + "dest_appliance" + "/lts"); 
		long dCount = generateData("dest_appliance", lastMonth, 1);

		tCount = dCount;
		
		changeMTSForDest();
		Timestamp timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 365*10);
    	logger.info("Running ETL now as if it is " + TimeUtils.convertToHumanReadableString(timeETLruns));
    	ETLExecutor.runETLs(configService, timeETLruns);
    	
    	
    	logger.info("Checking merged data after running ETL");
		long lCount = testMergedRetrieval("pb://localhost?name=LTS&rootFolder=" + "tomcat_"+ this.getClass().getSimpleName() + "/" + "dest_appliance" + "/lts" + "&partitionGranularity=PARTITION_YEAR", TimeUtils.minusDays(TimeUtils.now(), 365*2), TimeUtils.plusDays(TimeUtils.now(), 365*2));		
		assertTrue("We expected LTS to have failed " + lCount, lCount == 0);
		long mCount = testMergedRetrieval("pb://localhost?name=MTS&rootFolder=" + "tomcat_"+ this.getClass().getSimpleName() + "/" + "dest_appliance" + "/mts" + "&partitionGranularity=PARTITION_DAY", TimeUtils.minusDays(TimeUtils.now(), 365*2), TimeUtils.plusDays(TimeUtils.now(), 365*2));		
		assertTrue("We expected MTS to have the same amount of data " + tCount + " instead we got " + mCount, mCount == tCount);

	}	
}
