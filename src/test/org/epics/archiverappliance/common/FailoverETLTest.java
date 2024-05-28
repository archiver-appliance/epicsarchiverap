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
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
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

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Test basic failover - test the ETL side of things.
 * @author mshankar
 *
 */
@Tag("integration")
public class FailoverETLTest {
	private static final Logger logger = LogManager.getLogger(FailoverETLTest.class.getName());
	private ConfigServiceForTests configService;
	String pvName = "FailoverETLTest";
	ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
	TomcatSetup tomcatSetup = new TomcatSetup();
	long tCount = 0;
	long stepSeconds = 2;
	
	@BeforeEach
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(-1);
		System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER",  "../sts"); 
		System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", "../mts"); 
		System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER",   "../lts"); 
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());		
	}

	/**
	 * Generate a months worth of data.
	 * @param applURL - The URL for the appliance.
	 * @param applianceName - The name of the appliance
	 * @param lastMonth - The month we generate data for. We generate a month's worth of MTS data.
	 * @param startingOffset - Use 0 for even seconds; 1 for odd seconds. When merged, we test to make sure; we get data one second apart.
	 * @throws Exception
	 */
    private long generateDataAndRegisterPV(String applURL, String applianceName, Instant lastMonth, int startingOffset)
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

		int genEventCount = generateData(applianceName, lastMonth, startingOffset);


		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(applURL + "/retrieval/data/getData.raw");
		long rtvlEventCount = 0;
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, TimeUtils.minusDays(TimeUtils.now(), 90), TimeUtils.plusDays(TimeUtils.now(), 31), null)) {
			long lastEvEpoch = 0;
			if(stream != null) {
				for(Event e : stream) {
					long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
					if(lastEvEpoch != 0) {
						Assertions.assertTrue((evEpoch - lastEvEpoch) == stepSeconds, "We got events more than " + stepSeconds + " seconds apart " + TimeUtils.convertToHumanReadableString(lastEvEpoch) + " and  " +  TimeUtils.convertToHumanReadableString(evEpoch));
					}
					lastEvEpoch = evEpoch;
					rtvlEventCount++;
				}
			} else { 
				Assertions.fail("Stream is null when retrieving data.");
			}
		}		
		Assertions.assertTrue(genEventCount == rtvlEventCount, "We expected event count  " + genEventCount + " but got  " + rtvlEventCount);
		return rtvlEventCount;
	}

    private int generateData(String applianceName, Instant lastMonth, long startingOffset) throws IOException {
		int genEventCount = 0;
		PlainStoragePlugin plugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + "build/tomcats/tomcat_"+ this.getClass().getSimpleName() + "/" + applianceName + "/mts" + "&partitionGranularity=PARTITION_DAY", configService);
		try(BasicContext context = new BasicContext()) {
			ArrayListEventStream strm = new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.convertToYearSecondTimestamp(lastMonth).getYear()));
			Instant start = TimeUtils.getPreviousPartitionLastSecond(lastMonth, PartitionGranularity.PARTITION_MONTH).plusSeconds(stepSeconds / 2 + startingOffset);
			Instant end = TimeUtils.getNextPartitionFirstSecond(lastMonth, PartitionGranularity.PARTITION_MONTH);
			for (Instant s = start; // We generate a months worth of data.
                 s.isBefore(end);
                 s = s.plusSeconds(stepSeconds)) {
                strm.add(new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, s, new ScalarValue<Double>((double) s.getEpochSecond()), 0, 0));
				genEventCount++;
			}
			plugin.appendData(context, pvName, strm);

		}		
		logger.info("Done generating dest data");
		return genEventCount;
	}
	
	@AfterEach
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
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
		configService.getETLLookup().manualControlForUnitTests();

	}

    private long testMergedRetrieval(String applianceName, Instant startTime, Instant endTime) throws Exception {
		long rtvlEventCount = 0;
		long lastEvEpoch = 0;
		StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=LTS&rootFolder=" + "build/tomcats/tomcat_"+ this.getClass().getSimpleName() + "/" + applianceName + "/lts" + "&partitionGranularity=PARTITION_YEAR", configService);
		try(BasicContext context = new BasicContext()) {
			List<Callable<EventStream>> callables = plugin.getDataForPV(context, pvName, startTime, endTime, new DefaultRawPostProcessor());
			Assertions.assertTrue(callables.size() > 0, "We got zero callables");
			for(Callable<EventStream> callable : callables) {
				EventStream ev = callable.call();
				logger.error("Event Stream " + ev.getDescription());
				for(Event e : ev) {
					long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
					logger.debug("Current event " + TimeUtils.convertToHumanReadableString(evEpoch) + " Previous: " + TimeUtils.convertToHumanReadableString(lastEvEpoch));
					if(lastEvEpoch != 0) {
						Assertions.assertTrue(evEpoch > lastEvEpoch, "We got events out of order " + TimeUtils.convertToHumanReadableString(lastEvEpoch) + " and  " +  TimeUtils.convertToHumanReadableString(evEpoch) + " at event count " + rtvlEventCount);
						Assertions.assertTrue((evEpoch - lastEvEpoch) == stepSeconds / 2, "We got events more than a second apart " + TimeUtils.convertToHumanReadableString(lastEvEpoch) + " and  " +  TimeUtils.convertToHumanReadableString(evEpoch) + " at event count " + rtvlEventCount);
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
        Instant lastMonth = TimeUtils.minusDays(TimeUtils.now(), 2*31);
		long oCount = generateDataAndRegisterPV("http://localhost:17665", ConfigServiceForTests.TESTAPPLIANCE0, lastMonth, 0);

		System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER",  "build/tomcats/tomcat_"+ this.getClass().getSimpleName() + "/" + "dest_appliance" + "/sts");
		System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", "build/tomcats/tomcat_"+ this.getClass().getSimpleName() + "/" + "dest_appliance" + "/mts");
		System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER",   "build/tomcats/tomcat_"+ this.getClass().getSimpleName() + "/" + "dest_appliance" + "/lts");
		long dCount = generateData("dest_appliance", lastMonth, 1);

		tCount = dCount + oCount;
		
		changeMTSForDest();
        Instant timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 365 * 10);
    	logger.info("Running ETL now as if it is " + TimeUtils.convertToHumanReadableString(timeETLruns));
    	ETLExecutor.runETLs(configService, timeETLruns);
    	
    	
    	logger.info("Checking merged data after running ETL");
		long rCount = testMergedRetrieval("dest_appliance", TimeUtils.minusDays(TimeUtils.now(), 365*2), TimeUtils.plusDays(TimeUtils.now(), 365*2));		
		Assertions.assertTrue(tCount == rCount, "We expected event count  " + tCount + " but got  " + rCount);

	}	
}
