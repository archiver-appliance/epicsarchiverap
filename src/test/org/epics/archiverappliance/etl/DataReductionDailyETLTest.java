/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import java.io.File;
import java.sql.Timestamp;
import java.util.LinkedList;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessors;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

/**
 * More complicated data reduction test case.
 * We have two PV's, one with reduced data and the other without.
 * We mimic data generation and ETL as it would occur in the appliance.
 * Finally, we compare the reduced data with the raw data + operator. 
 * @author mshankar
 *
 */
public class DataReductionDailyETLTest extends TestCase {
	private static final Logger logger = Logger.getLogger(DataReductionDailyETLTest.class);
	String shortTermFolderName=ConfigServiceForTests.getDefaultShortTermFolder()+"/shortTerm";
	String mediumTermFolderName=ConfigServiceForTests.getDefaultPBTestFolder()+"/mediumTerm";
	String longTermFolderName=ConfigServiceForTests.getDefaultPBTestFolder()+"/longTerm";
	private  ConfigServiceForTests configService;
	private String rawPVName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + DataReductionDailyETLTest.class.getSimpleName();
	private String reducedPVName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + DataReductionDailyETLTest.class.getSimpleName() + "reduced";
	private String reduceDataUsing = "firstSample_3600";

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		if(new File(shortTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(shortTermFolderName));
		}
		if(new File(mediumTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(mediumTermFolderName));
		}
		if(new File(longTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(longTermFolderName));
		}
	}

	@After
	public void tearDown() throws Exception {
		if(new File(shortTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(shortTermFolderName));
		}
		if(new File(mediumTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(mediumTermFolderName));
		}
		if(new File(longTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(longTermFolderName));
		}
	}

	/**
	 * 1) Set up the raw and reduced PV's
	 * 2) Generate data in STS
	 * 3) Run ETL
	 * 4) Compare
	 */
	@Test
	public void testReducedETL() throws Exception {
		// Set up the raw and reduced PV's
		PlainPBStoragePlugin etlSTS = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_HOUR", configService);
		PlainPBStoragePlugin etlMTS = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + mediumTermFolderName + "/&partitionGranularity=PARTITION_DAY", configService);
		PlainPBStoragePlugin etlLTS = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=LTS&rootFolder=" + longTermFolderName + "/&partitionGranularity=PARTITION_YEAR&reducedata=" + reduceDataUsing, configService);
		{ 
			PVTypeInfo typeInfo = new PVTypeInfo(rawPVName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			String[] dataStores = new String[] { etlSTS.getURLRepresentation(), etlMTS.getURLRepresentation(), etlLTS.getURLRepresentation() }; 
			typeInfo.setDataStores(dataStores);
			configService.updateTypeInfoForPV(rawPVName, typeInfo);
			configService.registerPVToAppliance(rawPVName, configService.getMyApplianceInfo());
		}
		{ 
			PVTypeInfo typeInfo = new PVTypeInfo(reducedPVName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			String[] dataStores = new String[] { etlSTS.getURLRepresentation(), etlMTS.getURLRepresentation(), etlLTS.getURLRepresentation() }; 
			typeInfo.setDataStores(dataStores);
			configService.updateTypeInfoForPV(reducedPVName, typeInfo);
			configService.registerPVToAppliance(reducedPVName, configService.getMyApplianceInfo());
		}
		// Control ETL manually
		configService.getETLLookup().manualControlForUnitTests();

		short currentYear = TimeUtils.getCurrentYear();

		for(int day = 0; day < 365; day++) { 
			// Generate data into the STS on a daily basis
			ArrayListEventStream genDataRaw = new ArrayListEventStream(86400, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, rawPVName, currentYear));
			ArrayListEventStream genDataReduced = new ArrayListEventStream(86400, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, reducedPVName, currentYear));
			for(int second = 0; second < 86400; second++) { 
				YearSecondTimestamp ysts = new YearSecondTimestamp(currentYear, day*86400 + second, 0);
				Timestamp ts = TimeUtils.convertFromYearSecondTimestamp(ysts);
				genDataRaw.add(new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<Double>(second*1.0),0, 0));
				genDataReduced.add(new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<Double>(second*1.0),0, 0));
			}

			try(BasicContext context = new BasicContext()) {
				etlSTS.appendData(context, rawPVName, genDataRaw);
				etlSTS.appendData(context, reducedPVName, genDataReduced);
			}        	
			logger.debug("Done generating data into the STS for day " + day);

			// Run ETL at the end of the day
			Timestamp timeETLruns = TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(currentYear, day*86400 + 86399, 0));
			ETLExecutor.runETLs(configService, timeETLruns);
			logger.debug("Done performing ETL as though today is " + TimeUtils.convertToHumanReadableString(timeETLruns));

			// Compare data for raw+postprocessor and reduced PV's.
			PostProcessor postProcessor = PostProcessors.findPostProcessor(reduceDataUsing);
			postProcessor.initialize(reduceDataUsing, rawPVName);

			int rawWithPPCount = 0;
			int reducedCount = 0;

			try (BasicContext context = new BasicContext()) { 
				Timestamp startTime = TimeUtils.minusDays(TimeUtils.now(), 10*366);
				Timestamp endTime = TimeUtils.plusDays(TimeUtils.now(), 10*366);
				LinkedList<Timestamp> rawTimestamps = new LinkedList<Timestamp>();
				LinkedList<Timestamp> reducedTimestamps = new LinkedList<Timestamp>();
				try(EventStream rawWithPP = new CurrentThreadWorkerEventStream(rawPVName, etlLTS.getDataForPV(context, rawPVName, startTime, endTime, postProcessor))) {
					for(Event e : rawWithPP) {
						rawTimestamps.add(e.getEventTimeStamp());
						rawWithPPCount++; 
					} 
				}
				try(EventStream reduced = new CurrentThreadWorkerEventStream(reducedPVName, etlLTS.getDataForPV(context, reducedPVName, startTime, endTime))) {
					for(Event e : reduced) {
						reducedTimestamps.add(e.getEventTimeStamp());
						reducedCount++; 
					} 
				}
				
				logger.debug("For day " + day + " we have " + rawWithPPCount + " rawWithPP events and " + reducedCount + " reduced events");
				if(rawTimestamps.size() != reducedTimestamps.size()) { 
					while(!rawTimestamps.isEmpty() || !reducedTimestamps.isEmpty()) { 
						if(!rawTimestamps.isEmpty()) logger.info("Raw/PP " + TimeUtils.convertToHumanReadableString(rawTimestamps.pop()));
						if(!reducedTimestamps.isEmpty()) logger.info("Reduced" + TimeUtils.convertToHumanReadableString(reducedTimestamps.pop()));
					}
				}
				assertTrue("For day " + day + " we have " + rawWithPPCount + " rawWithPP events and " + reducedCount + " reduced events", rawWithPPCount == reducedCount);
			}
			if(day > 2) { 
				assertTrue("For day " + day + ", seems like no events were moved by ETL into LTS for " + rawPVName + " Count = " + rawWithPPCount, (rawWithPPCount != 0));
				assertTrue("For day " + day + ", seems like no events were moved by ETL into LTS for " + reducedPVName + " Count = " + reducedCount, (reducedCount != 0));
			}

		}        	
	}
}
