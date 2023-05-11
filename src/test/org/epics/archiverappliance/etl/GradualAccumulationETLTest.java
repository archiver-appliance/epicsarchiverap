/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SlowTests;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

/**
 * Complex test for testing ETL as it is supposed to happen in production.
 * @author mshankar
 *
 */
public class GradualAccumulationETLTest {
	private static Logger logger = LogManager.getLogger(GradualAccumulationETLTest.class.getName());

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	
	/**
	 * In a running appserver, the sequence is expected to be as follows
	 * <ol>
	 * <li>The engine generates data for a PV at a certain rate.</li>
	 * <li>ETL runs every so often and copies a subset (the older portion) of the data over to the destination store.</li>
	 * <li>The engine continues to generate data.</li>
	 * </ol>
	 * This aims to test that. We generate data and then fake the time for ETL
	 * @throws Exception
	 */
	@Test
	@Category(SlowTests.class)
	public void testGradualAccumulation() throws Exception {
		for(PartitionGranularity granularity : PartitionGranularity.values()) {
			if(granularity.getNextLargerGranularity() == null) continue;
			testGradualAccumulation(granularity, true);
			testGradualAccumulation(granularity, false);
		}
	}
	
	private void testGradualAccumulation(PartitionGranularity granularity, boolean backUpfiles) throws Exception {
		PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin();
		PBCommonSetup srcSetup = new PBCommonSetup();
		PlainPBStoragePlugin etlDest = new PlainPBStoragePlugin();
		PBCommonSetup destSetup = new PBCommonSetup();
		DefaultConfigService configService = new ConfigServiceForTests(new File("./bin"), 1);
		etlDest.setBackupFilesBeforeETL(backUpfiles);

		srcSetup.setUpRootFolder(etlSrc, "GradualAccumulationETLTestSrc_"+granularity, granularity);
		destSetup.setUpRootFolder(etlDest, "GradualAccumulationETLTestDest"+granularity, granularity.getNextLargerGranularity());

		logger.info("Testing gradual accumulation for " + etlSrc.getPartitionGranularity() + " to " + etlDest.getPartitionGranularity() + " with backup = " + Boolean.toString(backUpfiles));

		short year = TimeUtils.getCurrentYear();
		long startOfYearInEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		long curEpochSeconds = startOfYearInEpochSeconds; 
		int secondsintoyear = 0;
		int incrementSeconds = 450;
		int eventsgenerated = 0;

		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testGradual" + etlSrc.getPartitionGranularity();

		PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		String[] dataStores = new String[] { etlSrc.getURLRepresentation(), etlDest.getURLRepresentation() }; 
		typeInfo.setDataStores(dataStores);
		configService.updateTypeInfoForPV(pvName, typeInfo);
		configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
		configService.getETLLookup().manualControlForUnitTests();

		while(secondsintoyear < 60*60*24*365) {
			// The 60x60X20 is to generate 20 hours worth of data in each shot.
			int eventsPerShot = Math.max(60*60*20, granularity.getApproxSecondsPerChunk()*2)/incrementSeconds;
			ArrayListEventStream instream = new ArrayListEventStream(eventsPerShot, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
			for(int i = 0; i < eventsPerShot; i++) {
				instream.add(new SimulationEvent(secondsintoyear, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>((double)secondsintoyear)));
				secondsintoyear += incrementSeconds;
				curEpochSeconds += incrementSeconds;
				eventsgenerated++;
			}
			try(BasicContext context = new BasicContext()) {
				etlSrc.appendData(context, pvName, instream);
			}
			ETLExecutor.runETLs(configService, TimeUtils.convertFromEpochSeconds(curEpochSeconds, 0));
			logger.debug("Done performing ETL");
			checkDataValidity(pvName, etlSrc, etlDest, startOfYearInEpochSeconds, incrementSeconds, eventsgenerated, granularity.toString() + "/" + Boolean.toString(backUpfiles));
		}
		

		srcSetup.deleteTestFolder();
		destSetup.deleteTestFolder();
		configService.shutdownNow();
	}
	
	private void checkDataValidity(String pvName, PlainPBStoragePlugin etlSrc, PlainPBStoragePlugin etlDest, long startOfYearInEpochSeconds, int incrementSeconds, int eventsgenerated, String testDesc) throws IOException {
		Timestamp startOfRequest = TimeUtils.minusDays(TimeUtils.now(), 2*366);
		Timestamp endOfRequest = TimeUtils.plusDays(TimeUtils.now(), 2*366);

		logger.debug(testDesc + "Asking for data between" 
				+ TimeUtils.convertToHumanReadableString(startOfRequest) 
				+ " and " 
				+ TimeUtils.convertToHumanReadableString(endOfRequest)
				);

		long expectedEpochSeconds = startOfYearInEpochSeconds;
		int afterCount = 0;
		
		try (BasicContext context = new BasicContext(); EventStream afterDest = new CurrentThreadWorkerEventStream(pvName, etlDest.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
			if(afterDest != null) { 
				for(Event e : afterDest) {
					assertTrue(testDesc + "Expected seconds " + TimeUtils.convertToHumanReadableString(expectedEpochSeconds) + " is not the same as actual seconds " + TimeUtils.convertToHumanReadableString(e.getEpochSeconds()), (expectedEpochSeconds == e.getEpochSeconds()));
					expectedEpochSeconds += incrementSeconds;
					afterCount++;
				}
				assertTrue(testDesc + "Seems like no events were moved by ETL " + afterCount, (afterCount != 0));
			}
		}
		
		
		try (BasicContext context = new BasicContext(); EventStream afterSrc = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
			if(afterSrc != null) {
				for(Event e : afterSrc) { 
					assertTrue(testDesc + "Expected seconds " + TimeUtils.convertToHumanReadableString(expectedEpochSeconds) + " is not the same as actual seconds " + TimeUtils.convertToHumanReadableString(e.getEpochSeconds()), (expectedEpochSeconds == e.getEpochSeconds()));
					expectedEpochSeconds += incrementSeconds;
					afterCount++;
				}
			}
		}
		
		assertTrue(testDesc + "Expected total events " + eventsgenerated + " is not the same as actual events " + afterCount, (eventsgenerated == afterCount));
	}
}
