/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.etl.ETLInfo;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.blackhole.BlackholeStoragePlugin;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;

/**
 * Test the ETL hold and gather logic
 * @author mshankar
 *
 */
public class HoldAndGatherTest {
	private static Logger logger = Logger.getLogger(HoldAndGatherTest.class.getName());
	PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin();
	PBCommonSetup srcSetup = new PBCommonSetup();

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testHoldAndGather() throws Exception {
		testHoldAndGather(PartitionGranularity.PARTITION_5MIN, 7, 5);
		testHoldAndGather(PartitionGranularity.PARTITION_15MIN, 7, 5);
		testHoldAndGather(PartitionGranularity.PARTITION_30MIN, 7, 5);
		testHoldAndGather(PartitionGranularity.PARTITION_HOUR, 7, 5);
		testHoldAndGather(PartitionGranularity.PARTITION_DAY, 7, 5);
		testHoldAndGather(PartitionGranularity.PARTITION_DAY, 5, 3);
		testHoldAndGather(PartitionGranularity.PARTITION_MONTH, 2, 1);
	}
	
	/**
	 * We generate data in chunks for a year.
	 * At each chunk we predict how many ETL streams we should get. 
	 * @param granularity
	 * @throws IOException
	 */
	private void testHoldAndGather(PartitionGranularity granularity, int hold, int gather)  throws Exception {
		PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin();
		PBCommonSetup srcSetup = new PBCommonSetup();
		ConfigServiceForTests configService = new ConfigServiceForTests(new File("./bin"));
		srcSetup.setUpRootFolder(etlSrc, "ETLHoldGatherTest_"+granularity, granularity);
		
		etlSrc.setHoldETLForPartions(hold);
		etlSrc.setGatherETLinPartitions(gather);
		
		BlackholeStoragePlugin etlDest = new BlackholeStoragePlugin();
		
		logger.info("Testing ETL hold gather for " + etlSrc.getPartitionGranularity());

		short year = TimeUtils.getCurrentYear();
		long startOfYearInEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		long curEpochSeconds = startOfYearInEpochSeconds; 
		int secondsintoyear = 0;
		int incrementSeconds = 10;

		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_hold_gather" + etlSrc.getPartitionGranularity();
		PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		String[] dataStores = new String[] { etlSrc.getURLRepresentation(), etlDest.getURLRepresentation() }; 
		typeInfo.setDataStores(dataStores);
		configService.updateTypeInfoForPV(pvName, typeInfo);
		configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
		
		configService.getETLLookup().manualControlForUnitTests();


		while(secondsintoyear < 60*60*24*366) {
			int eventsPerShot = (granularity.getApproxSecondsPerChunk())/incrementSeconds;
			ArrayListEventStream instream = new ArrayListEventStream(eventsPerShot, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
			for(int i = 0; i < eventsPerShot; i++) {
				instream.add(new SimulationEvent(secondsintoyear, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>((double)secondsintoyear)));
				secondsintoyear += incrementSeconds;
				curEpochSeconds += incrementSeconds;
			}
			try(BasicContext context = new BasicContext()) {
				etlSrc.appendData(context, pvName, instream);
			}

			List<ETLInfo> etlStreams = etlSrc.getETLStreams(pvName, TimeUtils.convertFromEpochSeconds(curEpochSeconds, 0), new ETLContext());
			assertTrue("At " + TimeUtils.convertToISO8601String(curEpochSeconds) + " we have " + ((etlStreams == null) ? "null" : Integer.toString(etlStreams.size())) + " for " + granularity.toString() + " hold = " + hold + " gather = " + gather, (etlStreams == null) || (etlStreams.size() == 0) || (etlStreams.size() == (gather)));
			ETLExecutor.runETLs(configService, TimeUtils.convertFromEpochSeconds(curEpochSeconds, 0));
		}

		srcSetup.deleteTestFolder();
	}
}