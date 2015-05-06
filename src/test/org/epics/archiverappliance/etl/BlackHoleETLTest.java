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
import java.nio.file.Path;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.blackhole.BlackholeStoragePlugin;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;

/**
 * Test pouring data into the black hole plugin.
 * @author mshankar
 *
 */
public class BlackHoleETLTest {
	private static Logger logger = Logger.getLogger(GradualAccumulationETLTest.class.getName());
	private ConfigService configService;

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
	}

	@After
	public void tearDown() throws Exception {
	}

	
	/**
	 * Variant of the gradual accumulation test where the destination is a black hole plugin
	 * @throws Exception
	 */
	@Test
	public void testBlackHoleETL() throws Exception {
		for(PartitionGranularity granularity : PartitionGranularity.values()) {
			if(granularity.getNextLargerGranularity() == null) continue;
			testBlackHoleETL(granularity);
		}
	}
	
	private void testBlackHoleETL(PartitionGranularity granularity) throws Exception {
		PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin();
		PBCommonSetup srcSetup = new PBCommonSetup();
		BlackholeStoragePlugin etlDest = new BlackholeStoragePlugin();
		ConfigServiceForTests configService = new ConfigServiceForTests(new File("./bin"));

		srcSetup.setUpRootFolder(etlSrc, "BlackholeETLTestSrc_"+granularity, granularity);

		logger.info("Testing black hole for " + etlSrc.getPartitionGranularity() + " to " + etlDest.getPartitionGranularity());

		short year = TimeUtils.getCurrentYear();
		long startOfYearInEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		long curEpochSeconds = startOfYearInEpochSeconds; 
		int secondsintoyear = 0;
		int incrementSeconds = 450;

		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_blackhole" + etlSrc.getPartitionGranularity();
		
		PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		String[] dataStores = new String[] { etlSrc.getURLRepresentation(), etlDest.getURLRepresentation() }; 
		typeInfo.setDataStores(dataStores);
		configService.updateTypeInfoForPV(pvName, typeInfo);
		configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
		configService.getETLLookup().manualControlForUnitTests();

		try(BasicContext context = new BasicContext()) {
			while(secondsintoyear < 60*60*24*365) {
				int eventsPerShot = (60*60*24*31)/incrementSeconds;
				ArrayListEventStream instream = new ArrayListEventStream(eventsPerShot, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
				for(int i = 0; i < eventsPerShot; i++) {
					instream.add(new SimulationEvent(secondsintoyear, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>((double)secondsintoyear)));
					secondsintoyear += incrementSeconds;
					curEpochSeconds += incrementSeconds;
				}
				etlSrc.appendData(context, pvName, instream);
				int filesWithDataBefore = getFilesWithData(pvName, etlSrc);
				ETLExecutor.runETLs(configService, 	TimeUtils.convertFromEpochSeconds(curEpochSeconds, 0));
				logger.debug("Done performing ETL");
				int filesWithDataAfter = getFilesWithData(pvName, etlSrc);
				assertTrue("Black hole did not remove source files before = " + filesWithDataBefore + " and after = " + filesWithDataAfter + " for granularity " + granularity.toString(), filesWithDataAfter < filesWithDataBefore);
			}
		}

		srcSetup.deleteTestFolder();
	}
	
	private int getFilesWithData(String pvName, PlainPBStoragePlugin etlSrc) throws Exception {
		// Check that all the files in the destination store are valid files.
		Path[] allPaths = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), etlSrc.getRootFolder(), pvName, ".pb", etlSrc.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
		return allPaths.length;
	}
	
}
