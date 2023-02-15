/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SlowTests;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import edu.stanford.slac.archiverappliance.PlainPB.utils.ValidatePBFile;

/**
 * Very basic ETL tests.
 * @author mshankar
 *
 */
@Category(SlowTests.class)
public class SimpleETLTest {
        private static final Logger logger = Logger.getLogger(SimpleETLTest.class);

        @Before
        public void setUp() throws Exception {
        }

        @After
        public void tearDown() throws Exception {
        }

        /**
         * Generates some data in STS; then calls the ETL to move it to MTS and checks that the total amount of data before and after is the same.  
         */
        @Test
        public void testMove() throws Exception {
        	for(PartitionGranularity granularity : PartitionGranularity.values()) {
        		PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin();
        		PBCommonSetup srcSetup = new PBCommonSetup();
        		PlainPBStoragePlugin etlDest = new PlainPBStoragePlugin();
        		PBCommonSetup destSetup = new PBCommonSetup();
        		ConfigServiceForTests configService = new ConfigServiceForTests(new File("./bin"), 1);

        		if(granularity.getNextLargerGranularity() == null) continue;
        		srcSetup.setUpRootFolder(etlSrc, "SimpleETLTestSrc_"+granularity, granularity);
        		destSetup.setUpRootFolder(etlDest, "SimpleETLTestDest"+granularity, granularity.getNextLargerGranularity());

        		logger.info("Testing simple ETL testMove for " + etlSrc.getPartitionGranularity() + " to " + etlDest.getPartitionGranularity());

        		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testMove" + etlSrc.getPartitionGranularity();
        		SimulationEventStream simstream = new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0));
        		try(BasicContext context = new BasicContext()) {
        			etlSrc.appendData(context, pvName, simstream);
        		}
        		logger.info("Done creating src data for PV " + pvName);

        		long beforeCount = 0;
        		try (BasicContext context = new BasicContext(); EventStream before = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, TimeUtils.minusDays(TimeUtils.now(), 366), TimeUtils.plusDays(TimeUtils.now(), 366)))) {
        			for(@SuppressWarnings("unused") Event e : before) { beforeCount++; } 
        		}

        		PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        		String[] dataStores = new String[] { etlSrc.getURLRepresentation(), etlDest.getURLRepresentation() }; 
        		typeInfo.setDataStores(dataStores);
        		configService.updateTypeInfoForPV(pvName, typeInfo);
        		configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        		configService.getETLLookup().manualControlForUnitTests();

        		Timestamp timeETLruns = TimeUtils.now();
        		DateTime ts = new DateTime();
        		if(ts.getMonthOfYear() == 1) {
        			// This means that we never test this in Jan but I'd rather have the null check than skip this. 
        			timeETLruns = TimeUtils.plusDays(timeETLruns, 35);
        		}
        		ETLExecutor.runETLs(configService, timeETLruns);
        		logger.info("Done performing ETL");

        		Timestamp startOfRequest = TimeUtils.minusDays(TimeUtils.now(), 366);
        		Timestamp endOfRequest = TimeUtils.plusDays(TimeUtils.now(), 366);

        		// Check that all the files in the destination store are valid files.
        		Path[] allPaths = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), etlDest.getRootFolder(), pvName, ".pb", etlDest.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
        		assertTrue("PlainPBFileNameUtility returns null for getAllFilesForPV for " + pvName, allPaths != null);
        		assertTrue("PlainPBFileNameUtility returns empty array for getAllFilesForPV for " + pvName + " when looking in " + etlDest.getRootFolder() , allPaths.length > 0);

        		for(Path destPath : allPaths) {
        			assertTrue("File validation failed for " + destPath.toAbsolutePath().toString(), ValidatePBFile.validatePBFile(destPath, false));

        		}

        		logger.info("Asking for data between" 
        				+ TimeUtils.convertToHumanReadableString(startOfRequest) 
        				+ " and " 
        				+ TimeUtils.convertToHumanReadableString(endOfRequest)
        				);

        		long afterCount = 0;
        		try (BasicContext context = new BasicContext(); EventStream afterDest = new CurrentThreadWorkerEventStream(pvName, etlDest.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
        			assertNotNull(afterDest);
        			for(@SuppressWarnings("unused") Event e : afterDest) { afterCount++; }
        		}
        		logger.info("Of the " + beforeCount + " events, " + afterCount + " events were moved into the dest store.");
        		assertTrue("Seems like no events were moved by ETL " + afterCount, (afterCount != 0));
        		try (BasicContext context = new BasicContext(); EventStream afterSrc = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
        			for(@SuppressWarnings("unused") Event e : afterSrc) { afterCount++; }
        		}

        		assertTrue("Before count " + beforeCount  + " and after count " + afterCount + " differ", beforeCount == afterCount);

        		srcSetup.deleteTestFolder();
        		destSetup.deleteTestFolder();
        		configService.shutdownNow();
        	}
        }
}
