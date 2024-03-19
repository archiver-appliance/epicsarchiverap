/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import edu.stanford.slac.archiverappliance.PlainPB.utils.ValidatePBFile;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;

import static edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.pbFileExtension;

/**
 * Test if the named flags control of ETL works if the flag is set and unset
 * If the flag is true, then ETL should move the data across.
 * If the flag is false, then ETL should not move the data across.
 * @author mshankar
 *
 */
 @Tag("singleFork")
public class NamedFlagETLTest {
        private static final Logger logger = LogManager.getLogger(NamedFlagETLTest.class);
    	String shortTermFolderName=ConfigServiceForTests.getDefaultShortTermFolder()+"/shortTerm";
    	String mediumTermFolderName=ConfigServiceForTests.getDefaultPBTestFolder()+"/mediumTerm";

        @BeforeEach
        public void setUp() throws Exception {
    		if(new File(shortTermFolderName).exists()) {
    			FileUtils.deleteDirectory(new File(shortTermFolderName));
    		}
    		if(new File(mediumTermFolderName).exists()) {
    			FileUtils.deleteDirectory(new File(mediumTermFolderName));
    		}
        }

        @AfterEach
        public void tearDown() throws Exception {
    		if(new File(shortTermFolderName).exists()) {
    			FileUtils.deleteDirectory(new File(shortTermFolderName));
    		}
    		if(new File(mediumTermFolderName).exists()) {
    			FileUtils.deleteDirectory(new File(mediumTermFolderName));
    		}
        }

        /**
         * Generates some data in STS; then calls the ETL to move it to MTS.
         * Check that we only move reduced data into the MTS.
         */
        @Test
        public void testMove() throws Exception {
        	{
		        ConfigServiceForTests configService = new ConfigServiceForTests(-1);
        		logger.info("Testing Plain ETL");
	        	BeforeAndAfterETLCounts etlCounts = generateAndMoveData(configService, "", "");
				Assertions.assertTrue(etlCounts.afterCountMTS > 0, "Seems like no events were moved by ETL ");
				Assertions.assertEquals(0, etlCounts.afterCountSTS, "Seems like we still have " + etlCounts.afterCountSTS + " events in the source ");
				Assertions.assertEquals(etlCounts.afterCountMTS, (etlCounts.beforeCountSTS + etlCounts.beforeCountMTS), "Did we miss some events when moving data? ");
        	}
        	{
				ConfigServiceForTests configService = new ConfigServiceForTests(-1);
        		logger.info("Testing with flag but value of flag is false");
	        	BeforeAndAfterETLCounts etlCounts = generateAndMoveData(configService, "", "&etlIntoStoreIf=testFlag");
	        	// By default testFlag is false, so we should lose data in the move.
				Assertions.assertEquals(0, etlCounts.afterCountMTS, "Seems like some events were moved into the MTS by ETL ");
				Assertions.assertEquals(0, etlCounts.afterCountSTS, "Seems like we still have " + etlCounts.afterCountSTS + " events in the source ");
				Assertions.assertEquals(0, etlCounts.afterCountMTS, "We should have lost all the data in this case");
        	}
        	{
				ConfigServiceForTests configService = new ConfigServiceForTests(-1);
        		configService.setNamedFlag("testFlag", true);
        		logger.info("Testing with flag but value of flag is true");
	        	BeforeAndAfterETLCounts etlCounts = generateAndMoveData(configService, "", "&etlIntoStoreIf=testFlag");
				Assertions.assertTrue(etlCounts.afterCountMTS > 0, "Seems like no events were moved by ETL ");
				Assertions.assertEquals(0, etlCounts.afterCountSTS, "Seems like we still have " + etlCounts.afterCountSTS + " events in the source ");
				Assertions.assertEquals(etlCounts.afterCountMTS, (etlCounts.beforeCountSTS + etlCounts.beforeCountMTS), "Did we miss some events when moving data? ");
        	}
        	{
				ConfigServiceForTests configService = new ConfigServiceForTests(-1);
        		configService.setNamedFlag("testFlag", true);
        		logger.info("Testing with some other flag but value of flag is true");
	        	BeforeAndAfterETLCounts etlCounts = generateAndMoveData(configService, "", "&etlIntoStoreIf=testSomeOtherFlag");
	        	// This is some other flag; so it should be false and we should behave like a black hole again
				Assertions.assertEquals(0, etlCounts.afterCountMTS, "Seems like some events were moved into the MTS by ETL ");
				Assertions.assertEquals(0, etlCounts.afterCountSTS, "Seems like we still have " + etlCounts.afterCountSTS + " events in the source ");
				Assertions.assertEquals(0, etlCounts.afterCountMTS, "We should have lost all the data in this case");
        	}

        	// Testing etlOutofStoreIf from here
        	{
				ConfigServiceForTests configService = new ConfigServiceForTests(-1);
        		logger.info("Testing with flag but value of flag is false");
	        	BeforeAndAfterETLCounts etlCounts = generateAndMoveData(configService, "&etlOutofStoreIf=testFlag", "");
	        	// By default testFlag is false, so no data should move.
	        	Assertions.assertTrue((etlCounts.beforeCountSTS > 0), "Did we generate any data?");
	        	Assertions.assertTrue((etlCounts.afterCountMTS == 0), "Seems like some events were moved into the MTS by ETL ");
	        	Assertions.assertTrue((etlCounts.beforeCountSTS == etlCounts.afterCountSTS), "We should not have moved any data in this case");
        	}
        	{
				ConfigServiceForTests configService = new ConfigServiceForTests(-1);
        		configService.setNamedFlag("testFlag", true);
        		logger.info("Testing with flag but value of flag is true");
	        	BeforeAndAfterETLCounts etlCounts = generateAndMoveData(configService, "&etlOutofStoreIf=testFlag", "");
	        	Assertions.assertTrue((etlCounts.afterCountMTS > 0), "Seems like no events were moved by ETL ");
	        	Assertions.assertTrue((etlCounts.afterCountSTS == 0), "Seems like we still have " + etlCounts.afterCountSTS + " events in the source ");
	        	Assertions.assertTrue((etlCounts.afterCountMTS == (etlCounts.beforeCountSTS + etlCounts.beforeCountMTS)), "Did we miss some events when moving data? ");
        	}
        	{
				ConfigServiceForTests configService = new ConfigServiceForTests(-1);
        		configService.setNamedFlag("testFlag", true);
        		logger.info("Testing with some other flag but value of flag is true");
	        	BeforeAndAfterETLCounts etlCounts = generateAndMoveData(configService, "&etlOutofStoreIf=testSomeOtherFlag", "");
	        	// This is some other flag; so it should be false and we should behave like a black hole again
	        	Assertions.assertTrue((etlCounts.beforeCountSTS > 0), "Did we generate any data?");
	        	Assertions.assertTrue((etlCounts.afterCountMTS == 0), "Seems like some events were moved into the MTS by ETL ");
	        	Assertions.assertTrue((etlCounts.beforeCountSTS == etlCounts.afterCountSTS), "We should not have moved any data in this case");
        	}

        }

	BeforeAndAfterETLCounts generateAndMoveData(ConfigServiceForTests configService, String appendToSourceURL, String appendToDestURL) throws Exception {
        	BeforeAndAfterETLCounts etlCounts = new BeforeAndAfterETLCounts();

    		if(new File(shortTermFolderName).exists()) {
    			FileUtils.deleteDirectory(new File(shortTermFolderName));
    		}
    		if(new File(mediumTermFolderName).exists()) {
    			FileUtils.deleteDirectory(new File(mediumTermFolderName));
    		}


        	PlainPBStoragePlugin etlSrc = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_DAY" + appendToSourceURL, configService);
        	PlainPBStoragePlugin etlDest = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + mediumTermFolderName + "/&partitionGranularity=PARTITION_YEAR" + appendToDestURL, configService);

        	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_NamedFlagTest" + etlSrc.getPartitionGranularity();
            short currentYear = TimeUtils.getCurrentYear();

            SimulationEventStream simstream = new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0), TimeUtils.getStartOfYear(currentYear), TimeUtils.getEndOfYear(currentYear), 1);
        	try(BasicContext context = new BasicContext()) {
        		etlSrc.appendData(context, pvName, simstream);
        	}
        	logger.info("Done creating src data for PV " + pvName);

        	try (BasicContext context = new BasicContext(); EventStream before = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, TimeUtils.minusDays(TimeUtils.now(), 366), TimeUtils.plusDays(TimeUtils.now(), 366)))) {
        		for(@SuppressWarnings("unused") Event e : before) { etlCounts.beforeCountSTS++; }
        	}
        	try (BasicContext context = new BasicContext(); EventStream before = new CurrentThreadWorkerEventStream(pvName, etlDest.getDataForPV(context, pvName, TimeUtils.minusDays(TimeUtils.now(), 366), TimeUtils.plusDays(TimeUtils.now(), 366)))) {
        		for(@SuppressWarnings("unused") Event e : before) { etlCounts.beforeCountMTS++; }
        	}

        	logger.info("Before ETL, the counts are STS = " + etlCounts.beforeCountSTS + " and MTS = " + etlCounts.beforeCountMTS);
        	PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        	String[] dataStores = new String[] { etlSrc.getURLRepresentation(), etlDest.getURLRepresentation() };
        	typeInfo.setDataStores(dataStores);
        	configService.updateTypeInfoForPV(pvName, typeInfo);
        	configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        	configService.getETLLookup().manualControlForUnitTests();

            Instant timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 365 * 10);
        	ETLExecutor.runETLs(configService, timeETLruns);
        	logger.info("Done performing ETL as though today is " + TimeUtils.convertToHumanReadableString(timeETLruns));

            Instant startOfRequest = TimeUtils.minusDays(TimeUtils.now(), 366);
            Instant endOfRequest = TimeUtils.plusDays(TimeUtils.now(), 366);

        	// Check that all the files in the destination store are valid files.
	        Path[] allPaths = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), etlDest.getRootFolder(), pvName, pbFileExtension, etlDest.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
		Assertions.assertTrue(true, "PlainPBFileNameUtility returns null for getAllFilesForPV for " + pvName);

        	for(Path destPath : allPaths) {
        		Assertions.assertTrue(ValidatePBFile.validatePBFile(destPath, false), "File validation failed for " + destPath.toAbsolutePath().toString());
        	}

        	logger.info("Asking for data between"
        			+ TimeUtils.convertToHumanReadableString(startOfRequest)
        			+ " and "
        			+ TimeUtils.convertToHumanReadableString(endOfRequest)
        			);

        	try (BasicContext context = new BasicContext(); EventStream after = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
        		for(@SuppressWarnings("unused") Event e : after) { etlCounts.afterCountSTS++; }
        	}
        	try (BasicContext context = new BasicContext(); EventStream after = new CurrentThreadWorkerEventStream(pvName, etlDest.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
        		for(@SuppressWarnings("unused") Event e : after) { etlCounts.afterCountMTS++; }
        	}
        	logger.info("After ETL, the counts are STS = " + etlCounts.afterCountSTS + " and MTS = " + etlCounts.afterCountMTS);

        	return etlCounts;
        }

	static class BeforeAndAfterETLCounts {
        	long beforeCountSTS = 0;
        	long beforeCountMTS = 0;
        	long afterCountSTS = 0;
        	long afterCountMTS = 0;
        }
}
