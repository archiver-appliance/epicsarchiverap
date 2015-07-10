/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import java.io.File;
import java.nio.file.Path;
import java.sql.Timestamp;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import edu.stanford.slac.archiverappliance.PlainPB.utils.ValidatePBFile;

/**
 * Test if the named flags control of ETL works if the flag is set and unset
 * If the flag is true, then ETL should move the data across.
 * If the flag is false, then ETL should not move the data across.
 * @author mshankar
 *
 */
public class NamedFlagETLTest extends TestCase {
        private static final Logger logger = Logger.getLogger(NamedFlagETLTest.class);
    	String shortTermFolderName=ConfigServiceForTests.getDefaultShortTermFolder()+"/shortTerm";
    	String mediumTermFolderName=ConfigServiceForTests.getDefaultPBTestFolder()+"/mediumTerm";

        @Before
        public void setUp() throws Exception {
    		if(new File(shortTermFolderName).exists()) {
    			FileUtils.deleteDirectory(new File(shortTermFolderName));
    		}
    		if(new File(mediumTermFolderName).exists()) {
    			FileUtils.deleteDirectory(new File(mediumTermFolderName));
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
        }
        
        class BeforeAndAfterETLCounts { 
        	long beforeCountSTS = 0;
        	long beforeCountMTS = 0;
        	long afterCountSTS = 0;
        	long afterCountMTS = 0;
        }

        /**
         * Generates some data in STS; then calls the ETL to move it to MTS.
         * Check that we only move reduced data into the MTS.
         */
        @Test
        public void testMove() throws Exception {
        	{
            	ConfigServiceForTests configService = new ConfigServiceForTests(new File("./bin"));
        		logger.info("Testing Plain ETL");
	        	BeforeAndAfterETLCounts etlCounts = generateAndMoveData(configService, "");
	        	assertTrue("Seems like no events were moved by ETL ", (etlCounts.afterCountMTS > 0));
	        	assertTrue("Seems like we still have " + etlCounts.afterCountSTS + " events in the source ", (etlCounts.afterCountSTS == 0));
	        	assertTrue("Did we miss some events when moving data? ", (etlCounts.afterCountMTS == (etlCounts.beforeCountSTS + etlCounts.beforeCountMTS)));
        	}
        	{
            	ConfigServiceForTests configService = new ConfigServiceForTests(new File("./bin"));
        		logger.info("Testing with flag but value of flag is false");
	        	BeforeAndAfterETLCounts etlCounts = generateAndMoveData(configService, "&etlIntoStoreIf=testFlag");
	        	// By default testFlag is true, so we should lose data in the move.
	        	assertTrue("Seems like some events were moved into the MTS by ETL ", (etlCounts.afterCountMTS == 0));
	        	assertTrue("Seems like we still have " + etlCounts.afterCountSTS + " events in the source ", (etlCounts.afterCountSTS == 0));
	        	assertTrue("We should have lost all the data in this case", (etlCounts.afterCountMTS == 0));
        	}
        	{
            	ConfigServiceForTests configService = new ConfigServiceForTests(new File("./bin"));
        		configService.setNamedFlag("testFlag", true);
        		logger.info("Testing with flag but value of flag is true");
	        	BeforeAndAfterETLCounts etlCounts = generateAndMoveData(configService, "&etlIntoStoreIf=testFlag");
	        	assertTrue("Seems like no events were moved by ETL ", (etlCounts.afterCountMTS > 0));
	        	assertTrue("Seems like we still have " + etlCounts.afterCountSTS + " events in the source ", (etlCounts.afterCountSTS == 0));
	        	assertTrue("Did we miss some events when moving data? ", (etlCounts.afterCountMTS == (etlCounts.beforeCountSTS + etlCounts.beforeCountMTS)));
        	}
        	{
            	ConfigServiceForTests configService = new ConfigServiceForTests(new File("./bin"));
        		configService.setNamedFlag("testFlag", true);
        		logger.info("Testing with some other flag but value of flag is true");
	        	BeforeAndAfterETLCounts etlCounts = generateAndMoveData(configService, "&etlIntoStoreIf=testSomeOtherFlag");
	        	// This is some other flag; so it should be false and we should behave like a black hole again
	        	assertTrue("Seems like some events were moved into the MTS by ETL ", (etlCounts.afterCountMTS == 0));
	        	assertTrue("Seems like we still have " + etlCounts.afterCountSTS + " events in the source ", (etlCounts.afterCountSTS == 0));
	        	assertTrue("We should have lost all the data in this case", (etlCounts.afterCountMTS == 0));
        	}
        }
        
        public BeforeAndAfterETLCounts generateAndMoveData(ConfigServiceForTests configService, String appendToDestURL) throws Exception {
        	BeforeAndAfterETLCounts etlCounts = new BeforeAndAfterETLCounts();
        	
    		if(new File(shortTermFolderName).exists()) {
    			FileUtils.deleteDirectory(new File(shortTermFolderName));
    		}
    		if(new File(mediumTermFolderName).exists()) {
    			FileUtils.deleteDirectory(new File(mediumTermFolderName));
    		}

        	
        	PlainPBStoragePlugin etlSrc = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_DAY", configService);;
        	PlainPBStoragePlugin etlDest = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + mediumTermFolderName + "/&partitionGranularity=PARTITION_YEAR" + appendToDestURL, configService);

        	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_NamedFlagTest" + etlSrc.getPartitionGranularity();
        	SimulationEventStream simstream = new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0));
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

        	Timestamp timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 365*10);
        	ETLExecutor.runETLs(configService, timeETLruns);
        	logger.info("Done performing ETL as though today is " + TimeUtils.convertToHumanReadableString(timeETLruns));

        	Timestamp startOfRequest = TimeUtils.minusDays(TimeUtils.now(), 366);
        	Timestamp endOfRequest = TimeUtils.plusDays(TimeUtils.now(), 366);

        	// Check that all the files in the destination store are valid files.
        	Path[] allPaths = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), etlDest.getRootFolder(), pvName, ".pb", etlDest.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
        	assertTrue("PlainPBFileNameUtility returns null for getAllFilesForPV for " + pvName, allPaths != null);

        	for(Path destPath : allPaths) {
        		assertTrue("File validation failed for " + destPath.toAbsolutePath().toString(), ValidatePBFile.validatePBFile(destPath, false));
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
}
