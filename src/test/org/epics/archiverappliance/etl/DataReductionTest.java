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
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessors;
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
import java.util.LinkedList;
import java.util.List;

/**
 * Test data reduction as part of ETL.
 * @author mshankar
 *
 */
@Tag("singleFork")
public class DataReductionTest {
        private static final Logger logger = LogManager.getLogger(DataReductionTest.class);
    	String shortTermFolderName=ConfigServiceForTests.getDefaultShortTermFolder()+"/shortTerm";
    	String mediumTermFolderName=ConfigServiceForTests.getDefaultPBTestFolder()+"/mediumTerm";
    	private  ConfigServiceForTests configService;

        @BeforeEach
        public void setUp() throws Exception {
	        configService = new ConfigServiceForTests(-1);
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
        	String reduceDataUsing = "firstSample_3600";
        	PlainPBStoragePlugin etlSrc = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_DAY", configService);
        	PlainPBStoragePlugin etlDest = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + mediumTermFolderName + "/&partitionGranularity=PARTITION_YEAR&reducedata=" + reduceDataUsing, configService);
        	logger.info("Testing data reduction for " + etlSrc.getPartitionGranularity() + " to " + etlDest.getPartitionGranularity());

        	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testMove" + etlSrc.getPartitionGranularity();
            short currentYear = TimeUtils.getCurrentYear();

            SimulationEventStream simstream = new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0), TimeUtils.getStartOfYear(currentYear), TimeUtils.getEndOfYear(currentYear), 1);
        	try(BasicContext context = new BasicContext()) {
        		etlSrc.appendData(context, pvName, simstream);
        	}
        	logger.info("Done creating src data for PV " + pvName);

        	long beforeCount = 0;
        	try (BasicContext context = new BasicContext(); EventStream before = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, TimeUtils.minusDays(TimeUtils.now(), 366), TimeUtils.plusDays(TimeUtils.now(), 366)))) {
        		for(@SuppressWarnings("unused") Event e : before) { beforeCount++; } 
        	}
        	
			PostProcessor postProcessor = PostProcessors.findPostProcessor(reduceDataUsing);
			postProcessor.initialize(reduceDataUsing, pvName);
        	long beforeReducedCount = 0;
        	List<Event> reducedEvents = new LinkedList<Event>();
        	try (BasicContext context = new BasicContext(); EventStream before = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, TimeUtils.minusDays(TimeUtils.now(), 366), TimeUtils.plusDays(TimeUtils.now(), 366), postProcessor))) {
        		for(Event e : before) {
        			reducedEvents.add(e.makeClone());
        			beforeReducedCount++; 
        		} 
        	}


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
	        Path[] allPaths = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), etlDest.getRootFolder(), pvName, PlainPBStoragePlugin.pbFileExtension, etlDest.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
        	Assertions.assertTrue(allPaths != null, "PlainPBFileNameUtility returns null for getAllFilesForPV for " + pvName);
        	Assertions.assertTrue(allPaths.length > 0, "PlainPBFileNameUtility returns empty array for getAllFilesForPV for " + pvName + " when looking in " + etlDest.getRootFolder());

        	for(Path destPath : allPaths) {
        		Assertions.assertTrue(ValidatePBFile.validatePBFile(destPath, false), "File validation failed for " + destPath.toAbsolutePath().toString());

        	}

        	logger.info("Asking for data between" 
        			+ TimeUtils.convertToHumanReadableString(startOfRequest) 
        			+ " and " 
        			+ TimeUtils.convertToHumanReadableString(endOfRequest)
        			);

        	long afterCount = 0;
        	try (BasicContext context = new BasicContext(); EventStream afterDest = new CurrentThreadWorkerEventStream(pvName, etlDest.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
        		Assertions.assertNotNull(afterDest);
        		for(@SuppressWarnings("unused") Event e : afterDest) { afterCount++; }
        	}
        	logger.info("Of the " + beforeCount + " events, " + afterCount + " events were moved into the dest store.");
        	Assertions.assertTrue((afterCount != 0), "Seems like no events were moved by ETL " + afterCount);

        	long afterSourceCount = 0;
        	try (BasicContext context = new BasicContext(); EventStream afterSrc = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
        		for(@SuppressWarnings("unused") Event e : afterSrc) { afterSourceCount++; }
        	}
        	Assertions.assertTrue((afterSourceCount == 0), "Seems like we still have " + afterSourceCount + " events in the source ");

        	// Now compare the events itself
        	try (BasicContext context = new BasicContext(); EventStream afterDest = new CurrentThreadWorkerEventStream(pvName, etlDest.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
            	int index = 0;
        		for(Event afterEvent : afterDest) {
        			Event beforeEvent = reducedEvents.get(index);
        			Assertions.assertTrue(beforeEvent.getEventTimeStamp().equals(afterEvent.getEventTimeStamp()), "Before timestamp " + TimeUtils.convertToHumanReadableString(beforeEvent.getEventTimeStamp())
        					+ " After timestamp " + TimeUtils.convertToHumanReadableString(afterEvent.getEventTimeStamp()));
        			Assertions.assertTrue(beforeEvent.getSampleValue().getValue().equals(afterEvent.getSampleValue().getValue()), "Before value " + beforeEvent.getSampleValue().getValue()
        					+ " After value " + afterEvent.getSampleValue().getValue());
        			index++;
        		}
        	}

        	Assertions.assertTrue(beforeReducedCount == afterCount, "Of the total " + beforeCount  + " event, we should have moved " + beforeReducedCount + ". Instead we seem to have moved " +  afterCount);
        }
}
