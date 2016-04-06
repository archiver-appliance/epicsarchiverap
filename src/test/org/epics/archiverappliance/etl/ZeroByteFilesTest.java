/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import edu.stanford.slac.archiverappliance.PlainPB.utils.ValidatePBFile;
import junit.framework.TestCase;

/**
 * Occasionally, we seem to get files that are 0 bytes long; this usually happens in unusual circumstances. 
 * For example, Terry reported this happening around the time IT changed the network config incorrectly.
 * Once we get a zero byte file in the ETL dest, we seem to be struck as we try to determine the last known timestamp in the file and fail.
 * This tests zero byte files in both the source and the dest.
 * @author mshankar
 *
 */
public class ZeroByteFilesTest extends TestCase {
	private static final Logger logger = Logger.getLogger(ZeroByteFilesTest.class);
	private String shortTermFolderName=ConfigServiceForTests.getDefaultShortTermFolder()+"/shortTerm";
	private String mediumTermFolderName=ConfigServiceForTests.getDefaultPBTestFolder()+"/mediumTerm";
	private  ConfigServiceForTests configService;
	private PVNameToKeyMapping pvNameToKeyConverter;
	private PlainPBStoragePlugin etlSrc;
	private PlainPBStoragePlugin etlDest;
	private short currentYear = TimeUtils.getCurrentYear();

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		configService.getETLLookup().manualControlForUnitTests();

		cleanUpDataFolders();
	
		pvNameToKeyConverter = configService.getPVNameToKeyConverter();
		etlSrc = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_DAY", configService);
		etlDest = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + mediumTermFolderName + "/&partitionGranularity=PARTITION_YEAR", configService);		
	}

	@After
	public void tearDown() throws Exception {
		cleanUpDataFolders();
	}
	
	public void cleanUpDataFolders() throws Exception { 
		if(new File(shortTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(shortTermFolderName));
		}
		if(new File(mediumTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(mediumTermFolderName));
		}
	}

	@Test
	public void testZeroByteETL() throws Exception {
		testZeroByteFileInDest();
		testZeroByteFilesInSource();
	}
	
	@FunctionalInterface
	public interface VoidFunction {
	   void apply() throws IOException;
	}
	
	public void testZeroByteFileInDest() throws Exception {
		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testZeroDest";
		// Create an zero byte file in the ETL dest
		VoidFunction zeroByteGenerator = () -> { 
			Path zeroDestPath = Paths.get(etlDest.getRootFolder(), pvNameToKeyConverter.convertPVNameToKey(pvName) + currentYear + PlainPBStoragePlugin.PB_EXTENSION);
			logger.info("Creating zero byte file " + zeroDestPath);
			Files.write(zeroDestPath, new byte[0], StandardOpenOption.CREATE);
		};
		runETLAndValidate(pvName, zeroByteGenerator);
	}
	
	
	public void testZeroByteFilesInSource() throws Exception { 
		// Create zero byte files in the ETL source; since this is a daily partition, we need something like so sine:2016_03_31.pb
		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testZeroSrc";
		VoidFunction zeroByteGenerator = () -> { 
			for(int day = 2; day < 10; day++) { 
				Path zeroSrcPath = Paths.get(etlSrc.getRootFolder(), pvNameToKeyConverter.convertPVNameToKey(pvName) + TimeUtils.getPartitionName(TimeUtils.getCurrentEpochSeconds() - day*86400, PartitionGranularity.PARTITION_DAY) + PlainPBStoragePlugin.PB_EXTENSION);
				logger.info("Creating zero byte file " + zeroSrcPath);
				Files.write(zeroSrcPath, new byte[0], StandardOpenOption.CREATE);
			}
		};
		runETLAndValidate(pvName, zeroByteGenerator);
	}

	/**
	 * Generates some data in STS; then calls the ETL to move it to MTS which has a zero byte file.
	 */
	public void runETLAndValidate(String pvName, VoidFunction zeroByteGenerationFunction) throws Exception {

		// Generate some data in the src
		int totalSamples = 1024;
		long currentSeconds = TimeUtils.getCurrentEpochSeconds();
		ArrayListEventStream srcData = new ArrayListEventStream(totalSamples, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));
		for(int i = 0; i < totalSamples; i++) {
			YearSecondTimestamp yts = TimeUtils.convertToYearSecondTimestamp(currentSeconds);
			srcData.add(new SimulationEvent(yts.getSecondsintoyear(), yts.getYear(), ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(Math.sin((double)yts.getSecondsintoyear()))));
			currentSeconds++;
		}
		try(BasicContext context = new BasicContext()) {
			etlSrc.appendData(context, pvName, srcData);
		}
		logger.info("Done creating src data for PV " + pvName);

		long beforeCount = 0;
		List<Event> beforeEvents = new LinkedList<Event>();
		try (BasicContext context = new BasicContext(); EventStream before = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, TimeUtils.minusDays(TimeUtils.now(), 366), TimeUtils.plusDays(TimeUtils.now(), 366)))) {
			for(Event e : before) {
				beforeEvents.add(e.makeClone());
				beforeCount++; 
			} 
		}

		logger.debug("Calling lambda to generate zero byte files");
		zeroByteGenerationFunction.apply();
		
		// Register the PV
		PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		String[] dataStores = new String[] { etlSrc.getURLRepresentation(), etlDest.getURLRepresentation() }; 
		typeInfo.setDataStores(dataStores);
		configService.updateTypeInfoForPV(pvName, typeInfo);
		configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());

		// Now do ETL...
		Timestamp timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 365*10);
		ETLExecutor.runETLs(configService, timeETLruns);
		logger.info("Done performing ETL as though today is " + TimeUtils.convertToHumanReadableString(timeETLruns));

		// Validation starts here
		Timestamp startOfRequest = TimeUtils.minusDays(TimeUtils.now(), 366);
		Timestamp endOfRequest = TimeUtils.plusDays(TimeUtils.now(), 366);

		// Check that all the files in the destination store are valid files.
		Path[] allPaths = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), etlDest.getRootFolder(), pvName, ".pb", etlDest.getPartitionGranularity(), CompressionMode.NONE, pvNameToKeyConverter);
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

		long afterSourceCount = 0;
		try (BasicContext context = new BasicContext(); EventStream afterSrc = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
			for(@SuppressWarnings("unused") Event e : afterSrc) { afterSourceCount++; }
		}
		assertTrue("Seems like we still have " + afterSourceCount + " events in the source ", (afterSourceCount == 0));

		// Now compare the events itself
		try (BasicContext context = new BasicContext(); EventStream afterDest = new CurrentThreadWorkerEventStream(pvName, etlDest.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
			int index = 0;
			for(Event afterEvent : afterDest) {
				Event beforeEvent = beforeEvents.get(index);
				assertTrue("Before timestamp " + TimeUtils.convertToHumanReadableString(beforeEvent.getEventTimeStamp()) 
				+ " After timestamp " + TimeUtils.convertToHumanReadableString(afterEvent.getEventTimeStamp()), beforeEvent.getEventTimeStamp().equals(afterEvent.getEventTimeStamp()));
				assertTrue("Before value " + beforeEvent.getSampleValue().getValue() 
						+ " After value " + afterEvent.getSampleValue().getValue(), beforeEvent.getSampleValue().getValue().equals(afterEvent.getSampleValue().getValue()));
				index++;
			}
		}

		assertTrue("Of the total " + beforeCount  + " event, we should have moved " + beforeCount + ". Instead we seem to have moved " +  afterCount, beforeCount == afterCount);
	}	
}
