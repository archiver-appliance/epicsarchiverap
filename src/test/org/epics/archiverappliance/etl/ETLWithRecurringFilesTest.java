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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;

/**
 * We want to test what happens when ETL encounters the same stream again; for example, when a delete fails. 
 * @author mshankar
 *
 */
@Category(SlowTests.class)
public class ETLWithRecurringFilesTest {
	private static Logger logger = LogManager.getLogger(ETLWithRecurringFilesTest.class.getName());

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testRecurringFiles() throws Exception {
		for(PartitionGranularity granularity : PartitionGranularity.values()) {
			if(granularity.getNextLargerGranularity() == null) continue;
			testRecurringFiles(granularity, true, false);
			testRecurringFiles(granularity, false, false);
			testRecurringFiles(granularity, true, true);
			testRecurringFiles(granularity, false, true);
		}
	}

	/**
	 * @param granularity
	 * @param backUpfiles
	 * @param useNewDest - This creates a new ETLDest for the rerun. New dest should hopefully not have any state and should still work.
	 * @throws Exception
	 */
	private void testRecurringFiles(PartitionGranularity granularity, boolean backUpfiles, boolean useNewDest) throws Exception {
		PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin();
		PBCommonSetup srcSetup = new PBCommonSetup();
		PlainPBStoragePlugin etlDest = new PlainPBStoragePlugin();
		PBCommonSetup destSetup = new PBCommonSetup();
		PlainPBStoragePlugin etlNewDest = new PlainPBStoragePlugin();
		PBCommonSetup newDestSetup = new PBCommonSetup();
		ConfigServiceForTests configService = new ConfigServiceForTests(new File("./bin"), 1);
		etlDest.setBackupFilesBeforeETL(backUpfiles);

		srcSetup.setUpRootFolder(etlSrc, "RecurringFilesTestSrc"+granularity, granularity);
		destSetup.setUpRootFolder(etlDest, "RecurringFilesTestDest"+granularity, granularity.getNextLargerGranularity());
		newDestSetup.setUpRootFolder(etlNewDest, "RecurringFilesTestDest"+granularity, granularity.getNextLargerGranularity());

		logger.info("Testing recurring files for " + etlSrc.getPartitionGranularity() + " to " + etlDest.getPartitionGranularity() + " with backup = " + Boolean.toString(backUpfiles) + " and newDest = " + Boolean.toString(useNewDest));

		short year = TimeUtils.getCurrentYear();
		long startOfYearInEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		long curEpochSeconds = startOfYearInEpochSeconds; 
		int secondsintoyear = 0;
		int incrementSeconds = 450;
		int eventsgenerated = 0;

		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testRecurringFiles" + etlSrc.getPartitionGranularity();
		{
			PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			String[] dataStores = new String[] { etlSrc.getURLRepresentation(), etlDest.getURLRepresentation() }; 
			typeInfo.setDataStores(dataStores);
			configService.updateTypeInfoForPV(pvName, typeInfo);
			configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
			configService.getETLLookup().manualControlForUnitTests();
		}
		
		// Generate 90 days worth of data
		int eventsPerShot = (60*60*24*90)/incrementSeconds;
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
		
		// We should now have some data in the src root folder...
		// Make a copy of these files so that we can restore them back later after ETL.
		Path[] allSrcPaths = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), etlSrc.getRootFolder(), pvName, PlainPBStoragePlugin.PB_EXTENSION, etlSrc.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
		for(Path srcPath : allSrcPaths) {
			Path destPath = srcPath.resolveSibling(srcPath.getFileName().toString().replace(".pb", ".etltest"));
			logger.debug("Path for backup is " + destPath.toString());
			Files.copy(srcPath, destPath, StandardCopyOption.REPLACE_EXISTING);
		}

		// Run ETL as if it was a week later.
		long ETLIsRunningAtEpochSeconds = curEpochSeconds + 24*60*60*7;
		// Now do ETL and the srcFiles should have disappeared.
		ETLExecutor.runETLs(configService, TimeUtils.convertFromEpochSeconds(ETLIsRunningAtEpochSeconds, 0));
		
		// Restore the files from the backup and run ETL again.
		for(Path srcPath : allSrcPaths) {
			assertTrue(srcPath.toAbsolutePath().toString() + " was not deleted in the last run", !srcPath.toFile().exists());
			Path destPath = srcPath.resolveSibling(srcPath.getFileName().toString().replace(".pb", ".etltest"));
			Files.copy(destPath, srcPath, StandardCopyOption.COPY_ATTRIBUTES);
		}
		
		if(useNewDest) {
			ConfigServiceForTests newConfigService = new ConfigServiceForTests(new File("./bin"), 1);
			PVTypeInfo typeInfo2 = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			String[] dataStores2 = new String[] { etlSrc.getURLRepresentation(), etlNewDest.getURLRepresentation() }; 
			typeInfo2.setDataStores(dataStores2);
			newConfigService.registerPVToAppliance(pvName, newConfigService.getMyApplianceInfo());
			newConfigService.updateTypeInfoForPV(pvName, typeInfo2);
			newConfigService.getETLLookup().manualControlForUnitTests();
			
			logger.debug("Running ETL again against a new plugin; the debug logs should see a lot of skipping events messages from here on.");
			ETLExecutor.runETLs(configService, TimeUtils.convertFromEpochSeconds(ETLIsRunningAtEpochSeconds, 0));
			checkDataValidity(pvName, etlSrc, etlNewDest, startOfYearInEpochSeconds, incrementSeconds, eventsgenerated, granularity.toString() + "/" + Boolean.toString(backUpfiles));
			
		} else {
			logger.debug("Running ETL again; the debug logs should see a lot of skipping events messages from here on.");
			ETLExecutor.runETLs(configService, TimeUtils.convertFromEpochSeconds(ETLIsRunningAtEpochSeconds, 0));
			checkDataValidity(pvName, etlSrc, etlDest, startOfYearInEpochSeconds, incrementSeconds, eventsgenerated, granularity.toString() + "/" + Boolean.toString(backUpfiles));
		}
		

		srcSetup.deleteTestFolder();
		destSetup.deleteTestFolder();
		newDestSetup.deleteTestFolder();
		
		configService.shutdownNow();
	}
	
	private void checkDataValidity(String pvName, PlainPBStoragePlugin etlSrc, PlainPBStoragePlugin etlDest, long startOfYearInEpochSeconds, int incrementSeconds, int eventsgenerated, String testDesc) throws IOException {
		Timestamp startOfRequest = TimeUtils.minusDays(TimeUtils.now(), 366);
		Timestamp endOfRequest = TimeUtils.plusDays(TimeUtils.now(), 366);

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
					assertTrue(testDesc + "Dest Expected seconds " + TimeUtils.convertToHumanReadableString(expectedEpochSeconds) + " is not the same as actual seconds " + TimeUtils.convertToHumanReadableString(e.getEpochSeconds()) + " for afterCount " + afterCount, (expectedEpochSeconds == e.getEpochSeconds()));
					expectedEpochSeconds += incrementSeconds;
					afterCount++;
				}
				assertTrue(testDesc + "Seems like no events were moved by ETL " + afterCount, (afterCount != 0));
			}
		}
		
		
		try (BasicContext context = new BasicContext(); EventStream afterSrc = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
			if(afterSrc != null) {
				for(Event e : afterSrc) { 
					assertTrue(testDesc + "Src Expected seconds " + TimeUtils.convertToHumanReadableString(expectedEpochSeconds) + " is not the same as actual seconds " + TimeUtils.convertToHumanReadableString(e.getEpochSeconds()) + " for afterCount " + afterCount, (expectedEpochSeconds == e.getEpochSeconds()));
					expectedEpochSeconds += incrementSeconds;
					afterCount++;
				}
			}
		}
		
		assertTrue(testDesc + "Expected total events " + eventsgenerated + " is not the same as actual events " + afterCount, (eventsgenerated == afterCount));
	}
}
