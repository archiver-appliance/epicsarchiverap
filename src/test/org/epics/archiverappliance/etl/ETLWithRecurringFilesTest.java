/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * We want to test what happens when ETL encounters the same stream again; for example, when a delete fails.
 * @author mshankar
 *
 */
@Tag("slow")
public class ETLWithRecurringFilesTest {
    private static final Logger logger = LogManager.getLogger(ETLWithRecurringFilesTest.class.getName());
    int ratio = 5; // size of test

    public static Stream<Arguments> provideRecurringFiles() {
        return Arrays.stream(PartitionGranularity.values())
                .flatMap(g -> Stream.of(
                                Arguments.of(g, true, true),
                                Arguments.of(g, true, false),
                                Arguments.of(g, false, true),
                                Arguments.of(g, false, false)));
    }


    /**
     * @param granularity
     * @param backUpFiles
     * @param useNewDest - This creates a new ETLDest for the rerun. New dest should hopefully not have any state and should still work.
     * @throws Exception
     */
    @ParameterizedTest
    @MethodSource("provideRecurringFiles")
    public void testRecurringFiles(
            PartitionGranularity granularity,
            boolean backUpFiles,
            boolean useNewDest)
            throws Exception {
        PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin();
        PBCommonSetup srcSetup = new PBCommonSetup();
        PlainPBStoragePlugin etlDest = new PlainPBStoragePlugin();
        PBCommonSetup destSetup = new PBCommonSetup();
        PlainPBStoragePlugin etlNewDest = new PlainPBStoragePlugin();
        PBCommonSetup newDestSetup = new PBCommonSetup();
        ConfigServiceForTests configService = new ConfigServiceForTests(1);
        etlDest.setBackupFilesBeforeETL(backUpFiles);

        srcSetup.setUpRootFolder(etlSrc, "RecurringFilesTestSrc" + granularity, granularity);
        destSetup.setUpRootFolder(
                etlDest, "RecurringFilesTestDest" + granularity, granularity.getNextLargerGranularity());
        newDestSetup.setUpRootFolder(
                etlNewDest, "RecurringFilesTestDest" + granularity, granularity.getNextLargerGranularity());

        logger.info("Testing recurring files for " + etlSrc.getPartitionGranularity() + " to "
                + etlDest.getPartitionGranularity() + " with backup = " + backUpFiles
                + " and newDest = " + useNewDest);

        short year = TimeUtils.getCurrentYear();
        Instant start = TimeUtils.getStartOfYear(year);
        Instant curTime = start;
        int incrementSeconds = granularity.getApproxSecondsPerChunk() / ratio; // ratio events per granularity
        int eventsGenerated;

        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testRecurringFiles"
                + etlSrc.getPartitionGranularity();
        {
            PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
            String[] dataStores = new String[] {etlSrc.getURLRepresentation(), etlDest.getURLRepresentation()};
            typeInfo.setDataStores(dataStores);
            configService.updateTypeInfoForPV(pvName, typeInfo);
            configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
            configService.getETLLookup().manualControlForUnitTests();
        }

        // Generate 3 times the granularity
        int eventsPerShot = granularity.getApproxSecondsPerChunk() * 3 / incrementSeconds;
        ArrayListEventStream instream = new ArrayListEventStream(
                eventsPerShot, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
        for (int i = 0; i < eventsPerShot; i++) {
            int secondsintoyear = TimeUtils.getSecondsIntoYear(curTime.getEpochSecond());
            instream.add(new SimulationEvent(secondsintoyear
                    , year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>((double)
                            secondsintoyear)));
            curTime = curTime.plusSeconds(incrementSeconds);
        }

        try (BasicContext context = new BasicContext()) {
            eventsGenerated = etlSrc.appendData(context, pvName, instream);
        }

        // We should now have some data in the src root folder...
        // Make a copy of these files so that we can restore them back later after ETL.
        Path[] allSrcPaths = PlainPBPathNameUtility.getAllPathsForPV(
                new ArchPaths(),
                etlSrc.getRootFolder(),
                pvName,
                PlainPBStoragePlugin.pbFileExtension,
                etlSrc.getPartitionGranularity(),
                CompressionMode.NONE,
                configService.getPVNameToKeyConverter());
        for (Path srcPath : allSrcPaths) {
            Path destPath =
                    srcPath.resolveSibling(srcPath.getFileName().toString().replace(PlainPBStoragePlugin.pbFileExtension, ".etltest"));
            logger.debug("Path for backup is " + destPath);
            Files.copy(srcPath, destPath, StandardCopyOption.REPLACE_EXISTING);
        }

        // Run ETL as if it was a week later.
        Instant ETLIsRunningAt =
                curTime.plusSeconds(PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 7L);
        // Now do ETL and the srcFiles should have disappeared.
        ETLExecutor.runETLs(configService, ETLIsRunningAt);

        // Restore the files from the backup and run ETL again.
        for (Path srcPath : allSrcPaths) {
            Assertions.assertFalse(
                    srcPath.toFile().exists(), srcPath.toAbsolutePath() + " was not deleted in the last run");
            Path destPath =
                    srcPath.resolveSibling(srcPath.getFileName().toString().replace(PlainPBStoragePlugin.pbFileExtension, ".etltest"));
            Files.copy(destPath, srcPath, StandardCopyOption.COPY_ATTRIBUTES);
        }

        if (useNewDest) {
            ConfigServiceForTests newConfigService = new ConfigServiceForTests(new File("./bin"), 1);
            PVTypeInfo typeInfo2 = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
            String[] dataStores2 = new String[] {etlSrc.getURLRepresentation(), etlNewDest.getURLRepresentation()};
            typeInfo2.setDataStores(dataStores2);
            newConfigService.registerPVToAppliance(pvName, newConfigService.getMyApplianceInfo());
            newConfigService.updateTypeInfoForPV(pvName, typeInfo2);
            newConfigService.getETLLookup().manualControlForUnitTests();

            logger.debug(
                    "Running ETL again against a new plugin; the debug logs should see a lot of skipping events messages from here on.");
            ETLExecutor.runETLs(configService, ETLIsRunningAt);
            checkDataValidity(
                    pvName,
                    etlSrc,
                    etlNewDest,
                    start,
                    incrementSeconds,
                    eventsGenerated,
                    granularity + "/" + backUpFiles);

        } else {
            logger.debug(
                    "Running ETL again; the debug logs should see a lot of skipping events messages from here on.");
            ETLExecutor.runETLs(configService, ETLIsRunningAt);
            checkDataValidity(
                    pvName,
                    etlSrc,
                    etlDest,
                    start,
                    incrementSeconds,
                    eventsGenerated,
                    granularity + "/" + backUpFiles);
        }

        srcSetup.deleteTestFolder();
        destSetup.deleteTestFolder();
        newDestSetup.deleteTestFolder();

        configService.shutdownNow();
    }

    private void checkDataValidity(
            String pvName,
            PlainPBStoragePlugin etlSrc,
            PlainPBStoragePlugin etlDest,
            Instant start,
            int incrementSeconds,
            int eventsGenerated,
            String testDesc)
            throws IOException {
        Instant startOfRequest = TimeUtils.minusDays(TimeUtils.now(), 366);
        Instant endOfRequest = TimeUtils.plusDays(TimeUtils.now(), 366);

        logger.info(testDesc + "Asking for data between "
                + TimeUtils.convertToHumanReadableString(startOfRequest)
                + " and "
                + TimeUtils.convertToHumanReadableString(endOfRequest));

        Instant expectedTime = start;
        int afterCount = 0;

        try (BasicContext context = new BasicContext();
                EventStream afterDest = new CurrentThreadWorkerEventStream(
                        pvName, etlDest.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
            for (Event e : afterDest) {
                Assertions.assertEquals(
                        expectedTime,
                        e.getEventTimeStamp());
                expectedTime = expectedTime.plusSeconds(incrementSeconds);
                afterCount++;
            }
            Assertions.assertTrue(afterCount != 0, testDesc + "Seems like no events were moved by ETL " + afterCount);
        }

        try (BasicContext context = new BasicContext();
                EventStream afterSrc = new CurrentThreadWorkerEventStream(
                        pvName, etlSrc.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
            for (Event e : afterSrc) {
                Assertions.assertEquals(
                        expectedTime,
                        e.getEventTimeStamp());
                expectedTime = expectedTime.plusSeconds(incrementSeconds);
                afterCount++;
            }
        }

        Assertions.assertEquals(
                eventsGenerated,
                afterCount,
                testDesc + "Expected total events " + eventsGenerated + " is not the same as actual events "
                        + afterCount);
    }
}
