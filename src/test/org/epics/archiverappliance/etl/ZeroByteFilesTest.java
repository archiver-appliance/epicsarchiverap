/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.plain.PathNameUtility;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import edu.stanford.slac.archiverappliance.plain.pb.PBCompressionMode;
import edu.stanford.slac.archiverappliance.plain.utils.ValidatePBFile;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

/**
 * Occasionally, we seem to get files that are 0 bytes long; this usually happens in unusual circumstances.
 * For example, Terry reported this happening around the time IT changed the network config incorrectly.
 * Once we get a zero byte file in the ETL dest, we seem to be struck as we try to determine the last known timestamp in the file and fail.
 * This tests zero byte files in both the source and the dest.
 * @author mshankar
 *
 */
public class ZeroByteFilesTest {
    private static final Logger logger = LogManager.getLogger(ZeroByteFilesTest.class);
    private final String shortTermFolderName = ConfigServiceForTests.getDefaultShortTermFolder() + "/shortTerm";
    private final String mediumTermFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/mediumTerm";
    private ConfigServiceForTests configService;
    private PVNameToKeyMapping pvNameToKeyConverter;
    private final short currentYear = TimeUtils.getCurrentYear();

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        configService.getETLLookup().manualControlForUnitTests();

        cleanUpDataFolders();

        pvNameToKeyConverter = configService.getPVNameToKeyConverter();
    }

    @AfterEach
    public void tearDown() throws Exception {
        cleanUpDataFolders();
    }

    public void cleanUpDataFolders() throws Exception {
        if (new File(shortTermFolderName).exists()) {
            FileUtils.deleteDirectory(new File(shortTermFolderName));
        }
        if (new File(mediumTermFolderName).exists()) {
            FileUtils.deleteDirectory(new File(mediumTermFolderName));
        }
    }

    @FunctionalInterface
    public interface VoidFunction {
        void apply() throws IOException;
    }

    @Test
    public void testZeroByteFileInDest() throws Exception {

        PlainStoragePlugin etlSrc = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb" + "://localhost?name=STS&rootFolder=" + shortTermFolderName
                        + "/&partitionGranularity=PARTITION_DAY",
                configService);
        PlainStoragePlugin etlDest = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb" + "://localhost?name=MTS&rootFolder=" + mediumTermFolderName
                        + "/&partitionGranularity=PARTITION_YEAR",
                configService);
        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testZeroDest";
        // Create an zero byte file in the ETL dest
        VoidFunction zeroByteGenerator = () -> {
            assert etlDest != null;
            Path zeroDestPath = Paths.get(
                    etlDest.getRootFolder(),
                    pvNameToKeyConverter.convertPVNameToKey(pvName) + currentYear + etlDest.getExtensionString());
            logger.info("Creating zero byte file " + zeroDestPath);
            Files.write(zeroDestPath, new byte[0], StandardOpenOption.CREATE);
        };
        runETLAndValidate(pvName, zeroByteGenerator, etlSrc, etlDest);
    }

    @Test
    public void testZeroByteFilesInSource() throws Exception {

        PlainStoragePlugin etlSrc = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb" + "://localhost?name=STS&rootFolder=" + shortTermFolderName
                        + "/&partitionGranularity=PARTITION_DAY",
                configService);
        PlainStoragePlugin etlDest = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb" + "://localhost?name=MTS&rootFolder=" + mediumTermFolderName
                        + "/&partitionGranularity=PARTITION_YEAR",
                configService);
        // Create zero byte files in the ETL source; since this is a daily partition, we need something like so
        // sine:2016_03_31.pb
        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testZeroSrc";
        VoidFunction zeroByteGenerator = () -> {
            for (int day = 2; day < 10; day++) {
                assert etlSrc != null;
                Path zeroSrcPath = Paths.get(
                        etlSrc.getRootFolder(),
                        pvNameToKeyConverter.convertPVNameToKey(pvName)
                                + TimeUtils.getPartitionName(
                                        Instant.now()
                                                .minusSeconds((long) day
                                                        * PartitionGranularity.PARTITION_DAY
                                                                .getApproxSecondsPerChunk()),
                                        PartitionGranularity.PARTITION_DAY)
                                + etlSrc.getExtensionString());
                logger.info("Creating zero byte file " + zeroSrcPath);
                Files.write(zeroSrcPath, new byte[0], StandardOpenOption.CREATE);
            }
        };
        runETLAndValidate(pvName, zeroByteGenerator, etlSrc, etlDest);
    }

    /**
     * Generates some data in STS; then calls the ETL to move it to MTS which has a zero byte file.
     */
    public void runETLAndValidate(
            String pvName,
            VoidFunction zeroByteGenerationFunction,
            PlainStoragePlugin etlSrc,
            PlainStoragePlugin etlDest)
            throws Exception {

        // Generate some data in the src
        int totalSamples = 1024;
        long currentSeconds = TimeUtils.getCurrentEpochSeconds();
        ArrayListEventStream srcData = new ArrayListEventStream(
                totalSamples, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));
        for (int i = 0; i < totalSamples; i++) {
            YearSecondTimestamp yts = TimeUtils.convertToYearSecondTimestamp(currentSeconds);
            srcData.add(new SimulationEvent(
                    yts.getSecondsintoyear(),
                    yts.getYear(),
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    new ScalarValue<>(Math.sin(yts.getSecondsintoyear()))));
            currentSeconds++;
        }
        try (BasicContext context = new BasicContext()) {
            etlSrc.appendData(context, pvName, srcData);
        }
        logger.info("Done creating src data for PV " + pvName);

        long beforeCount = 0;
        List<Event> beforeEvents = new LinkedList<Event>();
        try (BasicContext context = new BasicContext();
                EventStream before = new CurrentThreadWorkerEventStream(
                        pvName,
                        etlSrc.getDataForPV(
                                context,
                                pvName,
                                TimeUtils.minusDays(TimeUtils.now(), 366),
                                TimeUtils.plusDays(TimeUtils.now(), 366)))) {
            for (Event e : before) {
                beforeEvents.add(e.makeClone());
                beforeCount++;
            }
        }

        logger.info("Calling lambda to generate zero byte files");
        zeroByteGenerationFunction.apply();

        // Register the PV
        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores = new String[] {etlSrc.getURLRepresentation(), etlDest.getURLRepresentation()};
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());

        // Now do ETL...
        Instant timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 365 * 10);
        ETLExecutor.runETLs(configService, timeETLruns);
        logger.info("Done performing ETL as though today is " + TimeUtils.convertToHumanReadableString(timeETLruns));

        // Validation starts here
        Instant startOfRequest = TimeUtils.minusDays(TimeUtils.now(), 366);
        Instant endOfRequest = TimeUtils.plusDays(TimeUtils.now(), 366);

        // Check that all the files in the destination store are valid files.
        Path[] allPaths = PathNameUtility.getAllPathsForPV(
                new ArchPaths(),
                etlDest.getRootFolder(),
                pvName,
                etlDest.getExtensionString(),
                etlDest.getPartitionGranularity(),
                PBCompressionMode.NONE,
                pvNameToKeyConverter);
        Assertions.assertNotNull(allPaths, "PlainPBFileNameUtility returns null for getAllFilesForPV for " + pvName);
        Assertions.assertTrue(
                allPaths.length > 0,
                "PlainPBFileNameUtility returns empty array for getAllFilesForPV for " + pvName + " when looking in "
                        + etlDest.getRootFolder());

        logger.info("allPaths {}", (Object) allPaths);
        for (Path destPath : allPaths) {
            Assertions.assertTrue(
                    ValidatePBFile.validatePBFile(destPath, false),
                    "File validation failed for " + destPath.toAbsolutePath());
        }

        logger.info("Asking for data between"
                + TimeUtils.convertToHumanReadableString(startOfRequest)
                + " and "
                + TimeUtils.convertToHumanReadableString(endOfRequest));

        long afterCount = 0;
        try (BasicContext context = new BasicContext();
                EventStream afterDest = new CurrentThreadWorkerEventStream(
                        pvName, etlDest.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
            Assertions.assertNotNull(afterDest);
            for (@SuppressWarnings("unused") Event e : afterDest) {
                afterCount++;
            }
        }

        logger.info("Of the " + beforeCount + " events, " + afterCount + " events were moved into the dest store.");
        Assertions.assertTrue((afterCount != 0), "Seems like no events were moved by ETL " + afterCount);

        long afterSourceCount = 0;
        try (BasicContext context = new BasicContext();
                EventStream afterSrc = new CurrentThreadWorkerEventStream(
                        pvName, etlSrc.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
            for (@SuppressWarnings("unused") Event e : afterSrc) {
                afterSourceCount++;
            }
        }
        Assertions.assertTrue(
                (afterSourceCount == 0), "Seems like we still have " + afterSourceCount + " events in the source ");

        // Now compare the events itself
        try (BasicContext context = new BasicContext();
                EventStream afterDest = new CurrentThreadWorkerEventStream(
                        pvName, etlDest.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
            int index = 0;
            for (Event afterEvent : afterDest) {
                Event beforeEvent = beforeEvents.get(index);
                Assertions.assertEquals(
                        beforeEvent.getEventTimeStamp(),
                        afterEvent.getEventTimeStamp(),
                        "Before timestamp " + TimeUtils.convertToHumanReadableString(beforeEvent.getEventTimeStamp())
                                + " After timestamp "
                                + TimeUtils.convertToHumanReadableString(afterEvent.getEventTimeStamp()));
                Assertions.assertEquals(
                        beforeEvent.getSampleValue().getValue(),
                        afterEvent.getSampleValue().getValue(),
                        "Before value " + beforeEvent.getSampleValue().getValue() + " After value "
                                + afterEvent.getSampleValue().getValue());
                index++;
            }
        }

        Assertions.assertEquals(
                beforeCount,
                afterCount,
                "Of the total " + beforeCount + " event, we should have moved " + beforeCount
                        + ". Instead we seem to have moved " + afterCount);
    }
}
