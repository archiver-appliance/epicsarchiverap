/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.commons.io.FileUtils;
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
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * test for consolidate all pb files from short term storage and medium term storage to long term storage
 * @author Luofeng Li
 *
 */
class ConsolidateETLJobsForOnePVTest {

    private static final Logger logger = LogManager.getLogger(ConsolidateETLJobsForOnePVTest.class.getName());
    static String rootFolderName =
            ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ConsolidateETLJobsForOnePVTest";
    static String shortTermFolderName = rootFolderName + "/shortTerm";
    static String mediumTermFolderName = rootFolderName + "/mediumTerm";
    static String longTermFolderName = rootFolderName + "/longTerm";
    String pvNamePrefix = "ArchUnitTest" + "ConsolidateETLJobsForOnePVTest";
    static PlainPBStoragePlugin stsStoragePlugin;
    static PlainPBStoragePlugin mtsStoragePlugin;
    static PlainPBStoragePlugin ltsStoragePlugin;
    static short currentYear = TimeUtils.getCurrentYear();

    static Instant startOfYear = TimeUtils.getStartOfYear(currentYear);
    static Instant endOfYear = TimeUtils.getEndOfYear(currentYear);
    ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
    private static ConfigServiceForTests configService;

    @BeforeAll
    public static void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        if (new File(rootFolderName).exists()) {
            FileUtils.deleteDirectory(new File(rootFolderName));
        }

        stsStoragePlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_HOUR",
                configService);
        mtsStoragePlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=MTS&rootFolder=" + mediumTermFolderName
                        + "/&partitionGranularity=PARTITION_DAY&hold=" + mtsHold + "&gather=" + mtsGather,
                configService);
        ltsStoragePlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=LTS&rootFolder=" + longTermFolderName
                        + "/&partitionGranularity=PARTITION_DAY&compress=ZIP_PER_PV",
                configService);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        // FileUtils.deleteDirectory(new File(rootFolderName));
        configService.shutdownNow();
    }

    static long dayCount = 10;
    static long mtsGather = 3;
    static long mtsHold = 5;

    static Stream<Arguments> provideConsolidateTest() {
        return Stream.of("LTS", "MTS")
                .flatMap(name -> IntStream.range(1, (int) mtsGather + 1).mapToObj(i -> Arguments.of(name, i)));
    }

    @ParameterizedTest
    @MethodSource("provideConsolidateTest")
    void testConsolidate(String consolidateStorage, int etlTimeDay) throws AlreadyRegisteredException, IOException {
        String pvName = pvNamePrefix + consolidateStorage + etlTimeDay;
        setupConfigService(pvName);

        int eventsGenerated = generateData(pvName, dayCount);

        File shortTermFile = new File(shortTermFolderName);
        checkFileCount(shortTermFile, 24 * dayCount, pvName);

        File mediumTermFile = new File(mediumTermFolderName);
        checkFileCount(mediumTermFile, 0, pvName);

        checkFileCount(new File(longTermFolderName), 0, pvName);

        checkStorageEventCount(pvName, eventsGenerated, stsStoragePlugin);

        Instant etlTime = startOfYear.plusSeconds(
                (dayCount + etlTimeDay) * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk());

        runConsolidation(pvName, consolidateStorage, etlTime);

        checkFileCount(shortTermFile, 0, pvName);

        if (Objects.equals(consolidateStorage, "LTS")) {

            checkFileCount(mediumTermFile, mtsGather - etlTimeDay, pvName);

            checkStorageEventCount(
                    pvName,
                    (int) (PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk()
                            * (dayCount + etlTimeDay - mtsGather)),
                    ltsStoragePlugin);
        } else {

            checkFileCount(mediumTermFile, dayCount, pvName);

            checkStorageEventCount(pvName, eventsGenerated, mtsStoragePlugin);
        }
    }

    private void checkStorageEventCount(String pvName, int eventsGenerated, PlainPBStoragePlugin storagePlugin)
            throws IOException {

        long afterSourceCount = 0;
        try (BasicContext context = new BasicContext();
                EventStream afterSrc = new CurrentThreadWorkerEventStream(
                        pvName, storagePlugin.getDataForPV(context, pvName, startOfYear, endOfYear))) {
            for (@SuppressWarnings("unused") Event e : afterSrc) {
                afterSourceCount++;
            }
        }
        Assertions.assertEquals(eventsGenerated, afterSourceCount);
    }

    private void runConsolidation(String pvName, String consolidateStorage, Instant etlTime) throws IOException {
        // consolidate
        // The ConfigServiceForTests automatically adds a ETL Job for each PV. For consolidate, we need to have "paused"
        // the PV; we fake this by deleting the jobs.
        configService.getETLLookup().deleteETLJobs(pvName);
        ETLExecutor.runPvETLsBeforeOneStorage(configService, etlTime, pvName, consolidateStorage);
        // make sure there are no pb files in short term storage , medium term storage and all files in long term
        // storage
    }

    private static boolean checkFileName(String name, String pvName) {
        return name.startsWith(pvName);
    }

    private static void checkFileCount(File file, long count, String pvName) {
        String[] fileList = file.list((d, name) -> checkFileName(name, pvName));

        assert fileList != null;
        Assertions.assertEquals(count, fileList.length);
    }

    private void setupConfigService(String pvName) throws AlreadyRegisteredException {
        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores = new String[] {
            stsStoragePlugin.getURLRepresentation(),
            mtsStoragePlugin.getURLRepresentation(),
            ltsStoragePlugin.getURLRepresentation()
        };
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();
    }

    private int generateData(String pvName, long dayCount) throws IOException {
        // generate datas of 10 days PB file 2012_01_01.pb  to 2012_01_10.pb

        int runsPerDay = 12;
        int eventsPerRun = PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() / runsPerDay;
        ArrayListEventStream testData =
                new ArrayListEventStream(eventsPerRun, new RemotableEventStreamDesc(type, pvName, currentYear));
        for (int day = 0; day < dayCount; day++) {
            logger.debug("Generating data for day " + 1);
            int startOfDaySeconds = day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
            for (int currentrun = 0; currentrun < runsPerDay; currentrun++) {

                logger.debug("Generating data for run " + currentrun);

                for (int secondsinrun = 0; secondsinrun < eventsPerRun; secondsinrun++) {
                    testData.add(new SimulationEvent(
                            startOfDaySeconds + currentrun * eventsPerRun + secondsinrun,
                            currentYear,
                            type,
                            new ScalarValue<>((double) secondsinrun)));
                }
            }
        } // end for
        try (BasicContext context = new BasicContext()) {
            return stsStoragePlugin.appendData(context, pvName, testData);
        }
    }
}
