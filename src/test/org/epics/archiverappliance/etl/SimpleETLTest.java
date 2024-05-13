/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.plain.PlainPathNameUtility;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin.CompressionMode;
import edu.stanford.slac.archiverappliance.plain.utils.ValidatePBFile;
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
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Very basic ETL tests.
 * @author mshankar
 *
 */
public class SimpleETLTest {
    private static final Logger logger = LogManager.getLogger(SimpleETLTest.class);
    static PBCommonSetup srcSetup = new PBCommonSetup();
    static PBCommonSetup destSetup = new PBCommonSetup();
    static ConfigServiceForTests configService;
    static long ratio = 5;
    static List<ETLTestPlugins> etlPlugins;

    static Stream<Arguments> provideTestInput() {
        return Arrays.stream(new PartitionGranularity[] {PartitionGranularity.PARTITION_MONTH})
                .filter(g -> g.getNextLargerGranularity() != null)
                .flatMap(g -> etlPlugins.stream().flatMap(plugins -> Stream.of(Arguments.of(g, plugins))));
    }

    @BeforeAll
    public static void setUp() throws ConfigException {
        etlPlugins = ETLTestPlugins.generatePlugins();

        configService = new ConfigServiceForTests(new File("./bin"), 1);
    }

    @AfterAll
    public static void tearDown() throws IOException {

        configService.shutdownNow();
    }

    @AfterEach
    public void tearDownEach() throws IOException {

        srcSetup.deleteTestFolder();
        destSetup.deleteTestFolder();
    }

    /**
     * Generates some data in STS; then calls the ETL to move it to MTS and checks that the total amount of data before and after is the same.
     */
    @ParameterizedTest
    @MethodSource("provideTestInput")
    public void testMove(PartitionGranularity granularity, ETLTestPlugins testPlugins) throws Exception {
        srcSetup.setUpRootFolder(
                testPlugins.src(),
                "SimpleETLTestSrc_" + granularity + testPlugins.src().pluginIdentifier(),
                granularity);
        destSetup.setUpRootFolder(
                testPlugins.dest(),
                "SimpleETLTestDest" + granularity + testPlugins.dest().pluginIdentifier(),
                granularity.getNextLargerGranularity());

        logger.info("Testing simple ETL testMove for " + testPlugins.src().getPartitionGranularity() + " to "
                + testPlugins.dest().getPartitionGranularity());

        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + testPlugins.pvNamePrefix() + "ETL_testMove"
                + granularity;

        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores = new String[] {
            testPlugins.src().getURLRepresentation(), testPlugins.dest().getURLRepresentation()
        };
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();

        short currentYear = TimeUtils.getCurrentYear();

        Instant startTime = TimeUtils.getStartOfYear(currentYear);
        Instant endTime =
                startTime.plusSeconds(granularity.getNextLargerGranularity().getApproxSecondsPerChunk() * ratio);
        SimulationEventStream simstream = new SimulationEventStream(
                ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0), startTime, endTime, (int)
                        (granularity.getApproxSecondsPerChunk() / ratio));
        try (BasicContext context = new BasicContext()) {
            testPlugins.src().appendData(context, pvName, simstream);
        }
        logger.info("Done creating src data for PV " + pvName);

        Instant timeETLruns =
                endTime.plusSeconds(granularity.getNextLargerGranularity().getApproxSecondsPerChunk() + 1);
        Instant startOfRequest = TimeUtils.minusDays(timeETLruns, 366);
        Instant endOfRequest = TimeUtils.plusDays(timeETLruns, 366);
        long beforeCount = 0;
        try (BasicContext context = new BasicContext();
                EventStream before = new CurrentThreadWorkerEventStream(
                        pvName, testPlugins.src().getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
            for (@SuppressWarnings("unused") Event e : before) {
                beforeCount++;
            }
        }

        ETLExecutor.runETLs(configService, timeETLruns);
        logger.info("Done performing ETL");

        // Check that all the files in the destination store are valid files.
        Path[] allPaths = PlainPathNameUtility.getAllPathsForPV(
                new ArchPaths(),
                testPlugins.dest().getRootFolder(),
                pvName,
                testPlugins.dest().getExtensionString(),
                testPlugins.dest().getPartitionGranularity(),
                PlainPBStoragePlugin.CompressionMode.NONE,
                configService.getPVNameToKeyConverter());
        Assertions.assertNotNull(allPaths, "PlainPBFileNameUtility returns null for getAllFilesForPV for " + pvName);
        Assertions.assertTrue(
                allPaths.length > 0,
                "PlainPBFileNameUtility returns empty array for getAllFilesForPV for " + pvName + " when looking in "
                        + testPlugins.dest().getRootFolder());

        for (Path destPath : allPaths) {
            Assertions.assertTrue(
                    ValidatePBFile.validatePBFile(destPath, true),
                    "File validation failed for " + destPath.toAbsolutePath());
        }

        logger.info("Asking for data between"
                + TimeUtils.convertToHumanReadableString(startOfRequest)
                + " and "
                + TimeUtils.convertToHumanReadableString(endOfRequest));

        long afterCount = 0;
        try (BasicContext context = new BasicContext();
                EventStream afterDest = new CurrentThreadWorkerEventStream(
                        pvName, testPlugins.dest().getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
            Assertions.assertNotNull(afterDest);
            for (@SuppressWarnings("unused") Event e : afterDest) {
                afterCount++;
            }
        }
        logger.info("Of the " + beforeCount + " events, " + afterCount + " events were moved into the dest store.");
        Assertions.assertTrue(afterCount != 0, "Seems like no events were moved by ETL " + afterCount);
        try (BasicContext context = new BasicContext();
                EventStream afterSrc = new CurrentThreadWorkerEventStream(
                        pvName, testPlugins.src().getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
            for (@SuppressWarnings("unused") Event e : afterSrc) {
                afterCount++;
            }
        }

        Assertions.assertEquals(
                beforeCount, afterCount, "Before count " + beforeCount + " and after count " + afterCount + " differ");
    }
}
