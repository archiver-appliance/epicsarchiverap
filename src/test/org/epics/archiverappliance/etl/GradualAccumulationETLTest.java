/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PB.data.PlainCommonSetup;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.exception.ConfigException;
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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Complex test for testing ETL as it is supposed to happen in production.
 * @author mshankar
 *
 */
public class GradualAccumulationETLTest {
    private static final Logger logger = LogManager.getLogger(GradualAccumulationETLTest.class.getName());
    static DefaultConfigService configService;
    static List<ETLTestPlugins> etlPlugins;
    int ratio = 10; // Size of the test

    @BeforeAll
    public static void setup() throws ConfigException {
        etlPlugins = ETLTestPlugins.generatePlugins();
        configService = new ConfigServiceForTests(new File("./bin"), 1);
    }

    @AfterAll
    public static void tearDown() {

        configService.shutdownNow();
    }

    /**
     * In a running appserver, the sequence is expected to be as follows
     * <ol>
     * <li>The engine generates data for a PV at a certain rate.</li>
     * <li>ETL runs every so often and copies a subset (the older portion) of the data over to the destination store.</li>
     * <li>The engine continues to generate data.</li>
     * </ol>
     * This aims to test that. We generate data and then fake the time for ETL
     */
    public static Stream<Arguments> provideGradualAccumulation() {
        return Arrays.stream(PartitionGranularity.values())
                .filter(g -> g.getNextLargerGranularity() != null)
                .flatMap(g -> etlPlugins.stream()
                        .flatMap(
                                plugins -> Stream.of(Arguments.of(g, true, plugins), Arguments.of(g, false, plugins))));
    }

    @ParameterizedTest
    @MethodSource("provideGradualAccumulation")
    public void testGradualAccumulation(
            PartitionGranularity granularity, boolean backupFiles, ETLTestPlugins testPlugins) throws Exception {

        PlainCommonSetup srcSetup = new PlainCommonSetup();
        PlainCommonSetup destSetup = new PlainCommonSetup();
        testPlugins.dest().setBackupFilesBeforeETL(backupFiles);

        srcSetup.setUpRootFolder(testPlugins.src(), "GradualAccumulationETLTestSrc_" + granularity, granularity);
        destSetup.setUpRootFolder(
                testPlugins.dest(),
                "GradualAccumulationETLTestDest_" + granularity,
                granularity.getNextLargerGranularity());

        logger.info("Testing gradual accumulation for " + testPlugins.src().getPartitionGranularity() + " to "
                + testPlugins.dest().getPartitionGranularity() + " with backup = " + backupFiles);

        short year = TimeUtils.getCurrentYear();
        long startOfYearInEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
        long curEpochSeconds = startOfYearInEpochSeconds;
        int secondsInTestingPeriod = 0;
        int incrementSeconds = granularity.getApproxSecondsPerChunk() / ratio;
        int eventsgenerated = 0;

        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + testPlugins.pvNamePrefix() + backupFiles
                + "ETL_testGradual" + granularity;

        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores = new String[] {
            testPlugins.src().getURLRepresentation(), testPlugins.dest().getURLRepresentation()
        };
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();

        while (secondsInTestingPeriod < granularity.getApproxSecondsPerChunk() * ratio) {
            ArrayListEventStream inStream = new ArrayListEventStream(
                    incrementSeconds, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
            for (int i = 0; i < ratio; i++) {
                inStream.add(new SimulationEvent(
                        secondsInTestingPeriod, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>((double)
                                secondsInTestingPeriod)));
                secondsInTestingPeriod += incrementSeconds;
                curEpochSeconds += incrementSeconds;
                eventsgenerated++;
            }
            try (BasicContext context = new BasicContext()) {
                testPlugins.src().appendData(context, pvName, inStream);
            }
            ETLExecutor.runETLs(configService, TimeUtils.convertFromEpochSeconds(curEpochSeconds, 0));
            logger.debug("Done performing ETL");
            checkDataValidity(
                    pvName,
                    testPlugins.src(),
                    testPlugins.dest(),
                    startOfYearInEpochSeconds,
                    incrementSeconds,
                    eventsgenerated,
                    granularity + "/" + backupFiles);
        }

        srcSetup.deleteTestFolder();
        destSetup.deleteTestFolder();
    }

    private void checkDataValidity(
            String pvName,
            PlainStoragePlugin etlSrc,
            PlainStoragePlugin etlDest,
            long startOfYearInEpochSeconds,
            int incrementSeconds,
            int eventsgenerated,
            String testDesc)
            throws IOException {
        Instant startOfRequest = TimeUtils.minusDays(TimeUtils.now(), 2 * 366);
        Instant endOfRequest = TimeUtils.plusDays(TimeUtils.now(), 2 * 366);

        logger.debug(testDesc + "Asking for data between"
                + TimeUtils.convertToHumanReadableString(startOfRequest)
                + " and "
                + TimeUtils.convertToHumanReadableString(endOfRequest));

        long expectedEpochSeconds = startOfYearInEpochSeconds;
        int afterCount = 0;

        try (BasicContext context = new BasicContext();
                EventStream afterDest = new CurrentThreadWorkerEventStream(
                        pvName, etlDest.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
            for (Event e : afterDest) {
                Assertions.assertEquals(
                        expectedEpochSeconds,
                        e.getEpochSeconds(),
                        testDesc + "Expected seconds "
                                + TimeUtils.convertToHumanReadableString(expectedEpochSeconds)
                                + " is not the same as actual seconds "
                                + TimeUtils.convertToHumanReadableString(e.getEpochSeconds()));
                expectedEpochSeconds += incrementSeconds;
                afterCount++;
            }
            Assertions.assertTrue(afterCount != 0, testDesc + "Seems like no events were moved by ETL " + afterCount);
        }

        try (BasicContext context = new BasicContext();
                EventStream afterSrc = new CurrentThreadWorkerEventStream(
                        pvName, etlSrc.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
            for (Event e : afterSrc) {
                Assertions.assertEquals(
                        expectedEpochSeconds,
                        e.getEpochSeconds(),
                        testDesc + "Expected seconds "
                                + TimeUtils.convertToHumanReadableString(expectedEpochSeconds)
                                + " is not the same as actual seconds "
                                + TimeUtils.convertToHumanReadableString(e.getEpochSeconds()));
                expectedEpochSeconds += incrementSeconds;
                afterCount++;
            }
        }

        Assertions.assertEquals(
                eventsgenerated,
                afterCount,
                testDesc + "Expected total events " + eventsgenerated + " is not the same as actual events "
                        + afterCount);
    }
}
