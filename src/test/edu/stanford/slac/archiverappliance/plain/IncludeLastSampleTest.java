/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

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
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
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
import java.util.LinkedList;
import java.util.stream.Stream;

class IncludeLastSampleTest {
    static final String testSpecificFolder = "IncludeLastSampleTest";
    static final String pvNamePB =
            ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":" + PlainStorageType.PB + testSpecificFolder;
    static final String pvNameParquet =
            ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":" + PlainStorageType.PARQUET + testSpecificFolder;
    private static final Logger logger = LogManager.getLogger(IncludeLastSampleTest.class.getName());
    private static LinkedList<Instant> generatedTimeStamps = new LinkedList<Instant>();
    static File dataFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ArchUnitTest"
            + File.separator + testSpecificFolder);
    static PlainCommonSetup pbSetup = new PlainCommonSetup();
    static PlainCommonSetup parquetSetup = new PlainCommonSetup();
    static PlainStoragePlugin pbPlugin = new PlainStoragePlugin(PlainStorageType.PB);
    static PlainStoragePlugin parquetPlugin = new PlainStoragePlugin(PlainStorageType.PARQUET);
    private static final short currentYear = TimeUtils.getCurrentYear();

    @BeforeAll
    public static void setUp() throws Exception {
        pbSetup.setUpRootFolder(pbPlugin, testSpecificFolder, PartitionGranularity.PARTITION_DAY);
        parquetSetup.setUpRootFolder(parquetPlugin, testSpecificFolder, PartitionGranularity.PARTITION_DAY);
        logger.info("Data folder is " + dataFolder.getAbsolutePath());
        FileUtils.deleteDirectory(dataFolder);
        generatedTimeStamps = generateData(pvNameParquet, parquetPlugin);
        generateData(pvNamePB, pbPlugin);
    }

    private static LinkedList<Instant> generateData(String pvName, PlainStoragePlugin plugin) throws IOException {
        LinkedList<Instant> timestamps = new LinkedList<>();
        ArrayListEventStream strmPB = new ArrayListEventStream(
                0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));

        YearSecondTimestamp yts = new YearSecondTimestamp(currentYear, 1, 10);
        strmPB.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));

        yts = new YearSecondTimestamp(
                currentYear, 1 + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(), 20);
        strmPB.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));
        timestamps.add(TimeUtils.convertFromYearSecondTimestamp(yts));

        yts = new YearSecondTimestamp(
                currentYear, 1 + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 2, 30);
        strmPB.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));
        timestamps.add(TimeUtils.convertFromYearSecondTimestamp(yts));

        yts = new YearSecondTimestamp(
                currentYear, 1 + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 3, 40);
        strmPB.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));
        timestamps.add(TimeUtils.convertFromYearSecondTimestamp(yts));

        try (BasicContext context = new BasicContext()) {
            plugin.appendData(context, pvName, strmPB);
        }
        return timestamps;
    }

    @AfterAll
    public static void tearDown() throws Exception {
        FileUtils.deleteDirectory(dataFolder);
    }

    static Stream<Arguments> provideIncludeLastSample() {
        return Stream.of(Arguments.of(pbPlugin, pvNamePB), Arguments.of(parquetPlugin, pvNameParquet));
    }

    @ParameterizedTest
    @MethodSource("provideIncludeLastSample")
    void testRetrieval(PlainStoragePlugin plugin, String pvName) {
        Instant start = TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(
                currentYear, 20 + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(), 1));
        Instant end = TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(
                currentYear, PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 10, 1));
        int eventCount = 0;
        LinkedList<Instant> actualTimestamps = new LinkedList<>();
        try (EventStream stream = new CurrentThreadWorkerEventStream(
                pvName, plugin.getDataForPV(new BasicContext(), pvName, start, end, new DefaultRawPostProcessor()))) {

            for (Event e : stream) {
                actualTimestamps.add(e.getEventTimeStamp());
                eventCount++;
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
        Assertions.assertEquals(generatedTimeStamps, actualTimestamps);
        Assertions.assertEquals(3, eventCount);
    }
}
