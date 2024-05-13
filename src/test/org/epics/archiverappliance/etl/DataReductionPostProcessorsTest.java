/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessors;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * Variation of DataReductionDailyETLTest; except we test multiple post processors
 * @author mshankar
 *
 */
public class DataReductionPostProcessorsTest {
    private static final Logger logger = LogManager.getLogger(DataReductionPostProcessorsTest.class);

    static final ConfigServiceForTests configService;

    static {
        try {
            configService = new ConfigServiceForTests(1);
        } catch (ConfigException e) {
            throw new RuntimeException(e);
        }
    }

    public static Stream<Arguments> provideReduceDataUsing() {
        return Stream.of(
                // No fill versions
                Arguments.of("lastSample_3600"),
                Arguments.of("firstSample_3600"),
                Arguments.of("firstSample_600"),
                Arguments.of("lastSample_600"),
                Arguments.of("meanSample_3600"),
                Arguments.of("meanSample_600"),
                Arguments.of("meanSample_1800"),
                Arguments.of("minSample_3600"),
                Arguments.of("maxSample_3600"),
                Arguments.of("medianSample_3600"),
                // Fill versions)
                Arguments.of("mean_3600"),
                Arguments.of("mean_600"),
                Arguments.of("mean_1800"),
                Arguments.of("min_3600"),
                Arguments.of("max_3600"),
                Arguments.of("median_3600"),
                Arguments.of("firstFill_3600"),
                Arguments.of("lastFill_3600"));
    }

    @AfterAll
    public static void afterAll() {
        configService.shutdownNow();
    }

    private void cleanDataFolders(String shortTermFolderName, String mediumTermFolderName, String longTermFolderName)
            throws IOException {
        if (new File(shortTermFolderName).exists()) {
            FileUtils.deleteDirectory(new File(shortTermFolderName));
        }
        if (new File(mediumTermFolderName).exists()) {
            FileUtils.deleteDirectory(new File(mediumTermFolderName));
        }
        if (new File(longTermFolderName).exists()) {
            FileUtils.deleteDirectory(new File(longTermFolderName));
        }
    }

    /**
     * 1) Set up the raw and reduced PV's
     * 2) Generate data in STS
     * 3) Run ETL
     * 4) Compare
     */
    @ParameterizedTest
    @MethodSource("provideReduceDataUsing")
    public void testPostProcessor(String reduceDataUsing) throws Exception {
        logger.info("Testing for " + reduceDataUsing);
        final String rawPVName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX
                + DataReductionPostProcessorsTest.class.getSimpleName()
                + reduceDataUsing
                + PlainStoragePlugin.pbFileSuffix;
        final String reducedPVName = rawPVName + "reduced";

        String shortTermFolderName =
                ConfigServiceForTests.getDefaultShortTermFolder() + String.format("/%s/shortTerm", reduceDataUsing);
        String mediumTermFolderName =
                ConfigServiceForTests.getDefaultPBTestFolder() + String.format("/%s/mediumTerm", reduceDataUsing);
        String longTermFolderName =
                ConfigServiceForTests.getDefaultPBTestFolder() + String.format("/%s/longTerm", reduceDataUsing);
        cleanDataFolders(shortTermFolderName, mediumTermFolderName, longTermFolderName);
        // Set up the raw and reduced PV's
        PlainStoragePlugin etlSTS = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_HOUR",
                configService);
        PlainStoragePlugin etlMTS = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=MTS&rootFolder=" + mediumTermFolderName + "/&partitionGranularity=PARTITION_DAY",
                configService);
        PlainStoragePlugin etlLTSRaw = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                PlainStoragePlugin.pbFileSuffix + "://localhost?name=LTS&rootFolder=" + longTermFolderName
                        + "/&partitionGranularity=PARTITION_YEAR",
                configService);
        PlainStoragePlugin etlLTSReduced = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                PlainStoragePlugin.pbFileSuffix + "://localhost?name=LTS&rootFolder=" + longTermFolderName
                        + "/&partitionGranularity=PARTITION_YEAR&reducedata=" + reduceDataUsing,
                configService);
        {
            PVTypeInfo typeInfo = new PVTypeInfo(rawPVName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
            String[] dataStores = new String[] {
                etlSTS.getURLRepresentation(), etlMTS.getURLRepresentation(), etlLTSRaw.getURLRepresentation()
            };
            typeInfo.setDataStores(dataStores);
            typeInfo.setPaused(true);
            configService.updateTypeInfoForPV(rawPVName, typeInfo);
            configService.registerPVToAppliance(rawPVName, configService.getMyApplianceInfo());
        }
        {
            PVTypeInfo typeInfo = new PVTypeInfo(reducedPVName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
            String[] dataStores = new String[] {
                etlSTS.getURLRepresentation(), etlMTS.getURLRepresentation(), etlLTSReduced.getURLRepresentation()
            };
            typeInfo.setDataStores(dataStores);
            typeInfo.setPaused(true);
            configService.updateTypeInfoForPV(reducedPVName, typeInfo);
            configService.registerPVToAppliance(reducedPVName, configService.getMyApplianceInfo());
        }
        // Control ETL manually
        configService.getETLLookup().manualControlForUnitTests();

        short currentYear = TimeUtils.getCurrentYear();

        logger.info("Testing data reduction for postprocessor " + reduceDataUsing);

        for (int day = 0; day < 4; day++) {
            // Generate data into the STS on a daily basis
            ArrayListEventStream genDataRaw = new ArrayListEventStream(
                    PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                    new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, rawPVName, currentYear));
            ArrayListEventStream genDataReduced = new ArrayListEventStream(
                    PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                    new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, reducedPVName, currentYear));
            for (int second = 0; second < PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(); second++) {
                YearSecondTimestamp ysts = new YearSecondTimestamp(
                        currentYear, day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() + second, 0);
                Instant ts = TimeUtils.convertFromYearSecondTimestamp(ysts);
                genDataRaw.add(
                        new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<Double>(second * 1.0), 0, 0));
                genDataReduced.add(
                        new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<Double>(second * 1.0), 0, 0));
            }

            try (BasicContext context = new BasicContext()) {
                etlSTS.appendData(context, rawPVName, genDataRaw);
                etlSTS.appendData(context, reducedPVName, genDataReduced);
            }
            logger.debug("For postprocessor " + reduceDataUsing + " done generating data into the STS for day " + day);

            // Run ETL at the end of the day
            Instant timeETLruns = TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(
                    currentYear, day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() + 86399, 0));
            ETLExecutor.runETLs(configService, timeETLruns);
            logger.debug("For postprocessor " + reduceDataUsing + " done performing ETL as though today is "
                    + TimeUtils.convertToHumanReadableString(timeETLruns));

            // Compare data for raw+postprocessor and reduced PV's.
            PostProcessor postProcessor = PostProcessors.findPostProcessor(reduceDataUsing);
            postProcessor.initialize(reduceDataUsing, rawPVName);

            int rawWithPPCount = 0;
            int reducedCount = 0;

            try (BasicContext context = new BasicContext()) {
                Instant startTime = TimeUtils.minusDays(TimeUtils.now(), 10 * 366);
                Instant endTime = TimeUtils.plusDays(TimeUtils.now(), 10 * 366);
                LinkedList<Instant> rawTimestamps = new LinkedList<Instant>();
                LinkedList<Instant> reducedTimestamps = new LinkedList<Instant>();
                if (postProcessor instanceof PostProcessorWithConsolidatedEventStream) {
                    List<Callable<EventStream>> callables =
                            etlLTSRaw.getDataForPV(context, rawPVName, startTime, endTime, postProcessor);
                    for (Callable<EventStream> callable : callables) {
                        callable.call();
                    }
                    for (Event e :
                            ((PostProcessorWithConsolidatedEventStream) postProcessor).getConsolidatedEventStream()) {
                        rawTimestamps.add(e.getEventTimeStamp());
                        rawWithPPCount++;
                    }
                } else {
                    try (EventStream rawWithPP = new CurrentThreadWorkerEventStream(
                            rawPVName, etlLTSRaw.getDataForPV(context, rawPVName, startTime, endTime, postProcessor))) {
                        for (Event e : rawWithPP) {
                            rawTimestamps.add(e.getEventTimeStamp());
                            rawWithPPCount++;
                        }
                    }
                }
                try (EventStream reduced = new CurrentThreadWorkerEventStream(
                        reducedPVName, etlLTSReduced.getDataForPV(context, reducedPVName, startTime, endTime))) {
                    for (Event e : reduced) {
                        reducedTimestamps.add(e.getEventTimeStamp());
                        reducedCount++;
                    }
                }

                logger.debug("For postprocessor " + reduceDataUsing + " for day " + day + " we have " + rawWithPPCount
                        + " raw with postprocessor events and " + reducedCount + " reduced events");
                if (rawTimestamps.size() != reducedTimestamps.size()) {
                    while (!rawTimestamps.isEmpty() || !reducedTimestamps.isEmpty()) {
                        if (!rawTimestamps.isEmpty())
                            logger.info("Raw/PP " + TimeUtils.convertToHumanReadableString(rawTimestamps.pop()));
                        if (!reducedTimestamps.isEmpty())
                            logger.info("Reduced" + TimeUtils.convertToHumanReadableString(reducedTimestamps.pop()));
                    }
                }
                Assertions.assertEquals(
                        reducedCount,
                        rawWithPPCount,
                        "For postprocessor " + reduceDataUsing + " for day " + day + " we have " + rawWithPPCount
                                + " rawWithPP events and " + reducedCount + " reduced events");
            }
            if (day > 2) {
                Assertions.assertTrue(
                        (rawWithPPCount != 0),
                        "For postprocessor " + reduceDataUsing + " for day " + day
                                + ", seems like no events were moved by ETL into LTS for " + rawPVName + " Count = "
                                + rawWithPPCount);
                Assertions.assertTrue(
                        (reducedCount != 0),
                        "For postprocessor " + reduceDataUsing + " for day " + day
                                + ", seems like no events were moved by ETL into LTS for " + reducedPVName + " Count = "
                                + reducedCount);
            }
        }
    }
}
