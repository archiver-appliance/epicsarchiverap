/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.plain.FileExtension;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Test the ETL source functionality of PlainStoragePlugin
 * @author mshankar
 *
 */
public class ETLSourceGetStreamsTest {
    private static ConfigService configService;

    File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ETLSrcStreamsTest");
    short currentYear = TimeUtils.getCurrentYear();
    ZonedDateTime startOfToday = ZonedDateTime.now(ZoneId.from(ZoneOffset.UTC))
            .withHour(0)
            .withMinute(0)
            .withSecond(0);

    static Stream<Arguments> provideTestArguments() {
        return Arrays.stream(FileExtension.values())
                .flatMap(f -> Stream.of(
                        Arguments.of(
                                f,
                                PartitionGranularity.PARTITION_5MIN,
                                PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk(),
                                600), // One hour; sample every 10 mins
                        Arguments.of(
                                f,
                                PartitionGranularity.PARTITION_15MIN,
                                PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk(),
                                600), // One hour; sample every 10 mins
                        Arguments.of(
                                f,
                                PartitionGranularity.PARTITION_30MIN,
                                PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk() * 4,
                                600), // Four hours day; sample every 10 mins
                        Arguments.of(
                                f,
                                PartitionGranularity.PARTITION_HOUR,
                                PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk() * 5,
                                600), // 5 hours ; sample every 10 mins
                        Arguments.of(
                                f,
                                PartitionGranularity.PARTITION_DAY,
                                PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 7,
                                PartitionGranularity.PARTITION_30MIN
                                        .getApproxSecondsPerChunk()), // One week; sample every 30 mins
                        Arguments.of(
                                f,
                                PartitionGranularity.PARTITION_MONTH,
                                PartitionGranularity.PARTITION_YEAR.getApproxSecondsPerChunk(),
                                PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk()
                                        * 15), // One year; sample every 1/2 MONTH
                        Arguments.of(
                                f,
                                PartitionGranularity.PARTITION_YEAR,
                                PartitionGranularity.PARTITION_YEAR.getApproxSecondsPerChunk() * 10,
                                PartitionGranularity.PARTITION_MONTH
                                        .getApproxSecondsPerChunk()) // 10 years; sample every MONTH
                        ));
    }

    @BeforeAll
    public static void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        configService.shutdownNow();
    }

    @ParameterizedTest
    @MethodSource("provideTestArguments")
    public void getETLStreams(
            FileExtension fileExtension, PartitionGranularity partitionGranularity, long sampleRange, long skipSeconds)
            throws Exception {
        PlainStoragePlugin pbplugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                fileExtension.getSuffix() + "://localhost?name=STS&rootFolder=" + testFolder
                        + "/src&partitionGranularity=PARTITION_HOUR",
                configService);
        ETLContext etlContext = new ETLContext();

        File rootFolder = new File(testFolder.getAbsolutePath()
                + File.separator
                + partitionGranularity.toString()
                + File.separator
                + fileExtension);
        rootFolder.mkdirs();
        pbplugin.setRootFolder(rootFolder.getAbsolutePath());
        pbplugin.setPartitionGranularity(partitionGranularity);
        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":ETLSourceGetStreamsTest:"
                + partitionGranularity + fileExtension;
        ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
        ArrayListEventStream testData =
                new ArrayListEventStream(1000, new RemotableEventStreamDesc(type, pvName, currentYear));
        for (long i = 0; i < sampleRange; i += skipSeconds) {
            testData.add(new SimulationEvent(
                    TimeUtils.getSecondsIntoYear(startOfToday.toEpochSecond() + i),
                    TimeUtils.computeYearForEpochSeconds(startOfToday.toEpochSecond() + i),
                    type,
                    new ScalarValue<>((double) i)));
        }
        try (BasicContext context = new BasicContext()) {
            pbplugin.appendData(context, pvName, testData);
        }
        // This should have generated many files; one for each partition.
        // So we now check the number of files we get as we cruise thru the whole day.

        int expectedFiles = 0;
        Instant firstSecondOfNextPartition =
                TimeUtils.getNextPartitionFirstSecond(startOfToday.toInstant(), partitionGranularity);
        for (long i = 0; i < sampleRange; i += skipSeconds) {
            Instant currentTime = startOfToday.toInstant().plusSeconds(i);
            if (currentTime.isAfter(firstSecondOfNextPartition) || currentTime.equals(firstSecondOfNextPartition)) {
                firstSecondOfNextPartition = TimeUtils.getNextPartitionFirstSecond(currentTime, partitionGranularity);
                expectedFiles++;
            }
            List<ETLInfo> ETLFiles = pbplugin.getETLStreams(pvName, currentTime, etlContext);
            Assertions.assertTrue(
                    (ETLFiles != null) ? (ETLFiles.size() == expectedFiles) : (expectedFiles == 0),
                    "getETLStream failed for "
                            + TimeUtils.convertToISO8601String(currentTime)
                            + " for partition " + partitionGranularity
                            + " Expected " + expectedFiles + " got "
                            + (ETLFiles != null ? Integer.toString(ETLFiles.size()) : "null"));
        }
    }
}
