/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.plain.PlainCommonSetup;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
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
import java.util.List;
import java.util.stream.Stream;

/**
 * Test retrieval across year spans when some of the data is missing.
 * We generate data for these time periods
 * <ol>
 * <li>Sep 2011 - Oct 2011</li>
 * <li>Jun 2012 - Jul 2012</li>
 * </ol>
 * <p>
 * We then make requests for various time periods and check the first sample and number of samples.
 *
 * @author mshankar
 */
class MissingDataYearSpanRetrievalUnitTest {
    static final String testSpecificFolder = "MissingDataYearSpanRetrievalUnit";
    static final String pvNamePB = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":" + testSpecificFolder;
    private static final Logger logger = LogManager.getLogger(MissingDataYearSpanRetrievalUnitTest.class.getName());
    private static final LinkedList<Instant> generatedTimeStamps = new LinkedList<Instant>();
    static File dataFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ArchUnitTest"
            + File.separator + testSpecificFolder);
    static PlainCommonSetup PBSetup = new PlainCommonSetup();
    static PlainStoragePlugin pbPlugin = new PlainStoragePlugin();

    static Stream<Arguments> provideMissingDataYearSpan() {
        return Stream.of(List.of(pvNamePB, pbPlugin)).flatMap(p -> getArgumentsStream(p.getFirst(), p.get(1)));
    }

    /**
     * Unit test version of {@link MissingDataYearSpanRetrievalTest#getArgumentsStream}
     * has different expectations as only going through {@link PlainStoragePlugin#getDataForPV(BasicContext, String, Instant, Instant)}
     * and not collating different files as in {@link org.epics.archiverappliance.retrieval.DataRetrievalServlet}
     *
     * @param pvName             name of pv
     * @param storagePlugin matching plugin for pvName
     * @return Arguments that match {@link #testRetrieval(String, String, int, String, int, String, PlainStoragePlugin)}
     */
    static Stream<Arguments> getArgumentsStream(Object pvName, Object storagePlugin) {
        return Stream.of(
                Arguments.of(
                        "2011-06-01T00:00:00.000Z", "2011-07-01T00:00:00.000Z", 0, null, -1, pvName, storagePlugin),
                Arguments.of(
                        "2011-08-10T00:00:00.000Z",
                        "2011-09-15T10:00:00.000Z",
                        15,
                        "2011-09-01T00:00:00.111Z",
                        0,
                        pvName,
                        storagePlugin),
                Arguments.of(
                        "2011-09-10T00:00:00.000Z",
                        "2011-09-15T10:00:00.000Z",
                        6,
                        "2011-09-09T00:00:00.111Z",
                        8,
                        pvName,
                        storagePlugin),
                Arguments.of(
                        "2011-09-10T00:00:00.000Z",
                        "2011-10-15T10:00:00.000Z",
                        22,
                        "2011-09-09T00:00:00.111Z",
                        8,
                        pvName,
                        storagePlugin),
                Arguments.of(
                        "2011-10-10T00:00:00.000Z",
                        "2011-10-15T10:00:00.000Z",
                        1,
                        "2011-09-30T00:00:00.111Z",
                        29,
                        pvName,
                        storagePlugin),
                Arguments.of(
                        "2012-01-10T00:00:00.000Z",
                        "2012-01-15T10:00:00.000Z",
                        1,
                        "2011-09-30T00:00:00.111Z",
                        29,
                        pvName,
                        storagePlugin),
                Arguments.of(
                        "2012-01-10T00:00:00.000Z",
                        "2012-06-15T10:00:00.000Z",
                        16,
                        "2011-09-30T00:00:00.111Z",
                        29,
                        pvName,
                        storagePlugin),
                Arguments.of(
                        "2013-01-10T00:00:00.000Z",
                        "2013-01-15T10:00:00.000Z",
                        1,
                        "2012-06-30T00:00:00.111Z",
                        59,
                        pvName,
                        storagePlugin));
    }

    @BeforeAll
    public static void setUp() throws Exception {
        PBSetup.setUpRootFolder(pbPlugin);
        logger.info("Data folder is " + dataFolder.getAbsolutePath());
        FileUtils.deleteDirectory(dataFolder);
        generateData();
    }

    private static void generateData() throws IOException {
        {
            // Generate some data for Sep 2011 - Oct 2011, one per day
            Instant sep2011 = TimeUtils.convertFromISO8601String("2011-09-01T00:00:00.000Z");
            int sep201101secsIntoYear = TimeUtils.getSecondsIntoYear(TimeUtils.convertToEpochSeconds(sep2011));
            short year = 2011;
            generateData(year, sep201101secsIntoYear);
        }

        {
            // Generate some data for Jun 2012 - Jul 2012, one per day
            Instant jun2012 = TimeUtils.convertFromISO8601String("2012-06-01T00:00:00.000Z");
            int jun201201secsIntoYear = TimeUtils.getSecondsIntoYear(TimeUtils.convertToEpochSeconds(jun2012));
            short year = 2012;
            generateData(year, jun201201secsIntoYear);
        }
    }

    private static void generateData(short year, int jun201201secsIntoYear) throws IOException {
        ArrayListEventStream strmPB = new ArrayListEventStream(
                0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvNamePB, year));
        for (int day = 0; day < 30; day++) {
            YearSecondTimestamp yts = new YearSecondTimestamp(
                    year,
                    jun201201secsIntoYear + day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                    111000000);
            strmPB.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));
            generatedTimeStamps.add(TimeUtils.convertFromYearSecondTimestamp(yts));
        }
        try (BasicContext context = new BasicContext()) {
            pbPlugin.appendData(context, pvNamePB, strmPB);
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {
        FileUtils.deleteDirectory(dataFolder);
    }

    /**
     * @param startStr                  - Start time of request
     * @param endStr                    - End time of request
     * @param expectedMinEventCount     - How many events we expect at a minimum
     * @param firstTimeStampExpectedStr - The time stamp of the first event
     * @param firstTSIndex              - If present, the index into generatedTimeStamps for the first event. Set to -1 if you want to skip this check.
     */
    @ParameterizedTest
    @MethodSource("provideMissingDataYearSpan")
    void testRetrieval(
            String startStr,
            String endStr,
            int expectedMinEventCount,
            String firstTimeStampExpectedStr,
            int firstTSIndex,
            String pvName,
            PlainStoragePlugin storagePlugin) {
        logger.info("testRetrieval: {} {} {}", startStr, endStr, pvName);
        Instant start = TimeUtils.convertFromISO8601String(startStr);
        Instant end = TimeUtils.convertFromISO8601String(endStr);
        Instant firstTimeStampExpected = null;
        if (firstTimeStampExpectedStr != null) {
            firstTimeStampExpected = TimeUtils.convertFromISO8601String(firstTimeStampExpectedStr);
        }
        if (firstTSIndex != -1) {
            Assertions.assertEquals(
                    firstTimeStampExpected,
                    generatedTimeStamps.get(firstTSIndex),
                    "Incorrect specification - Str is " + firstTimeStampExpectedStr + " and from array "
                            + TimeUtils.convertToISO8601String(generatedTimeStamps.get(firstTSIndex)));
        }
        String msg = String.format(
                "%s - %s, expected %s, first %s at %s with pv %s",
                start, end, expectedMinEventCount, firstTimeStampExpected, firstTSIndex, pvName);

        Instant obtainedFirstSample = null;
        int eventCount = 0;

        try (EventStream stream = new CurrentThreadWorkerEventStream(
                pvName,
                storagePlugin.getDataForPV(new BasicContext(), pvName, start, end, new DefaultRawPostProcessor()))) {

            for (Event e : stream) {
                if (obtainedFirstSample == null) {
                    obtainedFirstSample = e.getEventTimeStamp();
                }
                Assertions.assertEquals(
                        generatedTimeStamps.get(firstTSIndex + eventCount),
                        e.getEventTimeStamp(),
                        "Expecting sample with timestamp "
                                + TimeUtils.convertToISO8601String(generatedTimeStamps.get(firstTSIndex + eventCount))
                                + " got "
                                + TimeUtils.convertToISO8601String(e.getEventTimeStamp())
                                + " for " + msg);
                eventCount++;
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }

        Assertions.assertTrue(
                eventCount >= expectedMinEventCount,
                "Expecting at least " + expectedMinEventCount + " got " + eventCount + " for " + msg);
        if (firstTimeStampExpected != null) {
            if (obtainedFirstSample == null) {
                Assertions.fail("Expecting at least one value for " + msg);
            } else {
                Assertions.assertEquals(
                        firstTimeStampExpected,
                        obtainedFirstSample,
                        "Expecting first sample to be "
                                + TimeUtils.convertToISO8601String(firstTimeStampExpected)
                                + " got "
                                + TimeUtils.convertToISO8601String(obtainedFirstSample)
                                + " for " + msg);
            }
        } else {
            if (obtainedFirstSample != null) {
                Assertions.fail("Expecting no values for " + msg + " Got value from "
                        + TimeUtils.convertToISO8601String(obtainedFirstSample));
            }
        }
    }
}
