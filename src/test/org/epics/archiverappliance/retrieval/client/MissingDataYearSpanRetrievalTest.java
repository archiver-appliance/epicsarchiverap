/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.stream.Stream;

/**
 * Test retrieval across year spans when some of the data is missing.
 * We generate data for these time periods
 * <ol>
 * <li>Sep 2011 - Oct 2011</li>
 * <li>Jun 2012 - Jul 2012</li>
 * </ol>
 *
 * We then make requests for various time periods and check the first sample and number of samples.
 *
 * @author mshankar
 *
 */
@Tag("integration")
public class MissingDataYearSpanRetrievalTest {
    private static final Logger logger = LogManager.getLogger(MissingDataYearSpanRetrievalTest.class.getName());
    private static final LinkedList<Instant> generatedTimeStamps = new LinkedList<>();
    static String testSpecificFolder = "MissingDataYearSpanRetrieval";
    static String pvNamePB = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":" + testSpecificFolder;
    static File dataFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ArchUnitTest"
            + File.separator + testSpecificFolder);
    static PBCommonSetup PBSetup = new PBCommonSetup();
    static PlainPBStoragePlugin pbPlugin = new PlainPBStoragePlugin();
    static TomcatSetup tomcatSetup = new TomcatSetup();

    @BeforeAll
    public static void setUp() throws Exception {
        PBSetup.setUpRootFolder(pbPlugin);
        logger.info("Data folder is " + dataFolder.getAbsolutePath());
        FileUtils.deleteDirectory(dataFolder);
        generateData();
        tomcatSetup.setUpWebApps(MissingDataYearSpanRetrievalTest.class.getSimpleName());
    }

    private static void generateData() throws IOException {
        {
            // Generate some data for Sep 2011 - Oct 2011, one per day
            Instant sep2011 = TimeUtils.convertFromISO8601String("2011-09-01T00:00:00.000Z");
            int sep201101secsIntoYear = TimeUtils.getSecondsIntoYear(TimeUtils.convertToEpochSeconds(sep2011));
            short year = 2011;
            generateDate(year, sep201101secsIntoYear);
        }

        {
            // Generate some data for Jun 2012 - Jul 2012, one per day
            Instant jun2012 = TimeUtils.convertFromISO8601String("2012-06-01T00:00:00.000Z");
            int jun201201secsIntoYear = TimeUtils.getSecondsIntoYear(TimeUtils.convertToEpochSeconds(jun2012));
            short year = 2012;
            generateDate(year, jun201201secsIntoYear);
        }
    }

    private static void generateDate(short year, int jun201201secsIntoYear) throws IOException {
        ArrayListEventStream strmPB = new ArrayListEventStream(
                0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvNamePB, year));
        for (int day = 0; day < 30; day++) {
            YearSecondTimestamp yts = new YearSecondTimestamp(
                    year,
                    jun201201secsIntoYear + day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                    111000000);
            strmPB.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>(0.0)));
            generatedTimeStamps.add(TimeUtils.convertFromYearSecondTimestamp(yts));
        }
        try (BasicContext context = new BasicContext()) {
            pbPlugin.appendData(context, pvNamePB, strmPB);
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {
        tomcatSetup.tearDown();
        FileUtils.deleteDirectory(dataFolder);
    }

    /**
     * <pre>
     * .....Sep,2011.....Oct,2011..............Jan,1,2012..........Jun,2012......Jul,2012...............Dec,2012......
     * [] - should return no data
     * ...[.....] should return data whose first value should be Sep 1, 2011
     * ............[.....] should return data whose first value is start time - 1
     * .................[...........] should return data whose first value is start time - 1
     * ...................................[.] should return one sample for the last day of Sept, 2011
     * ...................................[...................] should return one sample for the last day of Sept, 2011
     * ................................................[..]  should return one sample for the last day of Sept, 2011
     * ................................................[...............]  should return may samples with the first sample as the last day of Sept, 2011
     * ..................................................................[......]  should return may samples all from 2012
     * ..........................................................................................[..] should return one sample for the last day of Jun, 2012
     * ...........................................................................................................................[..] should return one sample for the last day of Jun, 2012
     * <pre>
     */
    static Stream<Arguments> provideMissingDataYearSpan() {
        return Stream.of(pvNamePB).flatMap(MissingDataYearSpanRetrievalTest::getArgumentsStream);
    }

    static Stream<Arguments> getArgumentsStream(String pvName) {
        return Stream.of(
                Arguments.of("2011-06-01T00:00:00.000Z", "2011-07-01T00:00:00.000Z", 0, null, -1, pvName),
                Arguments.of(
                        "2011-08-10T00:00:00.000Z",
                        "2011-09-15T10:00:00.000Z",
                        15,
                        "2011-09-01T00:00:00.111Z",
                        0,
                        pvName),
                Arguments.of(
                        "2011-09-10T00:00:00.000Z",
                        "2011-09-15T10:00:00.000Z",
                        6,
                        "2011-09-09T00:00:00.111Z",
                        8,
                        pvName),
                Arguments.of(
                        "2011-09-10T00:00:00.000Z",
                        "2011-10-15T10:00:00.000Z",
                        22,
                        "2011-09-09T00:00:00.111Z",
                        8,
                        pvName),
                Arguments.of(
                        "2011-10-10T00:00:00.000Z",
                        "2011-10-15T10:00:00.000Z",
                        1,
                        "2011-09-30T00:00:00.111Z",
                        29,
                        pvName),
                Arguments.of(
                        "2011-10-10T00:00:00.000Z",
                        "2012-01-15T10:00:00.000Z",
                        1,
                        "2011-09-30T00:00:00.111Z",
                        29,
                        pvName),
                Arguments.of(
                        "2012-01-10T00:00:00.000Z",
                        "2012-01-15T10:00:00.000Z",
                        1,
                        "2011-09-30T00:00:00.111Z",
                        29,
                        pvName),
                Arguments.of(
                        "2012-01-10T00:00:00.000Z",
                        "2012-06-15T10:00:00.000Z",
                        16,
                        "2011-09-30T00:00:00.111Z",
                        29,
                        pvName),
                Arguments.of(
                        "2012-06-10T00:00:00.000Z",
                        "2012-06-15T10:00:00.000Z",
                        6,
                        "2012-06-09T00:00:00.111Z",
                        38,
                        pvName),
                Arguments.of(
                        "2012-09-10T00:00:00.000Z",
                        "2012-09-15T10:00:00.000Z",
                        1,
                        "2012-06-30T00:00:00.111Z",
                        59,
                        pvName),
                Arguments.of(
                        "2013-01-10T00:00:00.000Z",
                        "2013-01-15T10:00:00.000Z",
                        1,
                        "2012-06-30T00:00:00.111Z",
                        59,
                        pvName));
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
            String pvName)
            throws IOException {

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

        RawDataRetrievalAsEventStream rawDataRetrieval =
                new RawDataRetrievalAsEventStream(ConfigServiceForTests.RAW_RETRIEVAL_URL);
        Instant obtainedFirstSample = null;
        int eventCount = 0;
        try (EventStream stream = rawDataRetrieval.getDataForPVS(new String[] {pvName}, start, end, null)) {
            if (stream != null) {
                for (Event e : stream) {
                    if (obtainedFirstSample == null) {
                        obtainedFirstSample = e.getEventTimeStamp();
                    }
                    Assertions.assertEquals(
                            e.getEventTimeStamp(),
                            generatedTimeStamps.get(firstTSIndex + eventCount),
                            "Expecting sample with timestamp "
                                    + TimeUtils.convertToISO8601String(
                                            generatedTimeStamps.get(firstTSIndex + eventCount))
                                    + " got "
                                    + TimeUtils.convertToISO8601String(e.getEventTimeStamp())
                                    + " for " + msg);
                    eventCount++;
                }
            } else {
                logger.info("Stream is null for " + msg);
            }
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
