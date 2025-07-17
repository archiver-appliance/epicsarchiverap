/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;

/**
 * Test TimeUtils.
 * @author mshankar
 *
 */
public class TimeUtilsTest {
    private static final Logger logger = LogManager.getLogger(TimeUtilsTest.class.getName());
    private final LinkedList<Instant> testcases = new LinkedList<Instant>();

    private static boolean compareTimeSpans(List<TimeSpan> spans, String isoformattedlist) {
        String[] times = isoformattedlist.split(",");
        if (times.length != spans.size() * 2) {
            logger.warn("The string array has " + times.length + " elements and the time span list has " + spans.size()
                    + " elements");
            return false;
        }
        int currTimeIndex = 0;
        for (TimeSpan span : spans) {
            String expectedStartTime = times[currTimeIndex++];
            String expectedEndTime = times[currTimeIndex++];
            Instant expectedStartTS = TimeUtils.convertFromISO8601String(expectedStartTime);
            Instant expectedEndTS = TimeUtils.convertFromISO8601String(expectedEndTime);
            if (!expectedStartTS.equals(span.getStartTime())) {
                logger.warn("Expected start of " + expectedStartTime + " obtained "
                        + TimeUtils.convertToISO8601String(span.getStartTime()));
                return false;
            }
            if (!expectedEndTS
                    .truncatedTo(ChronoUnit.MILLIS)
                    .equals(span.getEndTime().truncatedTo(ChronoUnit.MILLIS))) {
                logger.warn("Expected end of " + expectedEndTime + " obtained "
                        + TimeUtils.convertToISO8601String(span.getEndTime()));
                return false;
            }
        }

        if (currTimeIndex != times.length) {
            logger.warn("We are missing some expected values");
            for (; currTimeIndex < times.length; currTimeIndex++) {
                logger.warn("We are missing value " + times[currTimeIndex++]);
            }
            return false;
        }

        return true;
    }

    private static void testNextEquals(String tsstr, PartitionGranularity granularity, String expectedStr) {
        Assertions.assertEquals(
                expectedStr,
                TimeUtils.convertToISO8601String(
                        TimeUtils.getNextPartitionFirstSecond(TimeUtils.convertFromISO8601String(tsstr), granularity)));
    }

    private static void testPrevEquals(String tsstr, PartitionGranularity granularity, String expectedStr) {
        Assertions.assertEquals(
                expectedStr,
                TimeUtils.convertToISO8601String(TimeUtils.getPreviousPartitionLastSecond(
                        TimeUtils.convertFromISO8601String(tsstr), granularity)));
    }

    @BeforeEach
    public void setUp() throws Exception {
        testcases.add(TimeUtils.now());
        testcases.add(TimeUtils.convertFromISO8601String("2000-01-01T00:00:00.000Z"));
        testcases.add(TimeUtils.convertFromISO8601String("2000-02-01T00:00:00.000Z"));
        testcases.add(TimeUtils.convertFromISO8601String("2002-02-01T00:00:00.000Z"));
        testcases.add(TimeUtils.convertFromISO8601String("2003-12-21T08:33:55.003Z"));
        testcases.add(TimeUtils.convertFromISO8601String("2013-12-21T08:33:55.003Z"));
        testcases.add(TimeUtils.convertFromISO8601String("2004-02-29T08:33:55.003Z"));
        testcases.add(TimeUtils.convertFromISO8601String("2008-02-29T08:33:55.003Z"));
        testcases.add(TimeUtils.convertFromISO8601String("2012-02-29T08:33:55.003Z"));
        long startOfCurrentYearInSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
        for (int secondsintoYear = 0;
                secondsintoYear < PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 365;
                secondsintoYear += 323) {
            testcases.add(Instant.ofEpochMilli((startOfCurrentYearInSeconds + secondsintoYear) * 1000));
        }
        // 2008 was a leap year
        long startOf2008InSeconds = TimeUtils.getStartOfYearInSeconds(2008);
        for (int secondsintoYear = 0;
                secondsintoYear < PartitionGranularity.PARTITION_YEAR.getApproxSecondsPerChunk();
                secondsintoYear += 656) {
            testcases.add(Instant.ofEpochMilli((startOf2008InSeconds + secondsintoYear) * 1000));
        }
        // Create some sample times for the rest of the millenium.
        DecimalFormat df = new DecimalFormat("0000");
        for (int year = 1970; year < 2000; year++) {
            testcases.add(TimeUtils.convertFromISO8601String(df.format(year) + "-12-21T08:33:53.000Z"));
        }
    }

    @Test
    public void testConvertFromEpochSeconds() {
        for (Instant ts : testcases) {
            long epochSeconds = TimeUtils.convertToEpochSeconds(ts);
            int nanos = ts.getNano();
            Instant finalresult = TimeUtils.convertFromEpochSeconds(epochSeconds, nanos);
            Assertions.assertEquals(ts, finalresult);
        }
    }

    @Test
    public void convertFromEpochMillis() {
        for (Instant ts : testcases) {
            // We can't really test the millis portion, however this is adequate for now
            long epochMilliSeconds = TimeUtils.convertToEpochMillis(ts);
            Instant finalresult = TimeUtils.convertFromEpochMillis(epochMilliSeconds);
            Assertions.assertEquals(
                    0,
                    Duration.between(ts, finalresult)
                            .truncatedTo(ChronoUnit.MILLIS)
                            .compareTo(Duration.ofMillis(0)));
        }
    }

    @Test
    public void testConvertFromJCATimeStamp() {
        for (Instant ts : testcases) {
            long epochSeconds = TimeUtils.convertToEpochSeconds(ts);
            int nanos = ts.getNano();
            gov.aps.jca.dbr.TimeStamp jcats =
                    new gov.aps.jca.dbr.TimeStamp(epochSeconds - TimeUtils.EPICS_EPOCH_2_JAVA_EPOCH_OFFSET, nanos);
            Instant finalresult = TimeUtils.convertFromJCATimeStamp(jcats);
            Assertions.assertEquals(ts, finalresult);
        }
    }

    @Test
    public void testConvertFromYearSecondTimestamp() {
        for (Instant ts : testcases) {
            YearSecondTimestamp yts = TimeUtils.convertToYearSecondTimestamp(ts);
            Instant finalresult = TimeUtils.convertFromYearSecondTimestamp(yts);
            Assertions.assertEquals(ts, finalresult);
        }
    }

    @Test
    public void testbreakIntoYearlyTimeSpans() {
        Assertions.assertTrue(compareTimeSpans(
                TimeUtils.breakIntoYearlyTimeSpans(
                        TimeUtils.convertFromISO8601String("2011-02-01T08:00:00.000Z"),
                        TimeUtils.convertFromISO8601String("2011-02-02T08:00:00.000Z")),
                "2011-02-01T08:00:00.000Z" + "," + "2011-02-02T08:00:00.000Z"));

        Assertions.assertTrue(compareTimeSpans(
                TimeUtils.breakIntoYearlyTimeSpans(
                        TimeUtils.convertFromISO8601String("2000-01-01T00:00:00.000Z"),
                        TimeUtils.convertFromISO8601String("2000-01-01T00:00:00.000Z")),
                "2000-01-01T00:00:00.000Z" + "," + "2000-01-01T00:00:00.000Z"));
        Assertions.assertTrue(compareTimeSpans(
                TimeUtils.breakIntoYearlyTimeSpans(
                        TimeUtils.convertFromISO8601String("2001-02-01T00:00:00.000Z"),
                        TimeUtils.convertFromISO8601String("2002-02-01T00:00:00.000Z")),
                "2001-02-01T00:00:00.000Z" + "," + "2001-12-31T23:59:59.999Z" + "," + "2002-01-01T00:00:00.000Z" + ","
                        + "2002-02-01T00:00:00.000Z"));
        Assertions.assertTrue(compareTimeSpans(
                TimeUtils.breakIntoYearlyTimeSpans(
                        TimeUtils.convertFromISO8601String("2001-02-01T00:00:00.000Z"),
                        TimeUtils.convertFromISO8601String("2002-01-01T00:00:00.000Z")),
                "2001-02-01T00:00:00.000Z" + "," + "2001-12-31T23:59:59.999Z" + "," + "2002-01-01T00:00:00.000Z" + ","
                        + "2002-01-01T00:00:00.000Z"));
        Assertions.assertTrue(compareTimeSpans(
                TimeUtils.breakIntoYearlyTimeSpans(
                        TimeUtils.convertFromISO8601String("2001-12-31T23:59:59.998Z"),
                        TimeUtils.convertFromISO8601String("2002-01-01T00:00:00.000Z")),
                "2001-12-31T23:59:59.998Z" + "," + "2001-12-31T23:59:59.999Z" + "," + "2002-01-01T00:00:00.000Z" + ","
                        + "2002-01-01T00:00:00.000Z"));
        Assertions.assertTrue(compareTimeSpans(
                TimeUtils.breakIntoYearlyTimeSpans(
                        TimeUtils.convertFromISO8601String("2001-12-31T00:00:00.000Z"),
                        TimeUtils.convertFromISO8601String("2003-02-01T00:00:00.000Z")),
                "2001-12-31T00:00:00.000Z" + "," + "2001-12-31T23:59:59.999Z" + "," + "2002-01-01T00:00:00.000Z"
                        + "," + "2002-12-31T23:59:59.999Z" + "," + "2003-01-01T00:00:00.000Z"
                        + "," + "2003-02-01T00:00:00.000Z"));
        Assertions.assertTrue(compareTimeSpans(
                TimeUtils.breakIntoYearlyTimeSpans(
                        TimeUtils.convertFromISO8601String("2001-12-31T00:00:00.000Z"),
                        TimeUtils.convertFromISO8601String("2004-02-01T00:00:00.000Z")),
                "2001-12-31T00:00:00.000Z" + "," + "2001-12-31T23:59:59.999Z" + "," + "2002-01-01T00:00:00.000Z"
                        + "," + "2002-12-31T23:59:59.999Z" + "," + "2003-01-01T00:00:00.000Z"
                        + "," + "2003-12-31T23:59:59.999Z" + "," + "2004-01-01T00:00:00.000Z"
                        + "," + "2004-02-01T00:00:00.000Z"));
        Assertions.assertTrue(compareTimeSpans(
                TimeUtils.breakIntoYearlyTimeSpans(
                        TimeUtils.convertFromISO8601String("2010-06-27T08:00:00.000Z"),
                        TimeUtils.convertFromISO8601String("2020-06-27T08:00:00.000Z")),
                "2010-06-27T08:00:00.000Z" + "," + "2010-12-31T23:59:59.999Z" + "," + "2011-01-01T00:00:00.000Z"
                        + "," + "2011-12-31T23:59:59.999Z" + "," + "2012-01-01T00:00:00.000Z"
                        + "," + "2012-12-31T23:59:59.999Z" + "," + "2013-01-01T00:00:00.000Z"
                        + "," + "2013-12-31T23:59:59.999Z" + "," + "2014-01-01T00:00:00.000Z"
                        + "," + "2014-12-31T23:59:59.999Z" + "," + "2015-01-01T00:00:00.000Z"
                        + "," + "2015-12-31T23:59:59.999Z" + "," + "2016-01-01T00:00:00.000Z"
                        + "," + "2016-12-31T23:59:59.999Z" + "," + "2017-01-01T00:00:00.000Z"
                        + "," + "2017-12-31T23:59:59.999Z" + "," + "2018-01-01T00:00:00.000Z"
                        + "," + "2018-12-31T23:59:59.999Z" + "," + "2019-01-01T00:00:00.000Z"
                        + "," + "2019-12-31T23:59:59.999Z" + "," + "2020-01-01T00:00:00.000Z"
                        + "," + "2020-06-27T08:00:00.000Z"));
    }

    @Test
    public void testConvertJCAToFromYearSecond() {
        for (Instant ts : testcases) {
            long epochSeconds = TimeUtils.convertToEpochSeconds(ts);
            int nanos = ts.getNano();
            gov.aps.jca.dbr.TimeStamp jcats =
                    new gov.aps.jca.dbr.TimeStamp(epochSeconds - TimeUtils.EPICS_EPOCH_2_JAVA_EPOCH_OFFSET, nanos);
            YearSecondTimestamp yts = TimeUtils.convertToYearSecondTimestamp(jcats);
            Instant finalresult = TimeUtils.convertFromYearSecondTimestamp(yts);
            Assertions.assertEquals(ts, finalresult);
        }
    }

    @Test
    public void testConvertEpochSecondsToFromYearSecond() {
        for (Instant ts : testcases) {
            long epochSeconds = TimeUtils.convertToEpochSeconds(ts);
            int nanos = ts.getNano();
            YearSecondTimestamp yts = TimeUtils.convertToYearSecondTimestamp(epochSeconds);
            yts.setNanos(nanos);
            Instant finalresult = TimeUtils.convertFromYearSecondTimestamp(yts);
            Assertions.assertEquals(ts, finalresult);
        }
    }

    @Test
    public void testGetNextPartitionFirstSecond() {
        testNextEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_YEAR, "2011-01-01T00:00:00.000Z");
        testNextEquals("2010-02-12T12:23:59.000Z", PartitionGranularity.PARTITION_YEAR, "2011-01-01T00:00:00.000Z");
        testNextEquals("2010-12-31T23:59:59.999Z", PartitionGranularity.PARTITION_YEAR, "2011-01-01T00:00:00.000Z");
        testNextEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_MONTH, "2010-02-01T00:00:00.000Z");
        testNextEquals("2010-01-15T00:00:00.000Z", PartitionGranularity.PARTITION_MONTH, "2010-02-01T00:00:00.000Z");
        testNextEquals("2010-01-31T23:59:59.999Z", PartitionGranularity.PARTITION_MONTH, "2010-02-01T00:00:00.000Z");
        testNextEquals("2010-02-01T00:00:00.000Z", PartitionGranularity.PARTITION_MONTH, "2010-03-01T00:00:00.000Z");
        testNextEquals("2012-02-29T23:59:59.999Z", PartitionGranularity.PARTITION_MONTH, "2012-03-01T00:00:00.000Z");
        testNextEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_DAY, "2010-01-02T00:00:00.000Z");
        testNextEquals("2010-01-15T13:23:33.000Z", PartitionGranularity.PARTITION_DAY, "2010-01-16T00:00:00.000Z");
        testNextEquals("2012-02-29T23:59:59.999Z", PartitionGranularity.PARTITION_DAY, "2012-03-01T00:00:00.000Z");
        testNextEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_HOUR, "2010-01-01T01:00:00.000Z");
        testNextEquals("2010-01-01T23:59:59.999Z", PartitionGranularity.PARTITION_HOUR, "2010-01-02T00:00:00.000Z");
        testNextEquals("2011-02-28T23:59:59.999Z", PartitionGranularity.PARTITION_HOUR, "2011-03-01T00:00:00.000Z");
        testNextEquals("2012-02-29T23:59:59.999Z", PartitionGranularity.PARTITION_HOUR, "2012-03-01T00:00:00.000Z");
        testNextEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_5MIN, "2010-01-01T00:05:00.000Z");
        testNextEquals("2010-03-17T22:17:44.000Z", PartitionGranularity.PARTITION_5MIN, "2010-03-17T22:20:00.000Z");
        testNextEquals("2010-03-17T22:56:44.000Z", PartitionGranularity.PARTITION_5MIN, "2010-03-17T23:00:00.000Z");
        testNextEquals("2010-01-01T23:59:59.999Z", PartitionGranularity.PARTITION_5MIN, "2010-01-02T00:00:00.000Z");
        testNextEquals("2011-02-28T23:59:59.999Z", PartitionGranularity.PARTITION_5MIN, "2011-03-01T00:00:00.000Z");
        testNextEquals("2012-02-29T23:59:59.999Z", PartitionGranularity.PARTITION_5MIN, "2012-03-01T00:00:00.000Z");
        testNextEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_15MIN, "2010-01-01T00:15:00.000Z");
        testNextEquals("2010-03-17T22:17:44.000Z", PartitionGranularity.PARTITION_15MIN, "2010-03-17T22:30:00.000Z");
        testNextEquals("2010-03-17T22:56:44.000Z", PartitionGranularity.PARTITION_15MIN, "2010-03-17T23:00:00.000Z");
        testNextEquals("2010-01-01T23:59:59.999Z", PartitionGranularity.PARTITION_15MIN, "2010-01-02T00:00:00.000Z");
        testNextEquals("2011-02-28T23:59:59.999Z", PartitionGranularity.PARTITION_15MIN, "2011-03-01T00:00:00.000Z");
        testNextEquals("2012-02-29T23:59:59.999Z", PartitionGranularity.PARTITION_15MIN, "2012-03-01T00:00:00.000Z");
        testNextEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_30MIN, "2010-01-01T00:30:00.000Z");
        testNextEquals("2010-03-17T22:17:44.000Z", PartitionGranularity.PARTITION_30MIN, "2010-03-17T22:30:00.000Z");
        testNextEquals("2010-03-17T22:56:44.000Z", PartitionGranularity.PARTITION_30MIN, "2010-03-17T23:00:00.000Z");
        testNextEquals("2010-01-01T23:59:59.999Z", PartitionGranularity.PARTITION_30MIN, "2010-01-02T00:00:00.000Z");
        testNextEquals("2011-02-28T23:59:59.999Z", PartitionGranularity.PARTITION_30MIN, "2011-03-01T00:00:00.000Z");
        testNextEquals("2012-02-29T23:59:59.999Z", PartitionGranularity.PARTITION_30MIN, "2012-03-01T00:00:00.000Z");
    }

    @Test
    public void testgetPartitionName() {
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2011-01-01T00:00:00.000Z"),
                        PartitionGranularity.PARTITION_YEAR),
                "2011");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2012-02-29T12:24:26.000Z"),
                        PartitionGranularity.PARTITION_YEAR),
                "2012");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2022-12-31T21:22:59.000Z"),
                        PartitionGranularity.PARTITION_YEAR),
                "2022");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2011-01-01T00:00:00.000Z"),
                        PartitionGranularity.PARTITION_MONTH),
                "2011_01");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2012-02-29T12:24:26.000Z"),
                        PartitionGranularity.PARTITION_MONTH),
                "2012_02");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2013-03-31T23:59:59.000Z"),
                        PartitionGranularity.PARTITION_MONTH),
                "2013_03");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2022-12-31T21:22:59.000Z"),
                        PartitionGranularity.PARTITION_MONTH),
                "2022_12");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2011-01-01T00:00:00.000Z"),
                        PartitionGranularity.PARTITION_DAY),
                "2011_01_01");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2012-02-29T12:24:26.000Z"),
                        PartitionGranularity.PARTITION_DAY),
                "2012_02_29");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2013-03-31T23:59:59.000Z"),
                        PartitionGranularity.PARTITION_DAY),
                "2013_03_31");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2022-12-31T21:22:59.000Z"),
                        PartitionGranularity.PARTITION_DAY),
                "2022_12_31");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2011-01-01T00:00:00.000Z"),
                        PartitionGranularity.PARTITION_HOUR),
                "2011_01_01_00");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2012-02-29T12:24:26.000Z"),
                        PartitionGranularity.PARTITION_HOUR),
                "2012_02_29_12");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2013-03-31T23:59:59.000Z"),
                        PartitionGranularity.PARTITION_HOUR),
                "2013_03_31_23");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2022-12-31T21:22:59.000Z"),
                        PartitionGranularity.PARTITION_HOUR),
                "2022_12_31_21");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2011-01-01T00:00:00.000Z"),
                        PartitionGranularity.PARTITION_5MIN),
                "2011_01_01_00_00");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2012-02-29T12:24:26.000Z"),
                        PartitionGranularity.PARTITION_5MIN),
                "2012_02_29_12_20");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2013-03-31T23:59:59.000Z"),
                        PartitionGranularity.PARTITION_5MIN),
                "2013_03_31_23_55");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2022-12-31T21:22:59.000Z"),
                        PartitionGranularity.PARTITION_5MIN),
                "2022_12_31_21_20");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2011-01-01T00:00:00.000Z"),
                        PartitionGranularity.PARTITION_15MIN),
                "2011_01_01_00_00");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2012-02-29T12:24:26.000Z"),
                        PartitionGranularity.PARTITION_15MIN),
                "2012_02_29_12_15");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2013-03-31T23:59:59.000Z"),
                        PartitionGranularity.PARTITION_15MIN),
                "2013_03_31_23_45");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2022-12-31T21:22:59.000Z"),
                        PartitionGranularity.PARTITION_15MIN),
                "2022_12_31_21_15");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2011-01-01T00:00:00.000Z"),
                        PartitionGranularity.PARTITION_30MIN),
                "2011_01_01_00_00");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2012-02-29T12:24:26.000Z"),
                        PartitionGranularity.PARTITION_30MIN),
                "2012_02_29_12_00");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2013-03-31T23:59:59.000Z"),
                        PartitionGranularity.PARTITION_30MIN),
                "2013_03_31_23_30");
        Assertions.assertEquals(
                TimeUtils.getPartitionName(
                        TimeUtils.convertFromISO8601String("2022-12-31T21:22:59.000Z"),
                        PartitionGranularity.PARTITION_30MIN),
                "2022_12_31_21_00");
    }

    @Test
    public void testPreviousPartitionLastSecond() {
        testPrevEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_YEAR, "2009-12-31T23:59:59.000Z");
        testPrevEquals("2010-06-30T12:23:43.000Z", PartitionGranularity.PARTITION_YEAR, "2009-12-31T23:59:59.000Z");
        testPrevEquals("2010-12-31T23:59:59.000Z", PartitionGranularity.PARTITION_YEAR, "2009-12-31T23:59:59.000Z");
        testPrevEquals("2012-02-29T23:59:59.999Z", PartitionGranularity.PARTITION_YEAR, "2011-12-31T23:59:59.000Z");
        testPrevEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_MONTH, "2009-12-31T23:59:59.000Z");
        testPrevEquals("2010-02-01T00:00:00.000Z", PartitionGranularity.PARTITION_MONTH, "2010-01-31T23:59:59.000Z");
        testPrevEquals("2010-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_MONTH, "2010-02-28T23:59:59.000Z");
        testPrevEquals("2012-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_MONTH, "2012-02-29T23:59:59.000Z");
        testPrevEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_DAY, "2009-12-31T23:59:59.000Z");
        testPrevEquals("2010-02-01T09:10:11.000Z", PartitionGranularity.PARTITION_DAY, "2010-01-31T23:59:59.000Z");
        testPrevEquals("2010-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_DAY, "2010-02-28T23:59:59.000Z");
        testPrevEquals("2012-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_DAY, "2012-02-29T23:59:59.000Z");
        testPrevEquals("2010-01-01T01:00:00.000Z", PartitionGranularity.PARTITION_HOUR, "2010-01-01T00:59:59.000Z");
        testPrevEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_HOUR, "2009-12-31T23:59:59.000Z");
        testPrevEquals("2010-02-01T00:00:00.000Z", PartitionGranularity.PARTITION_HOUR, "2010-01-31T23:59:59.000Z");
        testPrevEquals("2010-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_HOUR, "2010-02-28T23:59:59.000Z");
        testPrevEquals("2012-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_HOUR, "2012-02-29T23:59:59.000Z");
        testPrevEquals("2010-10-12T18:45:45.000Z", PartitionGranularity.PARTITION_5MIN, "2010-10-12T18:44:59.000Z");
        testPrevEquals("2010-10-12T18:44:45.000Z", PartitionGranularity.PARTITION_5MIN, "2010-10-12T18:39:59.000Z");
        testPrevEquals("2010-01-01T01:00:00.000Z", PartitionGranularity.PARTITION_5MIN, "2010-01-01T00:59:59.000Z");
        testPrevEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_5MIN, "2009-12-31T23:59:59.000Z");
        testPrevEquals("2010-02-01T00:00:00.000Z", PartitionGranularity.PARTITION_5MIN, "2010-01-31T23:59:59.000Z");
        testPrevEquals("2010-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_5MIN, "2010-02-28T23:59:59.000Z");
        testPrevEquals("2012-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_5MIN, "2012-02-29T23:59:59.000Z");
        testPrevEquals("2010-10-12T18:45:45.000Z", PartitionGranularity.PARTITION_15MIN, "2010-10-12T18:44:59.000Z");
        testPrevEquals("2010-10-12T18:44:45.000Z", PartitionGranularity.PARTITION_15MIN, "2010-10-12T18:29:59.000Z");
        testPrevEquals("2010-01-01T01:00:00.000Z", PartitionGranularity.PARTITION_15MIN, "2010-01-01T00:59:59.000Z");
        testPrevEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_15MIN, "2009-12-31T23:59:59.000Z");
        testPrevEquals("2010-02-01T00:00:00.000Z", PartitionGranularity.PARTITION_15MIN, "2010-01-31T23:59:59.000Z");
        testPrevEquals("2010-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_15MIN, "2010-02-28T23:59:59.000Z");
        testPrevEquals("2012-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_15MIN, "2012-02-29T23:59:59.000Z");
        testPrevEquals("2010-10-12T18:45:45.000Z", PartitionGranularity.PARTITION_30MIN, "2010-10-12T18:29:59.000Z");
        testPrevEquals("2010-10-12T18:44:45.000Z", PartitionGranularity.PARTITION_30MIN, "2010-10-12T18:29:59.000Z");
        testPrevEquals("2010-01-01T01:00:00.000Z", PartitionGranularity.PARTITION_30MIN, "2010-01-01T00:59:59.000Z");
        testPrevEquals("2010-01-01T00:00:00.000Z", PartitionGranularity.PARTITION_30MIN, "2009-12-31T23:59:59.000Z");
        testPrevEquals("2010-02-01T00:00:00.000Z", PartitionGranularity.PARTITION_30MIN, "2010-01-31T23:59:59.000Z");
        testPrevEquals("2010-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_30MIN, "2010-02-28T23:59:59.000Z");
        testPrevEquals("2012-03-01T00:00:00.000Z", PartitionGranularity.PARTITION_30MIN, "2012-02-29T23:59:59.000Z");
    }

    @Test
    public void testconvertToTenthsOfASecond() throws Exception {
        long currentEpochSeconds = TimeUtils.getCurrentEpochSeconds();
        Assertions.assertEquals(TimeUtils.convertToTenthsOfASecond(currentEpochSeconds, 0), currentEpochSeconds * 10);
        Assertions.assertEquals(
                TimeUtils.convertToTenthsOfASecond(currentEpochSeconds, 100000000), currentEpochSeconds * 10 + 1);
        Assertions.assertEquals(
                TimeUtils.convertToTenthsOfASecond(currentEpochSeconds, 2 * (100000000)), currentEpochSeconds * 10 + 2);
        Assertions.assertEquals(
                TimeUtils.convertToTenthsOfASecond(currentEpochSeconds, 9 * (100000000)), currentEpochSeconds * 10 + 9);
        try {
            TimeUtils.convertToTenthsOfASecond(currentEpochSeconds, 1999999999);
            Assertions.fail("We should have been thrown an exception here");
        } catch (NumberFormatException ex) {
        }

        for (int i = 0; i < 100000; i++) {
            Instant ts = TimeUtils.now();
            TimeUtils.convertToTenthsOfASecond(ts.toEpochMilli() / 1000, ts.getNano());
        }
    }

    /**
     * Test the method that breaks down a start time and end time into interval sizes
     * @throws Exception
     */
    @Test
    public void testStartEndIntervalBreakDown() throws Exception {
        testStartEndIntervalBreakDown(
                TimeUtils.convertFromISO8601String("2011-02-01T08:00:00.000Z"),
                TimeUtils.convertFromISO8601String("2012-02-01T08:00:00.000Z"),
                PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 30,
                13);
        testStartEndIntervalBreakDown(
                TimeUtils.convertFromISO8601String("2011-02-25T00:00:00.000Z"),
                TimeUtils.convertFromISO8601String("2012-02-01T08:00:00.000Z"),
                PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 30,
                12);
        testStartEndIntervalBreakDown(
                TimeUtils.convertFromISO8601String("2011-02-01T08:00:00.000Z"),
                TimeUtils.convertFromISO8601String("2012-01-21T00:00:00.000Z"),
                PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 30,
                12);
    }

    private void testStartEndIntervalBreakDown(Instant start, Instant end, long binInterval, int expectedBins)
            throws Exception {
        logger.debug("Testing interval breakdown for retrieval");
        List<TimeSpan> intervals = TimeUtils.breakIntoIntervals(start, end, binInterval);
        Assertions.assertEquals(
                intervals.size(), expectedBins, "Expecting " + expectedBins + " bins, got " + intervals.size());
        Instant previousEnd = null;
        for (int i = 0; i < intervals.size(); i++) {
            TimeSpan currentInterval = intervals.get(i);
            Assertions.assertTrue(
                    currentInterval.getEndTime().isAfter(currentInterval.getStartTime()),
                    "Expecting end greater than start"
                            + TimeUtils.convertToHumanReadableString(currentInterval.getStartTime())
                            + " - " + TimeUtils.convertToHumanReadableString(currentInterval.getEndTime()));

            logger.debug(TimeUtils.convertToISO8601String(currentInterval.getStartTime()) + " - "
                    + TimeUtils.convertToISO8601String(currentInterval.getEndTime()));
            if (i == 0) {
                Assertions.assertEquals(currentInterval.getStartTime(), start, "Expecting start at the beginning " + i);
            }
            if (i == intervals.size() - 1) {
                Assertions.assertEquals(currentInterval.getEndTime(), end, "Expecting end at the end " + i);
            }
            if (previousEnd != null) {
                Assertions.assertTrue(
                        (TimeUtils.convertToEpochSeconds(currentInterval.getStartTime())
                                        - TimeUtils.convertToEpochSeconds(previousEnd)
                                == 1),
                        "Expecting at most a one second difference between "
                                + TimeUtils.convertToHumanReadableString(previousEnd)
                                + " - " + TimeUtils.convertToHumanReadableString(currentInterval.getStartTime())
                                + " = "
                                + (TimeUtils.convertToEpochSeconds(currentInterval.getStartTime())
                                        - TimeUtils.convertToEpochSeconds(previousEnd)));
                Assertions.assertTrue(
                        TimeUtils.convertToEpochSeconds(currentInterval.getStartTime())
                                > TimeUtils.convertToEpochSeconds(previousEnd),
                        "Expecting non overlapping spans " + TimeUtils.convertToHumanReadableString(previousEnd) + " / "
                                + TimeUtils.convertToHumanReadableString(currentInterval.getStartTime()));
            }
            previousEnd = currentInterval.getEndTime();
        }
    }
}
