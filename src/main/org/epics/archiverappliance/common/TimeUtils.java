/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVALong;
import org.epics.pva.data.PVAStructure;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * There are various versions of timestamps in the archiver appliance.
 * The most commonly used one is Instant and is the one exposed in the APIs.
 * The other versions are follows
 * <ol>
 * <li>A long epoch seconds + optional nanos - The seconds part is the java epoch seconds, as returned by System.currentMillis()/1000</li>
 * <li>A long epoch milliseconds - This is the java epoch milliseconds, as returned by System.currentMillis()</li>
 * <li>A JCA timestamp - This is what comes out of JCA</li>
 * <li>A year/secondsintoyear/nanos combination - This is what is used in the protocol buffer storage plugin.</li>
 * <li>A ISO 8601 date time - We use JODA to convert from/to a ISO 8601 string</li>
 * </ol>
 *
 * This class contains utilities to convert various forms of timestamps to/from the Instant.
 * @author mshankar
 *
 */
public class TimeUtils {
    /**
     * EPICS epoch starts at January 1, 1990 UTC. This constant contains the offset that must be added to
     * epicstimestamps to generate java timestamps.
     */
    public static final long EPICS_EPOCH_2_JAVA_EPOCH_OFFSET = 631152000L;

    private static final String ISO_DATE_MILLIS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private static final short START_OF_CACHE_FOR_YEAR_STARTEPOCHSECONDS = 1970;
    private static final String[] TWO_DIGIT_EXPANSIONS = {"00", "01", "02", "03", "04", "05", "06", "07", "08", "09"};
    public static Logger logger = LogManager.getLogger(TimeUtils.class);
    // We cache the start of the year for about 16000 years..
    static long[] startOfYearInEpochSeconds = new long[16 * 1024];

    static {
        for (short i = 0; i < startOfYearInEpochSeconds.length; i++) {
            ZonedDateTime startoftheYear = ZonedDateTime.of(
                    START_OF_CACHE_FOR_YEAR_STARTEPOCHSECONDS + i, 1, 1, 0, 0, 0, 0, ZoneId.from(ZoneOffset.UTC));
            startOfYearInEpochSeconds[i] = startoftheYear.toEpochSecond();
        }
    }

    public static Instant convertFromEpochSeconds(long epochSeconds, int nanos) {
        return Instant.ofEpochSecond(epochSeconds, nanos);
    }

    public static Instant convertFromEpochMillis(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis);
    }

    public static Instant convertFromJCATimeStamp(gov.aps.jca.dbr.TimeStamp jcats) {
        return Instant.ofEpochSecond((jcats.secPastEpoch() + EPICS_EPOCH_2_JAVA_EPOCH_OFFSET), jcats.nsec());
    }

    public static Instant convertFromYearSecondTimestamp(YearSecondTimestamp ysts) {

        return Instant.ofEpochSecond(
                (TimeUtils.getStartOfYearInSeconds(ysts.getYear()) + ysts.getSecondsintoyear()), ysts.getNano());
    }

    public static Instant convertFromISO8601String(String tsstr) {
        // Sample ISO8601 string 2011-02-01T08:00:00.000Z
        return Instant.parse(tsstr);
    }

    public static Instant convertFromDateTimeStringWithOffset(String tsstr) {
        // Sample string 2012-11-03T00:00:00-07:00
        DateTimeFormatter fmt = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        OffsetDateTime dt = OffsetDateTime.parse(tsstr, fmt);
        return dt.toInstant();
    }

    public static long convertToEpochSeconds(Instant ts) {
        return ts.getEpochSecond();
    }

    public static long convertToEpochMillis(Instant ts) {
        return ts.toEpochMilli();
    }

    public static YearSecondTimestamp convertToYearSecondTimestamp(Instant ts) {
        Instant startOfYear = getStartOfYear(getYear(ts));
        Duration duration = Duration.between(startOfYear, ts);
        assert (duration.getSeconds() < Integer.MAX_VALUE);
        int secondsIntoYear = (int) duration.getSeconds();
        return new YearSecondTimestamp(
                (short) ts.atZone(ZoneId.from(ZoneOffset.UTC)).getYear(), secondsIntoYear, ts.getNano());
    }

    public static YearSecondTimestamp convertToYearSecondTimestamp(gov.aps.jca.dbr.TimeStamp jcats) {
        return convertToYearSecondTimestamp(convertFromJCATimeStamp(jcats));
    }

    public static YearSecondTimestamp convertToYearSecondTimestamp(long epochSeconds) {
        return convertToYearSecondTimestamp(Instant.ofEpochSecond(epochSeconds));
    }

    public static String convertToISO8601String(Instant ts) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(ISO_DATE_MILLIS_FORMAT);
        var truncTs = ts.truncatedTo(ChronoUnit.MILLIS).atZone(ZoneId.from(ZoneOffset.UTC));
        return formatter.format(truncTs);
    }

    public static String convertToISO8601String(long epochSeconds) {
        Instant ts = Instant.ofEpochSecond(epochSeconds);
        return convertToISO8601String(ts);
    }

    public static String convertToHumanReadableString(Instant ts) {
        if (ts == null || ts.getEpochSecond() == 0) return "Never";
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("MMM/dd/yyyy HH:mm:ss z");
        return ZonedDateTime.ofInstant(ts, ZoneId.systemDefault()).format(fmt);
    }

    public static String convertToHumanReadableString(long epochSeconds) {
        return convertToHumanReadableString(Instant.ofEpochSecond(epochSeconds));
    }

    public static long convertToLocalEpochMillis(long epochMillis) {
        if (epochMillis == 0) return 0;
        return epochMillis
                + ZoneId.systemDefault()
                                .getRules()
                                .getOffset(Instant.ofEpochMilli(epochMillis))
                                .getTotalSeconds()
                        * 1000L;
    }

    public static long getStartOfCurrentYearInSeconds() {
        return getStartOfYearInSeconds(
                ZonedDateTime.now(ZoneId.from(ZoneOffset.UTC)).getYear());
    }

    public static long getStartOfYearInSeconds(int year) {
        return getStartOfYear(year).getEpochSecond();
    }

    public static Instant getStartOfYear(int year) {
        ZonedDateTime startOfYear = ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, ZoneId.from(ZoneOffset.UTC));
        return startOfYear.toInstant();
    }

    public static short getYear(Instant instant) {
        return (short)
                ZonedDateTime.ofInstant(instant, ZoneId.from(ZoneOffset.UTC)).getYear();
    }

    public static long getStartOfYearInSeconds(long epochseconds) {
        // The JODA DateTime constructor takes millis
        return getStartOfYearInSeconds(getYear(Instant.ofEpochSecond(epochseconds)));
    }

    /**
     * In the protocol buffer storage plugin, we send the year as a short
     *
     * @param year Year
     * @return startOfYearInEpochSeconds  &emsp;
     */
    public static long getStartOfYearInSeconds(short year) {
        return startOfYearInEpochSeconds[year - START_OF_CACHE_FOR_YEAR_STARTEPOCHSECONDS];
    }

    public static Instant getEndOfYear(int year) {
        ZonedDateTime endoftheYear = ZonedDateTime.of(year, 12, 31, 23, 59, 59, 999999999, ZoneId.from(ZoneOffset.UTC));
        return endoftheYear.toInstant();
    }

    /**
     * Convert Java EPOCH seconds to a seconds into year
     *
     * @param instant &emsp;
     * @return SecondsIntoYear The difference in Seconds
     */
    public static int getSecondsIntoYear(Instant instant) {
        long epochseconds = instant.getEpochSecond();
        return getSecondsIntoYear(epochseconds);
    }

    /**
     * Convert Java EPOCH seconds to a seconds into year
     *
     * @param epochseconds &emsp;
     * @return SecondsIntoYear The difference in Seconds
     */
    public static int getSecondsIntoYear(long epochseconds) {
        long startOfYearInSeconds = getStartOfYearInSeconds(epochseconds);
        long diffInSecs = epochseconds - startOfYearInSeconds;
        assert (diffInSecs <= Integer.MAX_VALUE);
        return (int) (diffInSecs);
    }

    /**
     * Determine year from java epoch seconds.
     *
     * @param epochseconds &emsp;
     * @return YearForEpochSeconds &emsp;
     */
    public static short computeYearForEpochSeconds(long epochseconds) {
        // The JODA DateTime constructor takes millis
        return getYear(Instant.ofEpochSecond(epochseconds));
    }

    /**
     * Get the current year in the UTC timezone
     *
     * @return CurrentYear &emsp;
     */
    public static short getCurrentYear() {
        return getYear(Instant.now());
    }

    /**
     * Gets the current epoch seconds in the UTC timezone
     *
     * @return currentEpochSeconds &emsp;
     */
    public static long getCurrentEpochSeconds() {
        return Instant.now().getEpochSecond();
    }

    /**
     * Gets the current epoch milli seconds in the UTC timezone
     *
     * @return currentEpochMilliSeconds &emsp;
     */
    public static long getCurrentEpochMilliSeconds() {
        return Instant.now().toEpochMilli();
    }

    /**
     * Gets "now" as a Instant in the UTC timezone
     *
     * @return now A Instant in the UTC timezone
     */
    public static Instant now() {
        return Instant.now();
    }

    public static java.sql.Timestamp toSQLTimeStamp(Instant instant) {
        java.sql.Timestamp ts = new java.sql.Timestamp(instant.toEpochMilli());
        ts.setNanos(instant.getNano());
        return ts;
    }

    public static Instant fromSQLTimeStamp(java.sql.Timestamp ts) {
        return Instant.ofEpochSecond(ts.getTime() / 1000, ts.getNanos());
    }

    public static Instant plusHours(Instant ts, int hours) {

        return ts.plus(Duration.ofHours(hours));
    }

    public static Instant minusHours(Instant ts, int hours) {
        return ts.minus(Duration.ofHours(hours));
    }

    public static Instant plusDays(Instant ts, int days) {
        return ts.plus(Duration.ofDays(days));
    }

    public static Instant minusDays(Instant ts, int days) {
        return ts.minus(Duration.ofDays(days));
    }

    /**
     * Given a start time and an end time, this method breaks this span into a sequence of spans each of which fits
     * within a year. Used where data is partitioned by year....
     *
     * @param start The start time
     * @param end   The end time
     * @return breakIntoYearlyTimeSpans   &emsp;
     */
    public static List<TimeSpan> breakIntoYearlyTimeSpans(Instant start, Instant end) {
        assert (start.equals(end) || start.isBefore(end));
        YearSecondTimestamp startYTS = convertToYearSecondTimestamp(start);
        YearSecondTimestamp endYTS = convertToYearSecondTimestamp(end);
        LinkedList<TimeSpan> ret = new LinkedList<TimeSpan>();
        if (startYTS.getYear() == endYTS.getYear()) {
            ret.add(new TimeSpan(start, end));
            return ret;
        } else {
            ret.add(new TimeSpan(start, getEndOfYear(startYTS.getYear())));
            for (int year = startYTS.getYear() + 1; year < endYTS.getYear(); year++) {
                ret.add(new TimeSpan(getStartOfYear(year), getEndOfYear(year)));
            }
            ret.add(new TimeSpan(getStartOfYear(endYTS.getYear()), end));
            return ret;
        }
    }

    /**
     * Returns a partition name for the given epoch second based on the partition granularity.
     *
     * @param ts          &emsp;
     * @param granularity Partition granularity of the file.
     * @return PartitionName &emsp;
     */
    public static String getPartitionName(Instant ts, PartitionGranularity granularity) {
        ZonedDateTime dateTime = ts.atZone(ZoneId.from(ZoneOffset.UTC));
        switch (granularity) {
            case PARTITION_YEAR -> {
                return String.valueOf(dateTime.getYear());
            }
            case PARTITION_MONTH -> {
                return dateTime.getYear() + "_"
                        + (dateTime.getMonth().getValue() < 10
                                ? TWO_DIGIT_EXPANSIONS[dateTime.getMonth().getValue()]
                                : dateTime.getMonth().getValue());
            }
            case PARTITION_DAY -> {
                return dateTime.getYear()
                        + "_"
                        + (dateTime.getMonth().getValue() < 10
                                ? TWO_DIGIT_EXPANSIONS[dateTime.getMonth().getValue()]
                                : dateTime.getMonth().getValue())
                        + "_"
                        + (dateTime.getDayOfMonth() < 10
                                ? TWO_DIGIT_EXPANSIONS[dateTime.getDayOfMonth()]
                                : dateTime.getDayOfMonth());
            }
            case PARTITION_HOUR -> {
                return dateTime.getYear()
                        + "_"
                        + (dateTime.getMonth().getValue() < 10
                                ? TWO_DIGIT_EXPANSIONS[dateTime.getMonth().getValue()]
                                : dateTime.getMonth().getValue())
                        + "_"
                        + (dateTime.getDayOfMonth() < 10
                                ? TWO_DIGIT_EXPANSIONS[dateTime.getDayOfMonth()]
                                : dateTime.getDayOfMonth())
                        + "_"
                        + (dateTime.getHour() < 10 ? TWO_DIGIT_EXPANSIONS[dateTime.getHour()] : dateTime.getHour());
            }
            case PARTITION_5MIN, PARTITION_15MIN, PARTITION_30MIN -> {
                int approxMinutesPerChunk = granularity.getApproxMinutesPerChunk();
                int startOfPartition_Min = (dateTime.getMinute() / approxMinutesPerChunk) * approxMinutesPerChunk;
                return dateTime.getYear()
                        + "_"
                        + (dateTime.getMonth().getValue() < 10
                                ? TWO_DIGIT_EXPANSIONS[dateTime.getMonth().getValue()]
                                : dateTime.getMonth().getValue())
                        + "_"
                        + (dateTime.getDayOfMonth() < 10
                                ? TWO_DIGIT_EXPANSIONS[dateTime.getDayOfMonth()]
                                : dateTime.getDayOfMonth())
                        + "_"
                        + (dateTime.getHour() < 10 ? TWO_DIGIT_EXPANSIONS[dateTime.getHour()] : dateTime.getHour())
                        + "_"
                        + (startOfPartition_Min < 10
                                ? TWO_DIGIT_EXPANSIONS[startOfPartition_Min]
                                : startOfPartition_Min);
            }
            default -> throw new UnsupportedOperationException("Invalid Partition type " + granularity);
        }
    }

    /**
     * Given an epoch seconds and a granularity, this method gives you the first second in the next partition as epoch
     * seconds.
     *
     * @param ts          &emsp;
     * @param granularity Partition granularity of the file.
     * @return NextPartitionFirstSecond &emsp;
     */
    public static Instant getNextPartitionFirstSecond(Instant ts, PartitionGranularity granularity) {
        ZonedDateTime dateTime = ts.atZone(ZoneId.from(ZoneOffset.UTC));
        ZonedDateTime nextPartitionFirstSecond = null;
        switch (granularity) {
            case PARTITION_YEAR -> {
                nextPartitionFirstSecond = dateTime.plusYears(1)
                        .withMonth(1)
                        .withDayOfMonth(1)
                        .withHour(0)
                        .withMinute(0)
                        .withSecond(0)
                        .withNano(0);
                return nextPartitionFirstSecond.toInstant();
            }
            case PARTITION_MONTH -> {
                nextPartitionFirstSecond = dateTime.plusMonths(1)
                        .withDayOfMonth(1)
                        .withHour(0)
                        .withMinute(0)
                        .withSecond(0)
                        .withNano(0);
                return nextPartitionFirstSecond.toInstant();
            }
            case PARTITION_DAY -> {
                nextPartitionFirstSecond = dateTime.plusDays(1)
                        .withHour(0)
                        .withMinute(0)
                        .withSecond(0)
                        .withNano(0);
                return nextPartitionFirstSecond.toInstant();
            }
            case PARTITION_HOUR -> {
                nextPartitionFirstSecond =
                        dateTime.plusHours(1).withMinute(0).withSecond(0).withNano(0);
                return nextPartitionFirstSecond.toInstant();
            }
            case PARTITION_5MIN, PARTITION_15MIN, PARTITION_30MIN -> {
                int approxMinutesPerChunk = granularity.getApproxMinutesPerChunk();
                ZonedDateTime nextPartForMin = dateTime.plusMinutes(approxMinutesPerChunk);
                int startOfPartitionForMin =
                        (nextPartForMin.getMinute() / approxMinutesPerChunk) * approxMinutesPerChunk;
                nextPartitionFirstSecond = nextPartForMin
                        .withMinute(startOfPartitionForMin)
                        .withSecond(0)
                        .withNano(0);
                return nextPartitionFirstSecond.toInstant();
            }
            default -> throw new UnsupportedOperationException("Invalid Partition type " + granularity);
        }
    }

    /**
     * Given an epoch seconds and a granularity, this method gives you the last second in the previous partition as
     * epoch seconds.
     *
     * @param ts          &emsp;
     * @param granularity Partition granularity of the file.
     * @return PreviousPartitionLastSecond  &emsp;
     */
    public static Instant getPreviousPartitionLastSecond(Instant ts, PartitionGranularity granularity) {
        ZonedDateTime dateTime = ts.atZone(ZoneId.from(ZoneOffset.UTC));
        ZonedDateTime previousPartitionLastSecond = null;
        switch (granularity) {
            case PARTITION_YEAR -> {
                previousPartitionLastSecond = dateTime.minusYears(1)
                        .withMonth(12)
                        .withDayOfMonth(31)
                        .withHour(23)
                        .withMinute(59)
                        .withSecond(59)
                        .withNano(0);
                return previousPartitionLastSecond.toInstant();
            }
            case PARTITION_MONTH -> {
                previousPartitionLastSecond = dateTime.withDayOfMonth(1)
                        .minusDays(1)
                        .withHour(23)
                        .withMinute(59)
                        .withSecond(59)
                        .withNano(0);
                return previousPartitionLastSecond.toInstant();
            }
            case PARTITION_DAY -> {
                previousPartitionLastSecond = dateTime.minusDays(1)
                        .withHour(23)
                        .withMinute(59)
                        .withSecond(59)
                        .withNano(0);
                return previousPartitionLastSecond.toInstant();
            }
            case PARTITION_HOUR -> {
                previousPartitionLastSecond =
                        dateTime.minusHours(1).withMinute(59).withSecond(59).withNano(0);
                return previousPartitionLastSecond.toInstant();
            }
            case PARTITION_5MIN, PARTITION_15MIN, PARTITION_30MIN -> {
                int approxMinutesPerChunk = granularity.getApproxMinutesPerChunk();
                int startOfPartition_Min = (dateTime.getMinute() / approxMinutesPerChunk) * approxMinutesPerChunk;
                previousPartitionLastSecond =
                        dateTime.withMinute(startOfPartition_Min).withSecond(0).minusSeconds(1);
                return previousPartitionLastSecond.toInstant();
            }
            default -> throw new UnsupportedOperationException("Invalid Partition type " + granularity);
        }
    }

    /**
     * Event rate rate limiting uses a tenths of a seconds units to cater to monitor intervals of 0.1 seconds, 0.5
     * seconds etc.. This converts a epochSeconds+nanos to time in terms of tenths of a second.
     *
     * @param epochSeconds &emsp;
     * @param nanos        &emsp;
     * @return TenthsOfASecond  &emsp;
     * @throws NumberFormatException &emsp;
     */
    public static long convertToTenthsOfASecond(long epochSeconds, int nanos) throws NumberFormatException {
        int tenthsPieceOfNanos = (nanos / (100000000));
        if (tenthsPieceOfNanos > 9) {
            throw new NumberFormatException(
                    "Tenths of nanos cannot be greater than 9 but this is " + tenthsPieceOfNanos);
        }
        return epochSeconds * 10 + tenthsPieceOfNanos;
    }

    /**
     * Whether we are in DST for a particular time in the servers default timezone. Mostly used by Matlab.
     *
     * @param ts Instant
     * @return boolean True or False
     */
    public static boolean isDST(Instant ts) {
        return ZoneId.systemDefault().getRules().isDaylightSavings(ts);
    }

    /**
     * Convert the timeStamp from a pvAccess normative type to YearSecondTimestamp
     * @param timeStampPVStructure
     * @return Timestamp
     */
    public static YearSecondTimestamp convertFromPVTimeStamp(PVAStructure timeStampPVStructure) {
        long secondsPastEpoch = ((PVALong) timeStampPVStructure.get("secondsPastEpoch")).get();
        int nanoSeconds = ((PVAInt) timeStampPVStructure.get("nanoseconds")).get();
        Instant timestamp = TimeUtils.convertFromEpochSeconds(secondsPastEpoch, nanoSeconds);
        return TimeUtils.convertToYearSecondTimestamp(timestamp);
    }

    /**
     * Break a time span into smaller time spans according to binSize The first time span has the start time and the end
     * of the first bin. The next one has the end of the first bin and the start of the second bin. The last time span
     * has the end as its end. This is sometimes used to try to speed up retrieval when using post processors over a
     * large time span.
     *
     * @param start            Instant start
     * @param end              Instant end
     * @param binSizeInSeconds &emsp;
     * @return TimeSpan The list of smaller time spans according to binSize
     */
    public static List<TimeSpan> breakIntoIntervals(Instant start, Instant end, long binSizeInSeconds) {
        assert (start.isBefore(end));
        List<TimeSpan> ret = new ArrayList<TimeSpan>();
        long startEpochSeconds = convertToEpochSeconds(start);
        long endEpochSeconds = convertToEpochSeconds(end);
        long intervals = (endEpochSeconds - startEpochSeconds) / binSizeInSeconds;
        if (intervals <= 0) {
            ret.add(new TimeSpan(start, end));
            return ret;
        } else {
            long currentBinEndEpochSeconds = ((startEpochSeconds / binSizeInSeconds) + 1) * binSizeInSeconds;
            if (startEpochSeconds != currentBinEndEpochSeconds)
                ret.add(new TimeSpan(start, TimeUtils.convertFromEpochSeconds(currentBinEndEpochSeconds - 1, 0)));
            currentBinEndEpochSeconds = currentBinEndEpochSeconds + binSizeInSeconds;
            while (currentBinEndEpochSeconds < endEpochSeconds) {
                ret.add(new TimeSpan(
                        TimeUtils.convertFromEpochSeconds(currentBinEndEpochSeconds - binSizeInSeconds, 0),
                        TimeUtils.convertFromEpochSeconds(currentBinEndEpochSeconds - 1, 0)));
                currentBinEndEpochSeconds = currentBinEndEpochSeconds + binSizeInSeconds;
            }
            if (endEpochSeconds != (currentBinEndEpochSeconds - binSizeInSeconds))
                ret.add(new TimeSpan(
                        TimeUtils.convertFromEpochSeconds(currentBinEndEpochSeconds - binSizeInSeconds, 0), end));
            return ret;
        }
    }

    public static Instant fromString(String timestampString, Instant defaultTime) throws IllegalArgumentException {

        // ISO datetimes are of the form "2011-02-02T08:00:00.000Z"
        Instant res = defaultTime;
        if (!StringUtils.isEmpty(timestampString)) {
            try {
                res = convertFromISO8601String(timestampString);
            } catch (IllegalArgumentException ex) {
                res = convertFromDateTimeStringWithOffset(timestampString);
            }
        }
        return res;
    }
}
