/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * There are various versions of timestamps in the archiver appliance. 
 * The most commonly used one is java.sql.Timestamp and is the one exposed in the APIs.
 * The other versions are follows
 * <ol>
 * <li>A long epoch seconds + optional nanos - The seconds part is the java epoch seconds, as returned by System.currentMillis()/1000</li>
 * <li>A long epoch milliseconds - This is the java epoch milliseconds, as returned by System.currentMillis()</li>
 * <li>A JCA timestamp - This is what comes out of JCA</li>
 * <li>A year/secondsintoyear/nanos combination - This is what is used in the protocol buffer storage plugin.</li>
 * <li>A ISO 8601 date time - We use JODA to convert from/to a ISO 8601 string</li>
 * </ol>
 * 
 * This class contains utilities to convert various forms of timestamps to/from the java.sql.Timestamp.
 * @author mshankar
 *
 */

public class TimeUtils {
	/**
	 * EPICS epoch starts at January 1, 1990 UTC.
	 * This constant contains the offset that must be added to epicstimestamps to generate java timestamps.
	 */
	public static final long EPICS_EPOCH_2_JAVA_EPOCH_OFFSET = computeEpicsEpochSecondsOffset(); 
	public static Logger logger = LogManager.getLogger(TimeUtils.class);

	public static java.sql.Timestamp convertFromEpochSeconds(long epochSeconds, int nanos) {
		Timestamp ts = new Timestamp(epochSeconds*1000);
		ts.setNanos(nanos);
		return ts;
	}

	public static java.sql.Timestamp convertFromInstant(Instant instant) {
		Timestamp ts = new Timestamp(instant.getEpochSecond()*1000);
		ts.setNanos(instant.getNano());
		return ts;
	}

	public static java.sql.Timestamp convertFromEpochMillis(long epochMillis) {
		Timestamp ts = new Timestamp(epochMillis);
		return ts;
	}

	public static java.sql.Timestamp convertFromJCATimeStamp(gov.aps.jca.dbr.TimeStamp jcats) {
		Timestamp ts = new Timestamp((jcats.secPastEpoch()+EPICS_EPOCH_2_JAVA_EPOCH_OFFSET)*1000);
		ts.setNanos((int) jcats.nsec());
		return ts;
	}

	private static final int NSecPerSec = 1000000000;
	
	public static java.sql.Timestamp convertFromYearSecondTimestamp(YearSecondTimestamp ysts) {
		Timestamp ts = new Timestamp((TimeUtils.getStartOfYearInSeconds(ysts.getYear()) + ysts.getSecondsintoyear())*1000);
		try {
			ts.setNanos(ysts.getNanos());
		} catch(IllegalArgumentException ex){
			// Apply the same technique as EPICS base; increment the seconds by the nanos overflow.
			int absNanos = Math.abs(ysts.getNanos());
			ts = new Timestamp(ts.getTime() + (((int)(absNanos / NSecPerSec))*1000));
			ts.setNanos(absNanos % NSecPerSec);
			logger.error("Invalid nanos " + ysts.getNanos() + " After patching,  timestamp is " + ts.getTime()/1000 + "/" + ts.getNanos());
		}
		return ts;
	}
	
	public static java.sql.Timestamp convertFromISO8601String(String tsstr) {
		// Sample ISO8601 string 2011-02-01T08:00:00.000Z
		DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
		DateTime dt = fmt.parseDateTime(tsstr);
		Timestamp ts = new Timestamp(dt.getMillis());
		return ts;
	}
	
	public static java.sql.Timestamp convertFromDateTimeStringWithOffset(String tsstr) {
		// Sample string 2012-11-03T00:00:00-07:00
		DateTimeFormatter fmt = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ssZ").withOffsetParsed();
		DateTime dt = fmt.parseDateTime(tsstr);
		Timestamp ts = new Timestamp(dt.getMillis());
		return ts;
	}


	public static long convertToEpochSeconds(java.sql.Timestamp ts) {
		return ts.getTime()/1000;
	}
	
	public static long convertToEpochMillis(java.sql.Timestamp ts) {
		return ts.getTime();
	}

	public static YearSecondTimestamp convertToYearSecondTimestamp(java.sql.Timestamp ts) {
		DateTime dateTime = new DateTime(ts.getTime(), DateTimeZone.UTC);
		DateTime startoftheYear = new DateTime(dateTime.getYear(), 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
		long startOfYearInSeconds = startoftheYear.getMillis()/1000;
		long epochSeconds = dateTime.getMillis()/1000;
		assert((epochSeconds - startOfYearInSeconds) < Integer.MAX_VALUE);
		int secondsIntoYear = (int) (epochSeconds - startOfYearInSeconds);
		return new YearSecondTimestamp((short) (dateTime.getYear()), secondsIntoYear, ts.getNanos());
	}
	
	public static YearSecondTimestamp convertToYearSecondTimestamp(gov.aps.jca.dbr.TimeStamp jcats) {
		DateTime dateTime = new DateTime((jcats.secPastEpoch()+EPICS_EPOCH_2_JAVA_EPOCH_OFFSET)*1000, DateTimeZone.UTC);
		DateTime startoftheYear = new DateTime(dateTime.getYear(), 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
		long startOfYearInSeconds = startoftheYear.getMillis()/1000;
		long epochSeconds = dateTime.getMillis()/1000;
		assert((epochSeconds - startOfYearInSeconds) < Integer.MAX_VALUE);
		int secondsIntoYear = (int) (epochSeconds - startOfYearInSeconds);
		return new YearSecondTimestamp((short) (dateTime.getYear()), secondsIntoYear, (int) jcats.nsec());
	}

	public static YearSecondTimestamp convertToYearSecondTimestamp(long epochSeconds) {
		DateTime dateTime = new DateTime(epochSeconds*1000, DateTimeZone.UTC);
		DateTime startoftheYear = new DateTime(dateTime.getYear(), 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
		long startOfYearInSeconds = startoftheYear.getMillis()/1000;
		assert((epochSeconds - startOfYearInSeconds) < Integer.MAX_VALUE);
		int secondsIntoYear = (int) (epochSeconds - startOfYearInSeconds);
		return new YearSecondTimestamp((short) (dateTime.getYear()), secondsIntoYear, 0);
	}

	public static String convertToISO8601String(java.sql.Timestamp ts) {
		DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
		DateTime dateTime = new DateTime(ts.getTime(), DateTimeZone.UTC);
		String retval = fmt.print(dateTime);
		return retval;
	}
	
	public static String convertToISO8601String(long epochSeconds) {
		DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
		DateTime dateTime = new DateTime(epochSeconds*1000, DateTimeZone.UTC);
		String retval = fmt.print(dateTime);
		return retval;
	}

	public static String convertToHumanReadableString(java.sql.Timestamp ts) {
		if(ts == null || ts.getTime() == 0) return "Never";
		DateTimeFormatter fmt = DateTimeFormat.forPattern("MMM/dd/yyyy HH:mm:ss z");
		DateTime dateTime = new DateTime(ts.getTime(), DateTimeZone.getDefault());
		String retval = fmt.print(dateTime);
		return retval;
	}
	
	public static String convertToHumanReadableString(long epochSeconds) {
		if(epochSeconds == 0) return "Never";
		DateTimeFormatter fmt = DateTimeFormat.forPattern("MMM/dd/yyyy HH:mm:ss z");
		DateTime dateTime = new DateTime(epochSeconds*1000, DateTimeZone.getDefault());
		String retval = fmt.print(dateTime);
		return retval;
	}
	
	public static long convertToLocalEpochMillis(long epochMillis) {
		if(epochMillis == 0) return 0;
		return epochMillis + DateTimeZone.getDefault().getOffset(epochMillis);
	}

	
	
	public static long getStartOfCurrentYearInSeconds() {
		DateTime now = new DateTime(DateTimeZone.UTC);
		DateTime startoftheYear = new DateTime(now.getYear(), 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
		return startoftheYear.getMillis()/1000;
	}

	public static long getStartOfYearInSeconds(int year) {
		DateTime startoftheYear = new DateTime(year, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
		return startoftheYear.getMillis()/1000;
	}

	public static Timestamp getStartOfYear(int year) {
		DateTime startoftheYear = new DateTime(year, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
		return new Timestamp(startoftheYear.getMillis());
	}

	public static long getStartOfYearInSeconds(long epochseconds) {
		// The JODA DateTime constructor takes millis
		DateTime dateTime = new DateTime(epochseconds*1000, DateTimeZone.UTC);
		DateTime startoftheYear = new DateTime(dateTime.getYear(), 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
		return startoftheYear.getMillis()/1000;
	}
	
	
	// We cache the start of the year for about 16000 years..
	static long[] startOfYearInEpochSeconds = new long[16*1024];
	private static short START_OF_CACHE_FOR_YEAR_STARTEPOCHSECONDS = 1970;
	static {
		for(short i=0; i < startOfYearInEpochSeconds.length; i++) {
			DateTime startoftheYear = new DateTime(START_OF_CACHE_FOR_YEAR_STARTEPOCHSECONDS + i, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
			startOfYearInEpochSeconds[i] = startoftheYear.getMillis()/1000; 
		}
	}
	

	/**
	 * In the protocol buffer storage plugin, we send the year as a short
	 * @param year Year
	 * @return startOfYearInEpochSeconds  &emsp;
	 */
	public static long getStartOfYearInSeconds(short year) {
		return startOfYearInEpochSeconds[year - START_OF_CACHE_FOR_YEAR_STARTEPOCHSECONDS];
	}
	
	public static Timestamp getEndOfYear(int year) {
		DateTime endoftheYear = new DateTime(year, 12, 31, 23, 59, 59, 999, DateTimeZone.UTC);
		return new Timestamp(endoftheYear.getMillis());
	}
	
	public static long getStartOfCurrentDayInEpochSeconds() {
		DateTime now = new DateTime(DateTimeZone.UTC);
		DateTime startofCurrentDay = now.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
		return startofCurrentDay.getMillis()/1000;
	}

	
	
	/**
	 * Convert Java EPOCH seconds to a seconds into year given the start of the year in EPOCH seconds
	 * @param epochseconds  &emsp;
	 * @param startOfYearInSeconds  &emsp;
	 * @return SecondsIntoYear The difference in Seconds
	 */
	public static int getSecondsIntoYear(long epochseconds, long startOfYearInSeconds) {
		long diffInSecs = epochseconds - startOfYearInSeconds;
		assert(diffInSecs <= Integer.MAX_VALUE);
		return (int)(diffInSecs);
	}

	/**
	 * Convert Java EPOCH seconds to a seconds into year
	 * @param epochseconds  &emsp;
	 * @return SecondsIntoYear The difference in Seconds
	 */
	public static int getSecondsIntoYear(long epochseconds) {
		long startOfYearInSeconds = getStartOfYearInSeconds(epochseconds);
		long diffInSecs = epochseconds - startOfYearInSeconds;
		assert(diffInSecs <= Integer.MAX_VALUE);
		return (int)(diffInSecs);
	}
	
	/**
	 * Determine year from java epoch seconds.
	 * @param epochseconds &emsp;
	 * @return YearForEpochSeconds &emsp;
	 */
	public static short computeYearForEpochSeconds(long epochseconds) {
		// The JODA DateTime constructor takes millis
		DateTime dateTime = new DateTime(epochseconds*1000, DateTimeZone.UTC);
		return (short) dateTime.year().get();
	}
	
	/**
	 * Get the current year in the UTC timezone
	 * @return CurrentYear &emsp;
	 */
	public static short getCurrentYear() {
		DateTime dateTime = new DateTime(DateTimeZone.UTC);
		return (short) dateTime.year().get();
	}

	/**
	 * Gets the current epoch seconds in the UTC timezone
	 * @return currentEpochSeconds &emsp;
	 */
	public static long getCurrentEpochSeconds() {
		DateTime dateTime = new DateTime(DateTimeZone.UTC);
		return dateTime.getMillis()/1000;
	}
	
	/**
	 * Gets the current epoch milli seconds in the UTC timezone
	 * @return currentEpochMilliSeconds &emsp;
	 */
	public static long getCurrentEpochMilliSeconds() {
		DateTime dateTime = new DateTime(DateTimeZone.UTC);
		return dateTime.getMillis();
	}

	/**
	 * Gets "now" as a Timestamp in the UTC timezone
	 * @return now A Timestamp in the UTC timezone
	 */
	public static Timestamp now() {
		DateTime dateTime = new DateTime(DateTimeZone.UTC);
		return new Timestamp(dateTime.getMillis());
	}
	
	/**
	 * Gets "now" as a YearSecondTimestamp in the UTC timezone
	 * @return now A YearSecondTimestamp in the UTC timezone
	 */
	public static YearSecondTimestamp nowYTS() {
		return convertToYearSecondTimestamp(now());
	}

	
	public static Timestamp plusHours(Timestamp ts, int hours) {
		DateTime dateTime = new DateTime(ts.getTime(), DateTimeZone.UTC);
		DateTime retval = dateTime.plusHours(hours);
		Timestamp retts = new Timestamp(retval.getMillis());
		retts.setNanos(ts.getNanos());
		return retts;
	}
	
	public static Timestamp minusHours(Timestamp ts, int hours) {
		DateTime dateTime = new DateTime(ts.getTime(), DateTimeZone.UTC);
		DateTime retval = dateTime.minusHours(hours);
		Timestamp retts = new Timestamp(retval.getMillis());
		retts.setNanos(ts.getNanos());
		return retts;
	}

	public static Timestamp plusDays(Timestamp ts, int days) {
		DateTime dateTime = new DateTime(ts.getTime(), DateTimeZone.UTC);
		DateTime retval = dateTime.plusDays(days);
		Timestamp retts = new Timestamp(retval.getMillis());
		retts.setNanos(ts.getNanos());
		return retts;
	}
	
	public static Timestamp minusDays(Timestamp ts, int days) {
		DateTime dateTime = new DateTime(ts.getTime(), DateTimeZone.UTC);
		DateTime retval = dateTime.minusDays(days);
		Timestamp retts = new Timestamp(retval.getMillis());
		retts.setNanos(ts.getNanos());
		return retts;
	}
	
	private static long computeEpicsEpochSecondsOffset() {
		DateTime Jan11990UTC = new DateTime(1990, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
		long diff = Jan11990UTC.getMillis()/1000;
		// Per Bob, this should be 631152000 
		assert(diff == 631152000L);
		return diff;
	}

	/**
	 * Given a start time and an end time, this method breaks this span into a sequence of spans each of which fits within a year.
	 * Used where data is partitioned by year....
	 * @param start The start time
	 * @param end The end time
	 * @return breakIntoYearlyTimeSpans   &emsp;
	 */
	public static List<TimeSpan> breakIntoYearlyTimeSpans(Timestamp start, Timestamp end) {
		assert(start.getTime() <= end.getTime());
		YearSecondTimestamp startYTS = convertToYearSecondTimestamp(start);
		YearSecondTimestamp endYTS = convertToYearSecondTimestamp(end);
		LinkedList<TimeSpan> ret = new LinkedList<TimeSpan>(); 
		if(startYTS.getYear() == endYTS.getYear()) {
			ret.add(new TimeSpan(start, end));
			return ret;
		} else {
			ret.add(new TimeSpan(start, getEndOfYear(startYTS.getYear())));
			for(int year = startYTS.getYear()+1; year < endYTS.getYear(); year++) {
				ret.add(new TimeSpan(getStartOfYear(year), getEndOfYear(year)));
			}
			ret.add(new TimeSpan(getStartOfYear(endYTS.getYear()), end));
			return ret;
		}
	}
	
	private static String[] TWO_DIGIT_EXPANSIONS = {"00", "01", "02", "03", "04", "05", "06", "07", "08", "09"};
	
	/**
	 * Returns a partition name for the given epoch second based on the partition granularity.
	 * 
	 * @param epochSeconds &emsp;
	 * @param granularity Partition granularity of the file.
	 * @return PartitionName &emsp;
	 */
	public static String getPartitionName(long epochSeconds, PartitionGranularity granularity) {
		DateTime dateTime = new DateTime(epochSeconds*1000, DateTimeZone.UTC);
		switch(granularity) {
		case PARTITION_YEAR:
			return "" + dateTime.getYear();
		case PARTITION_MONTH:
			return "" + dateTime.getYear() 
			+ "_" + ( dateTime.getMonthOfYear() < 10 ? TWO_DIGIT_EXPANSIONS[dateTime.getMonthOfYear()] : dateTime.getMonthOfYear());
		case PARTITION_DAY:
			return "" + dateTime.getYear() 
			+ "_" + ( dateTime.getMonthOfYear() < 10 ? TWO_DIGIT_EXPANSIONS[dateTime.getMonthOfYear()] : dateTime.getMonthOfYear())
			+ "_" + ( dateTime.getDayOfMonth() < 10 ? TWO_DIGIT_EXPANSIONS[dateTime.getDayOfMonth()] : dateTime.getDayOfMonth());
		case PARTITION_HOUR:
			return "" + dateTime.getYear() 
			+ "_" + ( dateTime.getMonthOfYear() < 10 ? TWO_DIGIT_EXPANSIONS[dateTime.getMonthOfYear()] : dateTime.getMonthOfYear())
			+ "_" + ( dateTime.getDayOfMonth() < 10 ? TWO_DIGIT_EXPANSIONS[dateTime.getDayOfMonth()] : dateTime.getDayOfMonth())
			+ "_" + ( dateTime.getHourOfDay() < 10 ? TWO_DIGIT_EXPANSIONS[dateTime.getHourOfDay()] : dateTime.getHourOfDay());
		case PARTITION_5MIN:
		case PARTITION_15MIN:
		case PARTITION_30MIN:
			int approxMinutesPerChunk = granularity.getApproxMinutesPerChunk();
			int startOfPartition_Min = (dateTime.getMinuteOfHour()/approxMinutesPerChunk)*approxMinutesPerChunk;
			return "" + dateTime.getYear() 
			+ "_" + ( dateTime.getMonthOfYear() < 10 ? TWO_DIGIT_EXPANSIONS[dateTime.getMonthOfYear()] : dateTime.getMonthOfYear())
			+ "_" + ( dateTime.getDayOfMonth() < 10 ? TWO_DIGIT_EXPANSIONS[dateTime.getDayOfMonth()] : dateTime.getDayOfMonth())
			+ "_" + ( dateTime.getHourOfDay() < 10 ? TWO_DIGIT_EXPANSIONS[dateTime.getHourOfDay()] : dateTime.getHourOfDay())
			+ "_" + ( startOfPartition_Min < 10 ? TWO_DIGIT_EXPANSIONS[startOfPartition_Min] : startOfPartition_Min);
		default:
			throw new UnsupportedOperationException("Invalid Partition type " + granularity);
		}
	}
	
	/**
	 * Given an epoch seconds and a granularity, this method gives you the first second in the next partition as epoch seconds.
	 * @param epochSeconds &emsp;
	 * @param granularity Partition granularity of the file.
	 * @return NextPartitionFirstSecond &emsp;
	 */
	public static long getNextPartitionFirstSecond(long epochSeconds, PartitionGranularity granularity) {
		DateTime dateTime = new DateTime(epochSeconds*1000, DateTimeZone.UTC);
		DateTime nextPartitionFirstSecond = null;
		switch(granularity) {
		case PARTITION_YEAR:
			nextPartitionFirstSecond = dateTime.plusYears(1).withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
			return nextPartitionFirstSecond.getMillis()/1000;
		case PARTITION_MONTH:
			nextPartitionFirstSecond = dateTime.plusMonths(1).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
			return nextPartitionFirstSecond.getMillis()/1000;
		case PARTITION_DAY:
			nextPartitionFirstSecond = dateTime.plusDays(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
			return nextPartitionFirstSecond.getMillis()/1000;
		case PARTITION_HOUR:
			nextPartitionFirstSecond = dateTime.plusHours(1).withMinuteOfHour(0).withSecondOfMinute(0);
			return nextPartitionFirstSecond.getMillis()/1000;
		case PARTITION_5MIN:
		case PARTITION_15MIN:
		case PARTITION_30MIN:
			int approxMinutesPerChunk = granularity.getApproxMinutesPerChunk();
			DateTime nextPartForMin = dateTime.plusMinutes(approxMinutesPerChunk);
			int startOfPartitionForMin = (nextPartForMin.getMinuteOfHour()/approxMinutesPerChunk)*approxMinutesPerChunk;
			nextPartitionFirstSecond = nextPartForMin.withMinuteOfHour(startOfPartitionForMin).withSecondOfMinute(0);
			return nextPartitionFirstSecond.getMillis()/1000;
		default:
			throw new UnsupportedOperationException("Invalid Partition type " + granularity);
		}
	}
	
	/**
	 * Given an epoch seconds and a granularity, this method gives you the last second in the previous partition as epoch seconds.
	 * @param epochSeconds &emsp;
	 * @param granularity Partition granularity of the file.
	 * @return PreviousPartitionLastSecond  &emsp;
	 */
	public static long getPreviousPartitionLastSecond(long epochSeconds, PartitionGranularity granularity) {
		DateTime dateTime = new DateTime(epochSeconds*1000, DateTimeZone.UTC);
		DateTime previousPartitionLastSecond = null;
		switch(granularity) {
		case PARTITION_YEAR:
			previousPartitionLastSecond = dateTime.minusYears(1).withMonthOfYear(12).withDayOfMonth(31).withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59);
			return previousPartitionLastSecond.getMillis()/1000;
		case PARTITION_MONTH:
			previousPartitionLastSecond = dateTime.withDayOfMonth(1).minusDays(1).withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59);
			return previousPartitionLastSecond.getMillis()/1000;
		case PARTITION_DAY:
			previousPartitionLastSecond = dateTime.minusDays(1).withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59);
			return previousPartitionLastSecond.getMillis()/1000;
		case PARTITION_HOUR:
			previousPartitionLastSecond = dateTime.minusHours(1).withMinuteOfHour(59).withSecondOfMinute(59);
			return previousPartitionLastSecond.getMillis()/1000;
		case PARTITION_5MIN:
		case PARTITION_15MIN:
		case PARTITION_30MIN:
			int approxMinutesPerChunk = granularity.getApproxMinutesPerChunk();
			int startOfPartition_Min = (dateTime.getMinuteOfHour()/approxMinutesPerChunk)*approxMinutesPerChunk;
			previousPartitionLastSecond = dateTime.withMinuteOfHour(startOfPartition_Min).withSecondOfMinute(0).minusSeconds(1);
			return previousPartitionLastSecond.getMillis()/1000;
		default:
			throw new UnsupportedOperationException("Invalid Partition type " + granularity);
		}
	}
	
	
	/**
	 * Event rate rate limiting uses a tenths of a seconds units to cater to monitor intervals of 0.1 seconds, 0.5 seconds etc.. 
	 * This converts a epochSeconds+nanos to time in terms of tenths of a second.
	 * @param epochSeconds &emsp;
	 * @param nanos &emsp;
	 * @return TenthsOfASecond  &emsp;
	 * @throws NumberFormatException  &emsp; 
	 */
	public static long convertToTenthsOfASecond(long epochSeconds, int nanos) throws NumberFormatException {
		int tenthsPieceOfNanos = (nanos/(100000000));
		if(tenthsPieceOfNanos > 9) {
			throw new NumberFormatException("Tenths of nanos cannot be greater than 9 but this is " + tenthsPieceOfNanos);
		}
		return epochSeconds*10 + tenthsPieceOfNanos;
	}
	
	
	/**
	 * Whether we are in DST for a particular time in the servers default timezone.
	 * Mostly used by Matlab.
	 * @param ts Timestamp
	 * @return boolean True or False
	 */
	public static boolean isDST(Timestamp ts) { 
		return !DateTimeZone.getDefault().isStandardOffset(ts.getTime());
	}
	
	
	
	/**
	 * Break a time span into smaller time spans according to binSize
	 * The first time span has the start time and the end of the first bin.
	 * The next one has the end of the first bin and the start of the second bin.
	 * The last time span has the end as its end.
	 * This is sometimes used to try to speed up retrieval when using post processors over a large time span. 
	 * 
	 * @param start Timestamp start
	 * @param end Timestamp end
	 * @param binSizeInSeconds  &emsp;
	 * @return TimeSpan The list of smaller time spans according to binSize
	 */
	public static List<TimeSpan> breakIntoIntervals(Timestamp start, Timestamp end, long binSizeInSeconds) {
		assert(start.getTime() <= end.getTime());
		List<TimeSpan> ret = new ArrayList<TimeSpan>();
		long startEpochSeconds = convertToEpochSeconds(start);
		long endEpochSeconds = convertToEpochSeconds(end);
		long intervals = (endEpochSeconds - startEpochSeconds)/binSizeInSeconds;
		if(intervals <= 0) { 
			ret.add(new TimeSpan(start, end));
			return ret;
		} else { 
			long currentBinEndEpochSeconds = ((startEpochSeconds/binSizeInSeconds) + 1)*binSizeInSeconds;
			if(startEpochSeconds != currentBinEndEpochSeconds)
				ret.add(new TimeSpan(start, TimeUtils.convertFromEpochSeconds(currentBinEndEpochSeconds - 1, 0)));
			currentBinEndEpochSeconds = currentBinEndEpochSeconds + binSizeInSeconds;
			while(currentBinEndEpochSeconds < endEpochSeconds) { 
				ret.add(new TimeSpan(TimeUtils.convertFromEpochSeconds(currentBinEndEpochSeconds - binSizeInSeconds, 0), TimeUtils.convertFromEpochSeconds(currentBinEndEpochSeconds - 1, 0)));
				currentBinEndEpochSeconds = currentBinEndEpochSeconds + binSizeInSeconds;
			}
			if(endEpochSeconds != (currentBinEndEpochSeconds - binSizeInSeconds))
				ret.add(new TimeSpan(TimeUtils.convertFromEpochSeconds(currentBinEndEpochSeconds - binSizeInSeconds, 0), end));
			return ret;
		}
	}

}
