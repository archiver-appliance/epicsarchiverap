package edu.stanford.slac.archiverappliance.PlainPB;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEventStreamIterator;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Some simple tests for the FileBackedPBEventStream
 * We generate a years worth of data and then create FileBackedPBEventStream's using various constructors and make sure we get the expected amount of data. 
 * @author mshankar
 *
 */
public class FileBackedPBEventStreamTest {
	private static Logger logger = Logger.getLogger(FileBackedPBEventStreamTest.class.getName());
	File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "FileBackedPBEventStreamTest");
	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":FileBackedPBEventStreamTest";
	ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
	PlainPBStoragePlugin storagePlugin;
	private boolean leapYear = new GregorianCalendar().isLeapYear(TimeUtils.getCurrentYear());;
	private ConfigService configService;


	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		storagePlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=FileBackedPBEventStreamTest&rootFolder=" + testFolder.getAbsolutePath() + "&partitionGranularity=PARTITION_YEAR", configService);
		int phasediffindegrees = 10;
		SimulationEventStream simstream = new SimulationEventStream(dbrType, new SineGenerator(phasediffindegrees));
		try(BasicContext context = new BasicContext()) {
			storagePlugin.appendData(context, pvName, simstream);
		}
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void test() throws Exception {
		testLocationBasedEventBeforeTime();
		testCompleteStream();
		testLocationBasedIterator();
		testTimeBasedIterator();
	}

	private void testCompleteStream() throws Exception {
		try(BasicContext context = new BasicContext()) {
			long startMs = System.currentTimeMillis();
			Path path = PlainPBPathNameUtility.getPathNameForTime(storagePlugin, pvName, TimeUtils.getStartOfCurrentYearInSeconds() + 24*60*60*7, context.getPaths(), configService.getPVNameToKeyConverter());
			assertTrue("Did we not write any data?", path != null);
			int eventCount = 0;
			try(FileBackedPBEventStream stream = new FileBackedPBEventStream(pvName, path, dbrType)) {
				for(Event e : stream) {
					e.getEventTimeStamp();
					eventCount++;
				}
			}
			int expectedSamples = new GregorianCalendar().isLeapYear(TimeUtils.getCurrentYear()) ? SimulationEventStreamIterator.LEAPYEAR_NUMBER_OF_SAMPLES : SimulationEventStreamIterator.DEFAULT_NUMBER_OF_SAMPLES;
			assertTrue("Expected " + expectedSamples + " got " + eventCount, eventCount == expectedSamples);
			long endMs = System.currentTimeMillis();
			logger.info("Time for " + eventCount + " samples = " + (endMs - startMs) + "(ms)");
		}
	}
	
	private void testLocationBasedIterator() throws Exception {
		try(BasicContext context = new BasicContext()) {
			Path path = PlainPBPathNameUtility.getPathNameForTime(storagePlugin, pvName, TimeUtils.getStartOfCurrentYearInSeconds() + 24*60*60*7, context.getPaths(), configService.getPVNameToKeyConverter());
			int eventCount = 0;
			try(FileBackedPBEventStream stream = new FileBackedPBEventStream(pvName, path, dbrType, 0, Files.size(path))) {
				for(@SuppressWarnings("unused") Event e : stream) {
					eventCount++;
				}
			}
			int expectedSamples = leapYear ? SimulationEventStreamIterator.LEAPYEAR_NUMBER_OF_SAMPLES : SimulationEventStreamIterator.DEFAULT_NUMBER_OF_SAMPLES;
			assertTrue("Expected " + expectedSamples + " got " + eventCount, eventCount == expectedSamples);
		}

		try(BasicContext context = new BasicContext()) {
			Path path = PlainPBPathNameUtility.getPathNameForTime(storagePlugin, pvName, TimeUtils.getStartOfCurrentYearInSeconds() + 24*60*60*7, context.getPaths(), configService.getPVNameToKeyConverter());
			int eventCount = 0;
			try(FileBackedPBEventStream stream = new FileBackedPBEventStream(pvName, path, dbrType, Files.size(path), Files.size(path)+1)) {
				for(@SuppressWarnings("unused") Event e : stream) {
					eventCount++;
				}
			}
			int expectedSamples = 0;
			assertTrue("Expected " + expectedSamples + " got " + eventCount, eventCount == expectedSamples);
		}
	}
	
	private void testTimeBasedIterator() throws Exception {
		for(int i = 0; i < 2; i++) {
			boolean skipSearch = (i==0);
			try(BasicContext context = new BasicContext()) {
				Path path = PlainPBPathNameUtility.getPathNameForTime(storagePlugin, pvName, TimeUtils.getStartOfCurrentYearInSeconds() + 24*60*60*7, context.getPaths(), configService.getPVNameToKeyConverter());
				int eventCount = 0;
				// Start 11 days into the year and get two days worth of data.
				long startEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds() + 24*60*60*11;
				Timestamp start = TimeUtils.convertFromEpochSeconds(startEpochSeconds, 0);
				int secondsToExtract = 24*60*60*2;
				Timestamp end = TimeUtils.convertFromEpochSeconds(startEpochSeconds + secondsToExtract, 0);
				try(FileBackedPBEventStream stream = new FileBackedPBEventStream(pvName, path, dbrType, start, end, skipSearch)) {
					long eventEpochSeconds = 0;
					for(Event e : stream) {
						eventEpochSeconds = e.getEpochSeconds();
						if(eventCount < 2) {
							logger.info("Starting event timestamp " + TimeUtils.convertToHumanReadableString(eventEpochSeconds));
						}
						eventCount++;
					}
					logger.info("Final event timestamp " + TimeUtils.convertToHumanReadableString(eventEpochSeconds));
				}
				int expectedSamples = secondsToExtract + 1;
				assertTrue("Expected " + expectedSamples + " got " + eventCount + " with skipSearch " + skipSearch, eventCount == expectedSamples);
			}
			
			// Same as before expect the start time is before the year
			try(BasicContext context = new BasicContext()) {
				Path path = PlainPBPathNameUtility.getPathNameForTime(storagePlugin, pvName, TimeUtils.getStartOfCurrentYearInSeconds() + 24*60*60*7, context.getPaths(), configService.getPVNameToKeyConverter());
				int eventCount = 0;
				// Start 11 days into the year and get two days worth of data.
				long startEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds() - 24*60*60;
				Timestamp start = TimeUtils.convertFromEpochSeconds(startEpochSeconds, 0);
				int secondsToExtract = 24*60*60*2;
				Timestamp end = TimeUtils.convertFromEpochSeconds(startEpochSeconds + secondsToExtract, 0);
				try(FileBackedPBEventStream stream = new FileBackedPBEventStream(pvName, path, dbrType, start, end, skipSearch)) {
					long eventEpochSeconds = 0;
					for(Event e : stream) {
						eventEpochSeconds = e.getEpochSeconds();
						if(eventCount < 2) {
							logger.info("Starting event timestamp " + TimeUtils.convertToHumanReadableString(eventEpochSeconds));
						}
						eventCount++;
					}
					logger.info("Final event timestamp " + TimeUtils.convertToHumanReadableString(eventEpochSeconds));
				}
				// We should only get one days worth of data.
				int expectedSamples = 24*60*60 + 1;
				assertTrue("Expected " + expectedSamples + " got " + eventCount + " with skipSearch " + skipSearch, eventCount == expectedSamples);
			}

			// This time, change the end time
			try(BasicContext context = new BasicContext()) {
				Path path = PlainPBPathNameUtility.getPathNameForTime(storagePlugin, pvName, TimeUtils.getStartOfCurrentYearInSeconds() + 24*60*60*7, context.getPaths(), configService.getPVNameToKeyConverter());
				int eventCount = 0;
				// Start 11 days into the year and get two days worth of data.
				long startEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds() + 360*24*60*60;
				Timestamp start = TimeUtils.convertFromEpochSeconds(startEpochSeconds, 0);
				int secondsToExtract = 24*60*60*10;
				Timestamp end = TimeUtils.convertFromEpochSeconds(startEpochSeconds + secondsToExtract, 0);
				long eventEpochSeconds = 0;
				try(FileBackedPBEventStream stream = new FileBackedPBEventStream(pvName, path, dbrType, start, end, skipSearch)) {
					for(Event e : stream) {
						eventEpochSeconds = e.getEpochSeconds();
						if(eventCount < 2) {
							logger.info("Starting event timestamp " + TimeUtils.convertToHumanReadableString(eventEpochSeconds));
						}
						eventCount++;
					}
				}
				logger.info("Final event timestamp " + TimeUtils.convertToHumanReadableString(eventEpochSeconds));
				// Based on whether this is a leap year, we should get 5-6 days worth of data
				int expectedSamples = leapYear ? 24*60*60*6 + 1  : 24*60*60*5 + 1;
				assertTrue("Expected " + expectedSamples + " got " + eventCount + " with skipSearch " + skipSearch, eventCount == expectedSamples);
			}
		}
	}
	
	private void testLocationBasedEventBeforeTime() throws IOException { 
		try(BasicContext context = new BasicContext()) {
			Path path = PlainPBPathNameUtility.getPathNameForTime(storagePlugin, pvName, TimeUtils.getStartOfCurrentYearInSeconds() + 24*60*60*7, context.getPaths(), configService.getPVNameToKeyConverter());
			// Start 11 days into the year and get two days worth of data.
			long epochSeconds = TimeUtils.getStartOfCurrentYearInSeconds() + 7*24*60*60;
			Timestamp time = TimeUtils.convertFromEpochSeconds(epochSeconds, 0);
			try(FileBackedPBEventStream stream = new FileBackedPBEventStream(pvName, path, dbrType, time, TimeUtils.getEndOfYear(TimeUtils.getCurrentYear()), false)) {
				boolean firstEvent = true;
				for(Event e : stream) {
					if(firstEvent) {
						assertTrue(
								"The first event should be before timestamp " 
										+ TimeUtils.convertToHumanReadableString(time) 
										+ " got " 
										+ TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()), e.getEventTimeStamp().before(time));
						firstEvent = false;
					} else {
						// All other events should be after timestamp
						assertTrue(
								"All other events should be on or after timestamp " 
										+ TimeUtils.convertToHumanReadableString(time) 
										+ " got " 
										+ TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()), e.getEventTimeStamp().after(time) || e.getEventTimeStamp().equals(time));
					}
				}
			}
		}
	}
}
