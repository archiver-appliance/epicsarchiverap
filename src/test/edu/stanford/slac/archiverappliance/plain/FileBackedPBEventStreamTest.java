package edu.stanford.slac.archiverappliance.plain;

import static edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin.PB_PLUGIN_IDENTIFIER;
import static org.epics.archiverappliance.common.TimeUtils.convertFromEpochSeconds;
import static org.epics.archiverappliance.common.TimeUtils.convertToISO8601String;
import static org.epics.archiverappliance.common.TimeUtils.getCurrentYear;
import static org.epics.archiverappliance.common.TimeUtils.getEndOfYear;
import static org.epics.archiverappliance.common.TimeUtils.getStartOfCurrentYearInSeconds;
import static org.epics.archiverappliance.common.TimeUtils.getStartOfYearInSeconds;
import static org.epics.archiverappliance.utils.ui.URIUtils.pluginString;

import edu.stanford.slac.archiverappliance.plain.pb.FileBackedPBEventStream;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.BiDirectionalIterable;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Some simple tests for the FileBackedPBEventStream
 * We generate a years worth of data and then create FileBackedPBEventStream's using various constructors and make sure we get the expected amount of data.
 * @author mshankar
 *
 */
public class FileBackedPBEventStreamTest {
    private static final Logger logger = LogManager.getLogger(FileBackedPBEventStreamTest.class.getName());
    private static final ConfigServiceForTests configService;
    static File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder()
            + File.separator
            + FileBackedPBEventStreamTest.class.getSimpleName());
    static String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":"
            + FileBackedPBEventStreamTest.class.getSimpleName();
    static ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
    static String storagePBPluginString = pluginString(
            PB_PLUGIN_IDENTIFIER,
            "localhost",
            "name=" + FileBackedPBEventStreamTest.class.getSimpleName() + "&rootFolder=" + testFolder.getAbsolutePath()
                    + "&partitionGranularity=PARTITION_YEAR");
    private static long events;

    private static final Instant oneWeekIntoYear = TimeUtils.getStartOfYear(TimeUtils.getCurrentYear())
            .plusSeconds(PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 7L);

    static {
        try {
            configService = new ConfigServiceForTests(-1);
        } catch (ConfigException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    public static void setUp() throws Exception {
        deleteData();
        events = createTestData();
    }

    @AfterAll
    public static void tearDown() throws IOException {
        deleteData();
    }

    private static void deleteData() throws IOException {
        FileUtils.deleteDirectory(new File(getStoragePlugin().getRootFolder()));
    }

    private static long createTestData() throws IOException {
        PlainStoragePlugin storagePlugin = getStoragePlugin();
        short currentYear = TimeUtils.getCurrentYear();
        Instant start = TimeUtils.getStartOfYear(currentYear);
        Instant end = TimeUtils.getEndOfYear(currentYear);
        logger.info("start {} end {}", start, end);
        SimulationEventStream simstream = new SimulationEventStream(dbrType, new SineGenerator(10), start, end, 1);
        try (BasicContext context = new BasicContext()) {
            assert storagePlugin != null;
            return storagePlugin.appendData(context, pvName, simstream);
        }
    }

    private static PlainStoragePlugin getStoragePlugin() throws IOException {
        return (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(storagePBPluginString, configService);
    }

    private static Stream<Arguments> provideTimeBasedIterator() {
        long twoDays = PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 2L;
        return Arrays.stream(new Boolean[] {true, false})
                .flatMap(sS -> Stream.of(
                        // Start 11 seconds into the year and get two seconds worth of data.
                        Arguments.of(
                                sS,
                                convertFromEpochSeconds(getStartOfCurrentYearInSeconds() + 11L, 0),
                                convertFromEpochSeconds(getStartOfCurrentYearInSeconds() + 11L + 2, 0),
                                2 + 1),

                        // Start one second before the year and end one second in to get one second of data
                        Arguments.of(
                                sS,
                                convertFromEpochSeconds(getStartOfCurrentYearInSeconds() - 1, 0),
                                convertFromEpochSeconds(getStartOfCurrentYearInSeconds() - 1 + 2, 0),
                                1 + 1),

                        // Start at one second before end of year and end 2 seconds later to get 1 second
                        Arguments.of(
                                sS,
                                convertFromEpochSeconds(getStartOfYearInSeconds(getCurrentYear()) - 1, 0),
                                convertFromEpochSeconds(getStartOfYearInSeconds(getCurrentYear()) - 1 + 2, 0),
                                1 + 1)));
    }

    @Test
    public void testCompleteStream() throws Exception {
        PlainStoragePlugin storagePlugin = getStoragePlugin();

        try (BasicContext context = new BasicContext()) {
            long startMs = System.currentTimeMillis();
            Path path = PathNameUtility.getPathNameForTime(
                    storagePlugin,
                    pvName,
                    oneWeekIntoYear,
                    context.getPaths(),
                    configService.getPVNameToKeyConverter());
            Assertions.assertNotNull(path, "Did we not write any data?");
            long eventCount = 0;
            try (FileBackedPBEventStream stream = new FileBackedPBEventStream(pvName, path, dbrType)) {
                for (Event e : stream) {
                    e.getEventTimeStamp();
                    eventCount++;
                }
            }
            Assertions.assertEquals(events, eventCount, "Expected " + events + " got " + eventCount);
            long endMs = System.currentTimeMillis();
            logger.info("Time for " + eventCount + " samples = " + (endMs - startMs) + "(ms)");
        }
    }

    @Test
    public void testLocationBasedIterator() throws Exception {
        PlainStoragePlugin storagePlugin = getStoragePlugin();

        try (BasicContext context = new BasicContext()) {
            Path path = PathNameUtility.getPathNameForTime(
                    storagePlugin,
                    pvName,
                    oneWeekIntoYear,
                    context.getPaths(),
                    configService.getPVNameToKeyConverter());
            long eventCount = 0;
            try (FileBackedPBEventStream stream =
                    new FileBackedPBEventStream(pvName, path, dbrType, 0, Files.size(path))) {
                for (@SuppressWarnings("unused") Event e : stream) {
                    eventCount++;
                }
            }
            long expectedSamples = events;
            Assertions.assertEquals(expectedSamples, eventCount, "Expected " + expectedSamples + " got " + eventCount);
        }

        try (BasicContext context = new BasicContext()) {
            Path path = PathNameUtility.getPathNameForTime(
                    storagePlugin,
                    pvName,
                    oneWeekIntoYear,
                    context.getPaths(),
                    configService.getPVNameToKeyConverter());
            long eventCount = 0;
            try (FileBackedPBEventStream stream =
                    new FileBackedPBEventStream(pvName, path, dbrType, Files.size(path), Files.size(path) + 1)) {
                for (@SuppressWarnings("unused") Event e : stream) {
                    eventCount++;
                }
            }
            long expectedSamples = 0;
            Assertions.assertEquals(expectedSamples, eventCount, "Expected " + expectedSamples + " got " + eventCount);
        }
    }

    @ParameterizedTest
    @MethodSource("provideTimeBasedIterator")
    public void testTimeBasedIterator(boolean skipSearch, Instant start, Instant end, long expectedEventCount)
            throws IOException {

        PlainStoragePlugin storagePlugin = getStoragePlugin();
        try (BasicContext context = new BasicContext()) {
            Path path = PathNameUtility.getPathNameForTime(
                    storagePlugin,
                    pvName,
                    oneWeekIntoYear,
                    context.getPaths(),
                    configService.getPVNameToKeyConverter());
            long eventCount = 0;
            try (FileBackedPBEventStream stream =
                    new FileBackedPBEventStream(pvName, path, dbrType, start, end, skipSearch)) {
                long eventEpochSeconds = 0;
                for (Event e : stream) {
                    eventEpochSeconds = e.getEpochSeconds();
                    logger.info("event timestamp " + convertToISO8601String(eventEpochSeconds));

                    if (eventCount < 1) {
                        logger.info("Starting event timestamp " + convertToISO8601String(eventEpochSeconds));
                    } else if (eventCount >= expectedEventCount) {
                        logger.info("Ending event timestamp " + convertToISO8601String(eventEpochSeconds));
                    }
                    eventCount++;
                }
                logger.info("Final event timestamp " + convertToISO8601String(eventEpochSeconds));
            }
            Assertions.assertEquals(
                    expectedEventCount,
                    eventCount,
                    "Expected " + expectedEventCount + " got " + eventCount + " with skipSearch " + skipSearch);
        }
    }

    @Test
    public void testLocationBasedEventBeforeTime() throws IOException {
        PlainStoragePlugin storagePlugin = getStoragePlugin();

        try (BasicContext context = new BasicContext()) {
            Path path = PathNameUtility.getPathNameForTime(
                    storagePlugin,
                    pvName,
                    oneWeekIntoYear,
                    context.getPaths(),
                    configService.getPVNameToKeyConverter());
            // Start 11 days into the year and get two days worth of data.
            long epochSeconds = getStartOfCurrentYearInSeconds()
                    + 7L * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
            Instant time = convertFromEpochSeconds(epochSeconds, 0);
            try (EventStream stream = FileStreamCreator.getTimeStream(
                    pvName,
                    path,
                    dbrType,
                    time,
                    time.plusSeconds(PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 2L),
                    false)) {
                boolean firstEvent = true;
                for (Event e : stream) {
                    if (firstEvent) {
                        Assertions.assertEquals(
                                e.getEventTimeStamp(),
                                time,
                                "The first event should be equal timestamp "
                                        + convertToISO8601String(time)
                                        + " got "
                                        + convertToISO8601String(e.getEventTimeStamp()));
                        firstEvent = false;
                    } else {
                        // All other events should be after timestamp
                        Assertions.assertTrue(
                                e.getEventTimeStamp().isAfter(time)
                                        || e.getEventTimeStamp().equals(time),
                                "All other events should be on or after timestamp "
                                        + convertToISO8601String(time)
                                        + " got "
                                        + convertToISO8601String(e.getEventTimeStamp()));
                    }
                }
            }
        }
    }

    @Test
    public void testDirectionalIteration() throws IOException {
        PlainStoragePlugin storagePlugin = getStoragePlugin();

        for (BiDirectionalIterable.IterationDirection direction : BiDirectionalIterable.IterationDirection.values()) {
            logger.info("Testing directional iteration {}", direction);
            Instant startAtTime = (direction == BiDirectionalIterable.IterationDirection.BACKWARDS)
                    ? TimeUtils.getStartOfYear(TimeUtils.getCurrentYear() + 1)
                    : TimeUtils.getStartOfYear(TimeUtils.getCurrentYear());

            try (BasicContext context = new BasicContext()) {
                long startMs = System.currentTimeMillis();
                Path path = PathNameUtility.getPathNameForTime(
                        storagePlugin,
                        pvName,
                        oneWeekIntoYear,
                        context.getPaths(),
                        configService.getPVNameToKeyConverter());
                Assertions.assertNotNull(path, "Did we not write any data?");
                long eventCount = 0;
                try (FileBackedPBEventStream stream =
                        new FileBackedPBEventStream(pvName, path, dbrType, startAtTime, direction)) {
                    for (Event e : stream) {
                        try {
                            logger.debug(
                                    "Timestamp of sample {}",
                                    TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
                        } catch (Exception ex) {
                            logger.error("Exception at event " + eventCount, ex);
                            throw ex;
                        }
                        eventCount++;
                    }
                }
                Assertions.assertEquals(events, eventCount, "Expected " + events + " got " + eventCount);
                long endMs = System.currentTimeMillis();
                logger.info("Time for " + eventCount + " samples = " + (endMs - startMs) + "(ms)");
            }
        }
    }

    private Instant getFirstSampleTSUsingIteration(
            Instant startAtTime, BiDirectionalIterable.IterationDirection direction) throws IOException {
        PlainStoragePlugin storagePlugin = getStoragePlugin();
        Instant theInstant = null;
        try (BasicContext context = new BasicContext()) {
            Path path = PathNameUtility.getPathNameForTime(
                    storagePlugin,
                    pvName,
                    oneWeekIntoYear,
                    context.getPaths(),
                    configService.getPVNameToKeyConverter());
            Assertions.assertNotNull(path, "Did we not write any data?");
            long eventCount = 0;
            try (FileBackedPBEventStream stream = new FileBackedPBEventStream(
                    pvName, path, dbrType, startAtTime, BiDirectionalIterable.IterationDirection.FORWARDS)) {
                for (Event e : stream) {
                    try {
                        logger.info(
                                "Timestamp of first sample {} using direction {}",
                                TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()),
                                direction);
                        theInstant = e.getEventTimeStamp();
                        eventCount++;
                        break;
                    } catch (Exception ex) {
                        logger.error("Exception at event " + eventCount, ex);
                        throw ex;
                    }
                }
            }
            Assertions.assertEquals(1, eventCount, "Expected " + 1 + " event got " + eventCount);
        }
        Assertions.assertNotNull(theInstant, "The first sample using forwards iteration is null");
        return theInstant;
    }

    @Test
    public void testBothDirectionsYieldSameInstant() throws IOException {
        // Start iteration at the same time using forwards and backwards iteration and make sure we get the same first
        // event.

        // Somewhere in the middle of the year.
        Instant startAtTime =
                TimeUtils.getStartOfYear(TimeUtils.getCurrentYear()).plusSeconds(86400 * 30 * 6);
        Instant firstUsingForwards =
                getFirstSampleTSUsingIteration(startAtTime, BiDirectionalIterable.IterationDirection.FORWARDS);
        Instant firstUsingBackwards =
                getFirstSampleTSUsingIteration(startAtTime, BiDirectionalIterable.IterationDirection.BACKWARDS);
        Assertions.assertEquals(
                firstUsingForwards,
                firstUsingBackwards,
                "Forwards yields "
                        + TimeUtils.convertToHumanReadableString(firstUsingForwards)
                        + " Backwards yields "
                        + TimeUtils.convertToHumanReadableString(firstUsingForwards));
    }

    @Test
    public void makeSureWeGetTheLastEventInTheFile() throws IOException {
        PlainStoragePlugin storagePlugin = getStoragePlugin();

        try (BasicContext context = new BasicContext()) {
            Path path = PathNameUtility.getPathNameForTime(
                    storagePlugin,
                    pvName,
                    oneWeekIntoYear,
                    context.getPaths(),
                    configService.getPVNameToKeyConverter());
            // Start near the end of the year
            long startEpochSeconds = getStartOfCurrentYearInSeconds()
                    + 360L * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
            Instant startTime = convertFromEpochSeconds(startEpochSeconds, 0);
            Instant endTime = convertFromEpochSeconds(
                    startEpochSeconds + 20L * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(), 0);
            Event finalEvent = null;
            try (FileBackedPBEventStream stream =
                    new FileBackedPBEventStream(pvName, path, dbrType, startTime, endTime, false)) {
                boolean firstEvent = true;
                for (Event e : stream) {
                    if (firstEvent) {
                        Assertions.assertEquals(startTime, e.getEventTimeStamp());
                        firstEvent = false;
                    } else {
                        finalEvent = e.makeClone();
                    }
                }
            }

            Assertions.assertNotNull(finalEvent, "Final event is null");
            Instant finalSecondOfYear = getEndOfYear(getCurrentYear()).truncatedTo(ChronoUnit.SECONDS);
            Assertions.assertEquals(finalSecondOfYear, finalEvent.getEventTimeStamp());
        }
    }

    /**
     * This is Jud Gauden'z use case. We have a high rate PV (more than one event per second).
     * We then ask for data from the same second (start time and end time is the same second).
     * For this we generate data into a new PB file.
     * @throws IOException
     */
    @Test
    public void testHighRateEndLocation() throws IOException {
        PlainStoragePlugin highRatePlugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pluginString(
                        PB_PLUGIN_IDENTIFIER,
                        "localhost",
                        "name=FileBackedPBEventStreamTest&rootFolder=" + testFolder.getAbsolutePath()
                                + "&partitionGranularity=PARTITION_YEAR"),
                configService);
        String highRatePVName =
                ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":FileBackedPBEventStreamTestHighRate";
        int day = 60;
        int startofdayinseconds = day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
        try (BasicContext context = new BasicContext()) {
            short currentYear = getCurrentYear();
            ArrayListEventStream testData = new ArrayListEventStream(
                    PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                    new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, highRatePVName, currentYear));
            for (int secondintoday = 0;
                    secondintoday < PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                    secondintoday++) {
                // The value should be the secondsIntoYear integer divided by 600.
                // Add 10 events per second
                for (int i = 0; i < 10; i++) {
                    SimulationEvent sample = new SimulationEvent(
                            startofdayinseconds + secondintoday,
                            currentYear,
                            ArchDBRTypes.DBR_SCALAR_DOUBLE,
                            new ScalarValue<>((double) (((startofdayinseconds + secondintoday) / 600))));
                    sample.setNanos(i * 100);
                    testData.add(sample);
                }
            }
            assert highRatePlugin != null;
            highRatePlugin.appendData(context, highRatePVName, testData);
        }

        long requestEpochSeconds = getStartOfCurrentYearInSeconds()
                + startofdayinseconds
                + 12L * PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk();
        // Yes; start and end time are the same epochseconds. We can vary the nanos.
        Instant startTime = convertFromEpochSeconds(requestEpochSeconds, 0);
        Instant endTime = convertFromEpochSeconds(requestEpochSeconds, 999999999);

        try (BasicContext context = new BasicContext()) {
            Path path = PathNameUtility.getPathNameForTime(
                    highRatePlugin,
                    highRatePVName,
                    oneWeekIntoYear,
                    context.getPaths(),
                    configService.getPVNameToKeyConverter());
            try (FileBackedPBEventStream stream =
                    new FileBackedPBEventStream(highRatePVName, path, dbrType, startTime, endTime, false)) {
                boolean firstEvent = true;
                long eventCount = 0;
                long expectedEventCount = 10;
                for (Event e : stream) {
                    eventCount++;
                    if (firstEvent) {
                        Assertions.assertEquals(
                                e.getEventTimeStamp(),
                                startTime,
                                "The first event should be equal timestamp "
                                        + convertToISO8601String(startTime)
                                        + " got "
                                        + convertToISO8601String(e.getEventTimeStamp()));
                        firstEvent = false;
                    }
                }
                Assertions.assertEquals(expectedEventCount, eventCount);
            }
        }
    }
}
