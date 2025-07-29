package edu.stanford.slac.archiverappliance.PlainPB;

import edu.stanford.slac.archiverappliance.plain.FileStreamCreator;
import edu.stanford.slac.archiverappliance.plain.PBCompressionMode;
import edu.stanford.slac.archiverappliance.plain.PBFileInfo;
import edu.stanford.slac.archiverappliance.plain.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.plain.PlainPBStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Random;

import static edu.stanford.slac.archiverappliance.plain.PlainPBStoragePlugin.pbFileExtension;
import static edu.stanford.slac.archiverappliance.plain.PlainPBStoragePlugin.pbFileSuffix;

/**
 * Test the PlainPB Event stream when we have unexpected garbage in the data.
 * @author mshankar
 *
 */
@Tag("flaky")
public class ZeroedFileEventStreamTest {
    private static final Logger logger = LogManager.getLogger(ZeroedFileEventStreamTest.class.getName());
    static short currentYear = TimeUtils.getCurrentYear();
    static ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
    static Instant startTime = TimeUtils.getStartOfYear(currentYear);
    static Instant endTime = TimeUtils.getStartOfYear(currentYear + 1);
    static int defaultPeriodInSeconds = PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() / 10;
    private static ConfigService configService;
    String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ZeroedFileEventStreamTest/";
    String pvNamePrefix = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ZeroedFileEventStreamTest";

    private static int generateFreshData(PlainPBStoragePlugin pbplugin4data, String pvName) throws Exception {
        File rootFolder = new File(pbplugin4data.getRootFolder());
        if (rootFolder.exists()) {
            try {
                FileUtils.deleteDirectory(rootFolder);
            } catch (IOException e) {
                logger.error(e);
                Assertions.fail();
            }
        }

        try (BasicContext context = new BasicContext()) {
            return pbplugin4data.appendData(
                    context,
                    pvName,
                    new SimulationEventStream(
                            type,
                            (type, secondsIntoYear) -> new ScalarValue<>(1.0),
                            startTime,
                            endTime,
                            defaultPeriodInSeconds));
        }
    }

    @BeforeAll
    public static void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
    }

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(rootFolderName));
        FileUtils.deleteDirectory(new File(rootFolderName + "Dest"));
    }

    /**
     * Generate PB file with bad footers and then see if we survive PBFileInfo.
     */
    @Test
    public void testBadFooters() throws Exception {
        logger.info("Testing garbage in the last record");
        PlainPBStoragePlugin pbplugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pbFileSuffix + "://localhost?name=STS&rootFolder=" + rootFolderName
                        + "&partitionGranularity=PARTITION_YEAR",
                configService);

        logger.info("Testing garbage in the last record");
        String pvName = pvNamePrefix + "testBadFooters";

        generateFreshData(pbplugin, pvName);
        Path[] paths = null;
        try (BasicContext context = new BasicContext()) {
            paths = PlainPBPathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    rootFolderName,
                    pvName,
                    pbFileExtension,
                    PartitionGranularity.PARTITION_YEAR,
                    PBCompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }

        Assertions.assertTrue(
                paths.length > 0, "Cannot seem to find any plain pb files in " + rootFolderName + " for pv " + pvName);

        // Overwrite the tail end of each file with some garbage.
        for (Path path : paths) {
            try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
                // Seek to somewhere at the end
                int bytesToOverwrite = 100;
                channel.position(channel.size() - bytesToOverwrite);
                ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
                byte[] junk = new byte[bytesToOverwrite];
                new Random().nextBytes(junk);
                buf.put(junk);
                buf.flip();
                channel.write(buf);
            } catch (IOException e) {
                logger.error(e);
                Assertions.fail();
            }

            PBFileInfo info = new PBFileInfo(path);
            Assertions.assertNotNull(info, "Cannot generate PBFileInfo from " + path);
            Assertions.assertEquals(
                    info.getPVName(), pvName, "pvNames are different " + info.getPVName() + " expecting " + pvName);
            Assertions.assertNotNull(info.getLastEvent(), "Last event is null");
            Instant lastEventTs = info.getLastEvent().getEventTimeStamp();
            logger.info(TimeUtils.convertToHumanReadableString(lastEventTs));
            Assertions.assertTrue(
                    lastEventTs.isAfter(TimeUtils.convertFromISO8601String(currentYear + "-12-30T00:00:00.000Z"))
                            && lastEventTs.isBefore(
                                    TimeUtils.convertFromISO8601String(currentYear + 1 + "-01-01T00:00:00.000Z")),
                    "Last event is incorrect " + TimeUtils.convertToHumanReadableString(lastEventTs));
            try (EventStream strm = FileStreamCreator.getStream(pvName, path, type)) {
                long eventCount = 0;
                for (@SuppressWarnings("unused") Event e : strm) {
                    eventCount++;
                }
                Assertions.assertTrue(eventCount > 365, "Event count is too low " + eventCount);
            }
        }
    }

    /**
     * Generate PB file with bad footers in the ETL source and then see if we survive ETL
     */
    @Test
    public void testBadFootersInSrcETL() throws Exception {
        PlainPBStoragePlugin srcPlugin = null;
        try {
            srcPlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                    pbFileSuffix + "://localhost?name=STS&rootFolder=" + rootFolderName
                            + "&partitionGranularity=PARTITION_MONTH",
                    configService);
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }
        String pvName = pvNamePrefix + "testBadFootersInSrcETL";
        assert srcPlugin != null;
        int generatedCount = generateFreshData(srcPlugin, pvName);
        Path[] paths = null;
        try (BasicContext context = new BasicContext()) {
            paths = PlainPBPathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    rootFolderName,
                    pvName,
                    pbFileExtension,
                    PartitionGranularity.PARTITION_YEAR,
                    PBCompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }

        Assertions.assertTrue(
                paths.length > 0, "Cannot seem to find any plain pb files in " + rootFolderName + " for pv " + pvName);

        // Overwrite the tail end of each file with some garbage.
        for (Path path : paths) {
            try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
                // Seek to somewhere at the end
                int bytesToOverwrite = 100;
                channel.position(channel.size() - bytesToOverwrite);
                ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
                byte[] junk = new byte[bytesToOverwrite];
                new Random().nextBytes(junk);
                buf.put(junk);
                buf.flip();
                channel.write(buf);
            } catch (IOException e) {
                logger.error(e);
                Assertions.fail();
            }
        }

        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores = new String[] {
            srcPlugin.getURLRepresentation(),
            pbFileSuffix + "://localhost?name=STS&rootFolder=" + rootFolderName + "Dest"
                    + "&partitionGranularity=PARTITION_YEAR"
        };
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        try {
            configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        } catch (AlreadyRegisteredException e) {
            logger.error(e);
            Assertions.fail();
        }
        configService.getETLLookup().manualControlForUnitTests();

        Instant timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 366);
        ZonedDateTime ts = ZonedDateTime.now(ZoneId.from(ZoneOffset.UTC));
        if (ts.getMonth().getValue() == 1) {
            // This means that we never test this in Jan but I'd rather have the null check than skip this.
            timeETLruns = TimeUtils.plusDays(timeETLruns, 35);
        }
        try {
            ETLExecutor.runETLs(configService, timeETLruns);
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }
        logger.info("Done performing ETL");

        paths = null;
        try (BasicContext context = new BasicContext()) {
            paths = PlainPBPathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    rootFolderName + "Dest",
                    pvName,
                    pbFileExtension,
                    PartitionGranularity.PARTITION_YEAR,
                    PBCompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }

        Assertions.assertTrue(paths != null && paths.length > 0, "ETL did not seem to move any data?");

        long eventCount = 0;
        for (Path path : paths) {
            PBFileInfo info = new PBFileInfo(path);
            Assertions.assertNotNull(info, "Cannot generate PBFileInfo from " + path);
            Assertions.assertEquals(
                    info.getPVName(), pvName, "pvNames are different " + info.getPVName() + " expecting " + pvName);
            Assertions.assertNotNull(info.getLastEvent(), "Last event is null");
            Instant lastEventTs = info.getLastEvent().getEventTimeStamp();
            logger.info(TimeUtils.convertToHumanReadableString(lastEventTs));
            Assertions.assertTrue(
                    lastEventTs.isAfter(TimeUtils.convertFromISO8601String(currentYear + "-12-30T00:00:00.000Z"))
                            && lastEventTs.isBefore(
                                    TimeUtils.convertFromISO8601String(currentYear + 1 + "-01-01T00:00:00.000Z")),
                    "Last event is incorrect " + TimeUtils.convertToHumanReadableString(lastEventTs));
            try (EventStream strm = FileStreamCreator.getStream(pvName, path, type)) {
                for (@SuppressWarnings("unused") Event e : strm) {
                    eventCount++;
                }
            } catch (IOException e) {
                logger.error(e);
                Assertions.fail();
            }
        }
        int expectedEventCount = generatedCount - (int) ((float) generatedCount * 0.15);
        Assertions.assertTrue(
                eventCount >= expectedEventCount,
                "Event count is too low " + eventCount + " expecting at least " + expectedEventCount);
    }

    /**
     * Generate PB file with bad footers in the ETL dest and then see if we survive ETL
     */
    @Test
    public void testBadFootersInDestETL() throws Exception {
        String pvName = pvNamePrefix + "testBadFootersInDestETL";

        PlainPBStoragePlugin destPlugin = null;
        try {
            destPlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                    pbFileSuffix + "://localhost?name=STS&rootFolder=" + rootFolderName + "Dest"
                            + "&partitionGranularity=PARTITION_YEAR",
                    configService);
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }
        assert destPlugin != null;
        File destFolder = new File(destPlugin.getRootFolder());
        if (destFolder.exists()) {
            try {
                FileUtils.deleteDirectory(destFolder);
            } catch (IOException e) {
                logger.error(e);
                Assertions.fail();
            }
        }

        PlainPBStoragePlugin srcPlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pbFileSuffix + "://localhost?name=STS&rootFolder=" + rootFolderName
                        + "&partitionGranularity=PARTITION_MONTH",
                configService);
        File srcFolder = new File(srcPlugin.getRootFolder());
        if (srcFolder.exists()) {
            try {
                FileUtils.deleteDirectory(srcFolder);
            } catch (IOException e) {
                logger.error(e);
                Assertions.fail();
            }
        }

        long generatedCount = 0;
        try (BasicContext context = new BasicContext()) {
            for (int day = 0; day < 180; day++) { // Generate data for half the year...
                ArrayListEventStream testData = new ArrayListEventStream(
                        PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                        new RemotableEventStreamDesc(type, pvName, currentYear));
                int startofdayinseconds = day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                for (int secondintoday = 0;
                        secondintoday < PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                        secondintoday += defaultPeriodInSeconds) {
                    testData.add(new SimulationEvent(
                            startofdayinseconds + secondintoday, currentYear, type, new ScalarValue<Double>((double)
                                    secondintoday)));
                }
                generatedCount = generatedCount + destPlugin.appendData(context, pvName, testData);
            }
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }

        Path[] paths = null;
        try (BasicContext context = new BasicContext()) {
            paths = PlainPBPathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    destPlugin.getRootFolder(),
                    pvName,
                    pbFileExtension,
                    destPlugin.getPartitionGranularity(),
                    PBCompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }

        Assertions.assertTrue(
                paths.length > 0,
                "Cannot seem to find any plain pb files in " + destPlugin.getRootFolder() + " for pv " + pvName);

        // Overwrite the tail end of each file with some garbage.
        for (Path path : paths) {
            try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
                // Seek to somewhere at the end
                int bytesToOverwrite = 100;
                channel.position(channel.size() - bytesToOverwrite);
                ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
                byte[] junk = new byte[bytesToOverwrite];
                new Random().nextBytes(junk);
                buf.put(junk);
                buf.flip();
                channel.write(buf);
            } catch (IOException e) {
                logger.error(e);
                Assertions.fail();
            }
        }

        try (BasicContext context = new BasicContext()) {
            for (int day = 180; day < 365; day++) { // Generate data for the remaining half
                ArrayListEventStream testData = new ArrayListEventStream(
                        PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                        new RemotableEventStreamDesc(type, pvName, currentYear));
                int startofdayinseconds = day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                for (int secondintoday = 0;
                        secondintoday < PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                        secondintoday += defaultPeriodInSeconds) {
                    testData.add(new SimulationEvent(
                            startofdayinseconds + secondintoday, currentYear, type, new ScalarValue<Double>((double)
                                    secondintoday)));
                }
                generatedCount = generatedCount + srcPlugin.appendData(context, pvName, testData);
            }
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }

        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores = new String[] {srcPlugin.getURLRepresentation(), destPlugin.getURLRepresentation()};
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        try {
            configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        } catch (AlreadyRegisteredException e) {
            logger.error(e);
            Assertions.fail();
        }
        configService.getETLLookup().manualControlForUnitTests();

        Instant timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 366);
        ZonedDateTime ts = ZonedDateTime.now(ZoneId.from(ZoneOffset.UTC));
        if (ts.getMonth() == Month.JANUARY) {
            // This means that we never test this in Jan but I'd rather have the null check than skip this.
            timeETLruns = TimeUtils.plusDays(timeETLruns, 35);
        }
        try {
            ETLExecutor.runETLs(configService, timeETLruns);
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }
        logger.info("Done performing ETL");

        paths = null;
        try (BasicContext context = new BasicContext()) {
            paths = PlainPBPathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    destPlugin.getRootFolder(),
                    pvName,
                    pbFileExtension,
                    PartitionGranularity.PARTITION_YEAR,
                    PBCompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }

        Assertions.assertTrue(paths.length > 0, "ETL did not seem to move any data?");

        long eventCount = 0;
        for (Path path : paths) {
            PBFileInfo info = null;
            try {
                info = new PBFileInfo(path);
            } catch (IOException e) {
                logger.error(e);
                Assertions.fail();
            }
            Assertions.assertNotNull(info, "Cannot generate PBFileInfo from " + path);
            Assertions.assertEquals(
                    info.getPVName(), pvName, "pvNames are different " + info.getPVName() + " expecting " + pvName);
            Assertions.assertNotNull(info.getLastEvent(), "Last event is null");
            Instant lastEventTs = info.getLastEvent().getEventTimeStamp();
            logger.info(TimeUtils.convertToHumanReadableString(lastEventTs));
            Assertions.assertTrue(
                    lastEventTs.isAfter(TimeUtils.convertFromISO8601String(currentYear + "-12-30T00:00:00.000Z"))
                            && lastEventTs.isBefore(
                                    TimeUtils.convertFromISO8601String(currentYear + 1 + "-01-01T00:00:00.000Z")),
                    "Last event is incorrect " + TimeUtils.convertToHumanReadableString(lastEventTs));
            try (EventStream strm = FileStreamCreator.getStream(pvName, path, type)) {
                for (@SuppressWarnings("unused") Event e : strm) {
                    eventCount++;
                }
            } catch (IOException e) {
                logger.error(e);
                Assertions.fail();
            }
        }
        long expectedEventCount = generatedCount - (long) ((float) generatedCount * 0.15);
        Assertions.assertTrue(
                eventCount >= expectedEventCount,
                "Event count is too low " + eventCount + " expecting at least " + expectedEventCount);
    }

    /**
     * Generate PB file with bad footers and then see if we survive retrieval
     * @throws Exception
     */
    @Test
    public void testBadFootersRetrieval() throws Exception {
        String pvName = pvNamePrefix + "testBadFootersRetrieval";
        PlainPBStoragePlugin pbplugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pbFileSuffix + "://localhost?name=STS&rootFolder=" + rootFolderName
                        + "&partitionGranularity=PARTITION_YEAR",
                configService);
        assert pbplugin != null;

        int generatedCount = generateFreshData(pbplugin, pvName);
        Path[] paths = null;
        try (BasicContext context = new BasicContext()) {
            paths = PlainPBPathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    rootFolderName,
                    pvName,
                    pbFileExtension,
                    PartitionGranularity.PARTITION_YEAR,
                    PBCompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }

        Assertions.assertTrue(
                paths.length > 0, "Cannot seem to find any plain pb files in " + rootFolderName + " for pv " + pvName);

        // Overwrite the tail end of each file with some garbage.
        for (Path path : paths) {
            try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
                // Seek to somewhere at the end
                int bytesToOverwrite = 100;
                channel.position(channel.size() - bytesToOverwrite);
                ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
                byte[] junk = new byte[bytesToOverwrite];
                new Random().nextBytes(junk);
                buf.put(junk);
                buf.flip();
                channel.write(buf);
            } catch (IOException e) {
                logger.error(e);
                Assertions.fail();
            }
        }

        Instant start = TimeUtils.convertFromISO8601String(currentYear + "-03-01T00:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String(currentYear + "-04-01T00:00:00.000Z");
        long exepectedEventCount = (Duration.between(start, end).toSeconds() / defaultPeriodInSeconds) - 1;
        try (BasicContext context = new BasicContext();
                EventStream result = new CurrentThreadWorkerEventStream(
                        pvName, pbplugin.getDataForPV(context, pvName, start, end))) {
            long eventCount = 0;
            for (@SuppressWarnings("unused") Event e : result) {
                eventCount++;
            }
            Assertions.assertTrue(
                    eventCount >= exepectedEventCount,
                    "Event count is too low " + eventCount + " expecting " + exepectedEventCount
                            + " from total generated " + generatedCount);
        } catch (IOException e) {
            logger.error(e);
            Assertions.fail();
        }
    }

    /**
     * Generate PB file with zeroes at random places and then see if we survive retrieval
     * @throws Exception
     */
    @Test
    public void testZeroedDataRetrieval() throws Exception {
        PlainPBStoragePlugin pbplugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pbFileSuffix + "://localhost?name=STS&rootFolder=" + rootFolderName
                        + "&partitionGranularity=PARTITION_YEAR",
                configService);
        String pvName = pvNamePrefix + "testZeroedDataRetrieval";

        int generatedCount = generateFreshData(pbplugin, pvName);
        Path[] paths = null;
        try (BasicContext context = new BasicContext()) {
            paths = PlainPBPathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    rootFolderName,
                    pvName,
                    pbFileExtension,
                    PartitionGranularity.PARTITION_YEAR,
                    PBCompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
        } catch (IOException e) {
            logger.warn(e);
            Assertions.fail();
        }

        Assertions.assertTrue(
                paths.length > 0, "Cannot seem to find any plain pb files in " + rootFolderName + " for pv " + pvName);

        // Overwrite some lines in the file at random places.
        int zeroedLines = 100;
        Random random = new Random();
        for (Path path : paths) {
            try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
                for (int i = 0; i < zeroedLines; i++) {
                    int bytesToOverwrite = 10;
                    // Seek to a random spot after the first line
                    long randomSpot = 512 + (long) ((channel.size() - 512) * random.nextFloat());
                    channel.position(randomSpot - bytesToOverwrite);
                    ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
                    byte[] junk = new byte[bytesToOverwrite];
                    new Random().nextBytes(junk);
                    buf.put(junk);
                    buf.flip();
                    channel.write(buf);
                }
            } catch (IOException e) {
                logger.warn(e);
                Assertions.fail();
            }
        }

        Instant start = TimeUtils.convertFromISO8601String(currentYear + "-03-01T00:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String(currentYear + "-04-01T00:00:00.000Z");
        long exepectedEventCount = (Duration.between(start, end).toSeconds() / defaultPeriodInSeconds) - 1;

        try (BasicContext context = new BasicContext();
                EventStream result = new CurrentThreadWorkerEventStream(
                        pvName, pbplugin.getDataForPV(context, pvName, start, end))) {
            long eventCount = 0;
            for (@SuppressWarnings("unused") Event e : result) {
                eventCount++;
            }
            // There is really no right answer here. We should not lose too many points because of the zeroing....
            Assertions.assertTrue(
                    Math.abs(eventCount - exepectedEventCount) < zeroedLines * 3,
                    "Event count is too low " + eventCount + " expecting approximately " + exepectedEventCount);
        } catch (IOException e) {
            logger.warn(e);
            Assertions.fail();
        }
    }
}
