package org.epics.archiverappliance.utils.nio.tar;

import edu.stanford.slac.archiverappliance.plain.PathNameUtility;
import edu.stanford.slac.archiverappliance.plain.PathResolver;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/*
 * Create gztar files thru the PlainPBStoragePlugin.
 */
public class PBPluginTest {
    private static final Logger logger = LogManager.getLogger();
    private static final String rootFolderStr = ConfigServiceForTests.getDefaultPBTestFolder() + "/gztar/PBPluginTest";
    private static ConfigServiceForTests configService;

    @BeforeAll
    public static void setUp() throws Exception {
        File rootFolder = new File(rootFolderStr);
        FileUtils.deleteDirectory(rootFolder);
        String chunkKey = "epics:arch:gztartest:pb".replace(":", File.separator);
        Path pvPath = Paths.get(rootFolderStr, chunkKey);
        logger.debug("Creating folder {}", pvPath.getParent().toFile().toString());
        assert pvPath.getParent().toFile().mkdirs();
        configService = new ConfigServiceForTests(1);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        FileUtils.cleanDirectory(new File(rootFolderStr));
        FileUtils.deleteDirectory(new File(rootFolderStr));
    }

    static Stream<Arguments> pluginsAndPVs() {
        return Stream.of(
                Arguments.of("pb", "epics:arch:tartest:pb"), Arguments.of("parquet", "epics:arch:tartest:parquet"));
    }

    @ParameterizedTest
    @MethodSource("pluginsAndPVs")
    public void testAppendThruPlugin(String plugin, String pvName) throws Exception {
        String chunkKey = pvName.replace(":", File.separator);
        short currentYear = TimeUtils.getCurrentYear();
        for (int y = 3; y >= 0; y--) {
            appendAndTestForYear(pvName, (short) (currentYear - y), 365 * (3 - y + 1), 60);
        }

        Files.walk(Paths.get(rootFolderStr))
                .forEach(p -> logger.debug("---- " + p.toAbsolutePath().toString()));
        EAATar tarFile = new EAATar(rootFolderStr + File.separator + chunkKey + ".tar");
        logger.debug("Loading catalog for {}", tarFile);
        Map<String, TarEntry> entries = tarFile.loadCatalog();
        List<String> entryNames = new LinkedList<String>(entries.keySet());
        Collections.sort(entryNames);
        Assertions.assertTrue(
                entryNames.size() == 4 * 365, "Expecting " + 4 * 365 + " entries; got " + entries.size() + " entries");
        // for(String entryName : entryNames) {
        //     logger.debug("Entry {}", entryName);
        // }

        Files.walk(Paths.get(rootFolderStr))
                .forEach(p -> logger.debug("---- " + p.toAbsolutePath().toString()));

        // Test retrieval
        Instant end = TimeUtils.getStartOfYear(currentYear - 1).plusSeconds(24 * 60 * 60 * (365 / 2));
        for (int days = 1; days < 60; days++) {
            Instant start = end.minus(days, ChronoUnit.DAYS);
            long before = System.currentTimeMillis();
            testRetrieval(
                    pvName,
                    start,
                    end,
                    (days * 24 * 60)
                            + 1
                            + 1); // One for the last known event and one for the event that's exactly at the end time.
            long after = System.currentTimeMillis();
            logger.debug("Took {}(ms) to retrieve data for {} days", (after - before), days);
        }

        // Before update
        end = TimeUtils.getStartOfYear(currentYear).plusSeconds(24 * 60 * 60 * 32);
        for (int days = 1; days < 10; days++) {
            Instant start = end.minus(days, ChronoUnit.DAYS);
            testRetrieval(pvName, start, end, (days * 24 * 60) + 1 + 1);
        }

        // Delete the files that we are updating ( otherwise we'll be appending and we'll get the wrong result )
        logger.debug("Deleting some files now");
        try (ArchPaths archPaths = new ArchPaths()) {
            String currYearStr = Short.toString(currentYear);
            for (Path path : PathNameUtility.getAllPathsForPV(
                    archPaths,
                    URIUtils.TAR_SCHEME + "://" + rootFolderStr,
                    pvName,
                    ".pb",
                    PathResolver.BASE_PATH_RESOLVER,
                    configService.getPVNameToKeyConverter())) {
                if (path.toString().contains(currYearStr)) {
                    logger.debug("Deleting {}", path.toString());
                    Files.delete(path);
                }
            }
        }
        logger.debug("Done deleting some files now");

        logger.info("Updating some PB files now");
        appendAndTestForYear(pvName, currentYear, 365 * 4, 120);

        // After update
        end = TimeUtils.getStartOfYear(currentYear).plusSeconds(24 * 60 * 60 * 32);
        for (int days = 1; days < 10; days++) {
            Instant start = end.minus(days, ChronoUnit.DAYS);
            testRetrieval(
                    pvName,
                    start,
                    end,
                    ((days * 24 * 60) / 2)
                            + 1
                            + 1); // We generate data once every two minutes; so we should halve the number of samples
        }
    }

    private void appendAndTestForYear(String pvName, short forYear, int expectedCatalogEntryCount, int skipSeconds)
            throws Exception {
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=Test&rootFolder="
                        + URLEncoder.encode(URIUtils.TAR_SCHEME + "://" + rootFolderStr, "UTF-8")
                        + "&partitionGranularity=PARTITION_DAY",
                configService);
        try (BasicContext context = new BasicContext()) {
            short year = forYear;
            for (int day = 0; day < 365; day++) {
                ArrayListEventStream testData = new ArrayListEventStream(
                        24 * 60 * 60, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
                int startofdayinseconds = day * 24 * 60 * 60;
                for (int secondintoday = 0; secondintoday < 24 * 60 * 60; secondintoday += skipSeconds) {
                    Instant dataTs = TimeUtils.convertFromYearSecondTimestamp(
                            new YearSecondTimestamp(forYear, startofdayinseconds + secondintoday, 0));
                    testData.add(new POJOEvent(
                                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                    dataTs,
                                    new ScalarValue<Long>(dataTs.getEpochSecond()),
                                    0,
                                    0)
                            .makeClone());
                }
                storagePlugin.appendData(context, pvName, testData);
            }
        }

        String chunkKey = pvName.replace(":", File.separator);

        EAATar tarFile = new EAATar(rootFolderStr + File.separator + chunkKey + ".tar");
        logger.debug("Loading catalog for {}", tarFile);
        Map<String, TarEntry> entries = tarFile.loadCatalog();
        Assertions.assertTrue(
                entries.size() == expectedCatalogEntryCount,
                "Expecting " + expectedCatalogEntryCount + " entries; got " + entries.size() + " entries");
    }

    private void testRetrieval(String pvName, Instant start, Instant end, int expectedEventCount) throws Exception {
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=Test&rootFolder="
                        + URLEncoder.encode(URIUtils.TAR_SCHEME + "://" + rootFolderStr, "UTF-8")
                        + "&partitionGranularity=PARTITION_DAY",
                configService);
        logger.debug(
                "Looking for data between {} and {}",
                TimeUtils.convertToISO8601String(start),
                TimeUtils.convertToISO8601String(end));
        try (BasicContext context = new BasicContext()) {
            int eventCount = 0;
            for (Callable<EventStream> callable :
                    storagePlugin.getDataForPV(context, pvName, start, end, new DefaultRawPostProcessor())) {
                try (EventStream strm = callable.call()) {
                    for (Event ev : strm) {
                        // logger.debug("Found event at {} {}",
                        // TimeUtils.convertToISO8601String(ev.getEventTimeStamp()),
                        // TimeUtils.convertToHumanReadableString(ev.getEventTimeStamp()));
                        Assertions.assertEquals(
                                ev.getEventTimeStamp().getEpochSecond(),
                                ev.getSampleValue().getValue().doubleValue());
                        eventCount++;
                    }
                }
            }
            logger.debug("Done counting events");
            Assertions.assertTrue(
                    Math.abs(eventCount - expectedEventCount) < 2,
                    "Expecting " + expectedEventCount + " events; got "
                            + eventCount + " events" + " when looking from "
                            + TimeUtils.convertToISO8601String(start) + " to " + TimeUtils.convertToISO8601String(end));
        }
    }
}
