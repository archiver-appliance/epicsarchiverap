package org.epics.archiverappliance.utils.nio.tar;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.net.URLEncoder;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.Callable;

/*
 * Performance test. Generate a decades worth of 1Hz PV data into a gztar plugin and then measure the performance for data reqrieval.
 */
@Disabled("This can take a really long time; so this test is disabled by default.")
public class PerfTest {
    private static final Logger logger = LogManager.getLogger();
    private static final String pvName = "epics:arch:gztartest";
    private static final String chunkKey = pvName.replace(":", File.separator);
    private static final String rootFolderStr = ConfigServiceForTests.getDefaultPBTestFolder() + "/gztar/PerfTest";
    private static ConfigServiceForTests configService;
    private static final int numYears = 10;
    private static final int numDays = 60;
    private static final short currentYear = TimeUtils.getCurrentYear();

    @BeforeAll
    public static void setUp() throws Exception {
        File rootFolder = new File(rootFolderStr);
        FileUtils.deleteDirectory(rootFolder);
        Path pvPath = Paths.get(rootFolderStr, "epics/arch/gztartest");
        logger.debug("Creating folder {}", pvPath.getParent().toFile().toString());
        assert pvPath.getParent().toFile().mkdirs();
        configService = new ConfigServiceForTests(1);

        for (int y = numYears; y >= 0; y--) {
            appendAndTestForYear((short) (currentYear - y), 365 * (numYears - y + 1), 1);
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {
        configService.shutdownNow();
        FileUtils.cleanDirectory(new File(rootFolderStr));
        FileUtils.deleteDirectory(new File(rootFolderStr));
    }

    private static void appendAndTestForYear(short forYear, int expectedCatalogEntryCount, int skipSeconds) throws Exception {
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=Test&rootFolder="
                        + URLEncoder.encode(ArchPaths.TAR_SCHEME + "://" + rootFolderStr, "UTF-8")
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

        EAATar tarFile = new EAATar(rootFolderStr + File.separator + chunkKey + ".tar");
        Map<String, TarEntry> entries = tarFile.loadCatalog();
    }

    private int testRetrieval(Instant start, Instant end, int expectedEventCount) throws Exception {
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=Test&rootFolder="
                        + URLEncoder.encode(ArchPaths.TAR_SCHEME + "://" + rootFolderStr, "UTF-8")
                        + "&partitionGranularity=PARTITION_DAY",
                configService);
        logger.debug(
                "Looking for data between {} and {}",
                TimeUtils.convertToISO8601String(start),
                TimeUtils.convertToISO8601String(end));
        int eventCount = 0;
        try (BasicContext context = new BasicContext()) {
            for (Callable<EventStream> callable :
                    storagePlugin.getDataForPV(context, pvName, start, end, new DefaultRawPostProcessor())) {
                try (EventStream strm = callable.call()) {
                    for (Event ev : strm) {
                        // logger.debug("Found event at {} {}",
                        // TimeUtils.convertToISO8601String(ev.getEventTimeStamp()),
                        // TimeUtils.convertToHumanReadableString(ev.getEventTimeStamp()));
                        eventCount++;
                    }
                }
            }
            logger.debug("Done counting events");
        }
        return eventCount;
    }

    @Test
    public void testTarPerformance() throws Exception {
        Instant end = TimeUtils.getStartOfYear(currentYear).plusSeconds(24 * 60 * 60 * 90);
        testRetrieval(end.minus(1, ChronoUnit.DAYS), end, (1 * 24 * 60 * 60) + 1 + 1); // Precompile
        for (int days = 1; days < numDays; days++) {
            Instant start = end.minus(days, ChronoUnit.DAYS);
            long before = System.currentTimeMillis();
            int events = testRetrieval(
                    start,
                    end,
                    (days * 24 * 60 * 60)
                            + 1
                            + 1); // One for the last known event and one for the event that's exactly at the end time.
            long after = System.currentTimeMillis();
            System.out.println(
                    "Took " + (after - before) + "(ms) to retrieve " + events + " events for " + days + " days");
        }
    }
}
