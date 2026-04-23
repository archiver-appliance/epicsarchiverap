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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.Callable;

/*
 * Test the storage plugin's rename function
 */
public class RenameTest {
    private static final Logger logger = LogManager.getLogger();
    private static final String rootFolderStr = ConfigServiceForTests.getDefaultPBTestFolder() + "/gztar/RenameTest";
    private static final String pvNameOld = "epics:arch:gztartest";
    private static final String chunkKeyOld = pvNameOld.replace(":", File.separator);
    private static final String pvNameNew = "epics:arch:gztartest:renamed";
    private static final String chunkKeyNew = pvNameNew.replace(":", File.separator);
    private static ConfigServiceForTests configService;

    @BeforeAll
    public static void setUp() throws Exception {
        File rootFolder = new File(rootFolderStr);
        FileUtils.deleteDirectory(rootFolder);
        Path pvPath = Paths.get(rootFolderStr, chunkKeyOld);
        logger.debug("Creating folder {}", pvPath.getParent().toFile().toString());
        assert pvPath.getParent().toFile().mkdirs();
        configService = new ConfigServiceForTests(1);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        FileUtils.cleanDirectory(new File(rootFolderStr));
        FileUtils.deleteDirectory(new File(rootFolderStr));
    }

    private void appendAndTestForYear(short forYear, int expectedCatalogEntryCount, int skipSeconds) throws Exception {
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=Test&rootFolder=" + URLEncoder.encode(ArchPaths.TAR_SCHEME + "://" + rootFolderStr, "UTF-8")
                        + "&partitionGranularity=PARTITION_DAY",
                configService);
        try (BasicContext context = new BasicContext()) {
            short year = forYear;
            for (int day = 0; day < 365; day++) {
                ArrayListEventStream testData = new ArrayListEventStream(
                        24 * 60 * 60, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvNameOld, year));
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
                storagePlugin.appendData(context, pvNameOld, testData);
            }
        }

        EAATar tarFile = new EAATar(rootFolderStr + File.separator + chunkKeyOld + ".tar");
        Map<String, TarEntry> entries = tarFile.loadCatalog();
        Assertions.assertTrue(
                entries.size() == expectedCatalogEntryCount,
                "Expecting " + expectedCatalogEntryCount + " entries; got " + entries.size() + " entries");
    }

    private void testRetrieval(String pvName, Instant start, Instant end, int expectedEventCount) throws Exception {
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=Test&rootFolder=" + URLEncoder.encode(ArchPaths.TAR_SCHEME + "://" + rootFolderStr, "UTF-8")
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
                    eventCount == expectedEventCount,
                    "Expecting " + expectedEventCount + " events; got " + eventCount + " events");
        }
    }

    private void renamePV() throws IOException {
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=Test&rootFolder=" + URLEncoder.encode(ArchPaths.TAR_SCHEME + "://" + rootFolderStr, "UTF-8")
                        + "&partitionGranularity=PARTITION_DAY",
                configService);
        try (BasicContext context = new BasicContext()) {
            storagePlugin.renamePV(context, pvNameOld, pvNameNew);
        }
    }

    @Test
    public void testAppendThruPlugin() throws Exception {
        short currentYear = TimeUtils.getCurrentYear();
        appendAndTestForYear(currentYear, 365, 60);

        // Test retrieval before rename
        Instant end = TimeUtils.getStartOfYear(currentYear).plusSeconds(24 * 60 * 60 * 32);
        for (int days = 1; days < 5; days++) {
            Instant start = end.minus(days, ChronoUnit.DAYS);
            long before = System.currentTimeMillis();
            testRetrieval(pvNameOld, start, end, (days * 24 * 60) + 1);
            long after = System.currentTimeMillis();
            logger.debug("Took {}(ms) to retrieve data for {} days", (after - before), days);
        }

        for (int days = 1; days < 5; days++) {
            Instant start = end.minus(days, ChronoUnit.DAYS);
            long before = System.currentTimeMillis();
            testRetrieval(pvNameNew, start, end, 0);
            long after = System.currentTimeMillis();
            logger.debug("Took {}(ms) to retrieve data for {} days", (after - before), days);
        }

        renamePV();

        // Test retrieval before rename
        // Rename does not delete data under the older name
        for (int days = 1; days < 5; days++) {
            Instant start = end.minus(days, ChronoUnit.DAYS);
            long before = System.currentTimeMillis();
            testRetrieval(pvNameOld, start, end, (days * 24 * 60) + 1);
            long after = System.currentTimeMillis();
            logger.debug("Took {}(ms) to retrieve data for {} days", (after - before), days);
        }

        for (int days = 1; days < 5; days++) {
            Instant start = end.minus(days, ChronoUnit.DAYS);
            long before = System.currentTimeMillis();
            testRetrieval(pvNameNew, start, end, (days * 24 * 60) + 1);
            long after = System.currentTimeMillis();
            logger.debug("Took {}(ms) to retrieve data for {} days", (after - before), days);
        }
    }
}
