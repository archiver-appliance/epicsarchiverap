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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/*
 * Test append into an existing PB file inside a gztar file.
 * This is testing something that the gztar plugin is not really intended for.
 * Very similar to the PBPlugin test except we use a month's granularity inside the tar file
 */
public class SeqAppendTest {
    private static final Logger logger = LogManager.getLogger();
    private static final String rootFolderStr = ConfigServiceForTests.getDefaultPBTestFolder() + "/gztar/SeqAppend";
    private static ConfigServiceForTests configService;

    @BeforeAll
    public static void setUp() throws Exception {
        File rootFolder = new File(rootFolderStr);
        FileUtils.deleteDirectory(rootFolder);
        Path pvPath = Paths.get(rootFolderStr, "epics/arch/gztartest");
        logger.debug("Creating folder {}", pvPath.getParent().toFile().toString());
        assert pvPath.getParent().toFile().mkdirs();
        configService = new ConfigServiceForTests(1);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        FileUtils.cleanDirectory(new File(rootFolderStr));
        FileUtils.deleteDirectory(new File(rootFolderStr));
    }

    private void appendAndTestForYear(String pvName, short forYear, int expectedCatalogEntryCount, int skipSeconds)
            throws Exception {
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=Test&rootFolder=" + URLEncoder.encode(ArchPaths.TAR_SCHEME + "://" + rootFolderStr, "UTF-8")
                        + "&partitionGranularity=PARTITION_MONTH",
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
        Map<String, TarEntry> entries = tarFile.loadCatalog();
        Assertions.assertTrue(
                entries.size() == expectedCatalogEntryCount,
                "Expecting " + expectedCatalogEntryCount + " entries; got " + entries.size() + " entries");
    }

    private void testRetrieval(String pvName, Instant start, Instant end, int expectedEventCount) throws Exception {
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=Test&rootFolder=" + URLEncoder.encode(ArchPaths.TAR_SCHEME + "://" + rootFolderStr, "UTF-8")
                        + "&partitionGranularity=PARTITION_MONTH",
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
                    logger.debug(strm.getDescription());
                    for (Event ev : strm) {
                        logger.debug(
                                "Found event at {} {}",
                                TimeUtils.convertToISO8601String(ev.getEventTimeStamp()),
                                TimeUtils.convertToHumanReadableString(ev.getEventTimeStamp()));
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

    static Stream<Arguments> pluginsAndPVs() {
        return Stream.of(
                Arguments.of("pb", "epics:arch:tartest:pb"), Arguments.of("parquet", "epics:arch:tartest:parquet"));
    }

    @ParameterizedTest
    @MethodSource("pluginsAndPVs")
    public void testAppendThruPlugin(String plugin, String pvName) throws Exception {
        String chunkKey = pvName.replace(":", File.separator);
        short currentYear = (short) (TimeUtils.getCurrentYear() - 1);
        appendAndTestForYear(pvName, currentYear, 12, 60);
        // Test retrieval
        Instant end = TimeUtils.getStartOfYear(currentYear).plusSeconds(24 * 60 * 60 * 32);
        for (int days = 1; days < 10; days++) {
            Instant start = end.minus(days, ChronoUnit.DAYS).minus(1, ChronoUnit.MILLIS);
            testRetrieval(pvName, start, end, (days * 24 * 60) + 1 + 1);
        }

        try (ArchPaths archPaths = new ArchPaths()) {
            EAATar tarFile = new EAATar(rootFolderStr + File.separator + chunkKey + ".tar");
            tarFile.optimize();
        }

        for (int days = 1; days < 10; days++) {
            Instant start = end.minus(days, ChronoUnit.DAYS).minus(1, ChronoUnit.MILLIS);
            testRetrieval(pvName, start, end, (days * 24 * 60) + 1 + 1);
        }
    }
}
