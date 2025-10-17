package edu.stanford.slac.archiverappliance.plain;

import static org.epics.archiverappliance.utils.ui.URIUtils.pluginString;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Test data retrieval around the partition boundaries.
 * We use daily partitions; create data for two days ( 1Hz ) but leave large gaps in between.
 * We then ask for minutely data and make sure we always get data when the start time is >= the timestamp of the very first sample.
 * @author mshankar
 *
 */
public class DataAroundPartitionEndTest {
    private static final Logger logger = LogManager.getLogger(DataAroundPartitionEndTest.class.getName());
    private static final File testFolder =
            new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "DataAroundPartitionEndTest");
    private static final String pvName = "DataAroundPartitionEndTest";
    private static final ConfigService configService;

    static {
        try {
            configService = new ConfigServiceForTests(-1);
        } catch (ConfigException e) {
            throw new RuntimeException(e);
        }
    }

    private static final short dataYear = (short) (TimeUtils.getCurrentYear() - 1);
    private static final Instant generatedEndDate = Instant.parse(dataYear + "-06-01T00:00:00.00Z");
    private static final Instant generatedStartDate = generatedEndDate.minus(2, ChronoUnit.DAYS);

    private static Path filePath(PlainStorageType plainStorageType, String timestamp) {
        return Paths.get(
                testFolder.getAbsolutePath(),
                pvName.replace(":", "/").replace("--", "")
                        + ':'
                        + timestamp
                        + plainStorageType.plainFileHandler().getExtensionString());
    }

    private static void generateData(PlainStorageType plainStorageType) throws Exception {

        PlainStoragePlugin storagePlugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pluginString(
                        plainStorageType,
                        "localhost",
                        "name=DataAroundPartitionEndTest&rootFolder=" + testFolder.getAbsolutePath()
                                + "&partitionGranularity=PARTITION_DAY"),
                DataAroundPartitionEndTest.configService);
        ArrayListEventStream strm = new ArrayListEventStream(
                PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 3,
                new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, dataYear));

        Instant ts = generatedStartDate;
        int sampleCount = 1;
        while (ts.isBefore(generatedEndDate)) {
            if (sampleCount % 3600 == 0) {
                sampleCount = 1;
                ts = ts.plus(1, ChronoUnit.DAYS);
            } else {
                ts = ts.plus(1, ChronoUnit.SECONDS);
            }

            strm.add(new POJOEvent(
                    ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<>((double) ts.getEpochSecond()), 0, 0));
            sampleCount++;
            logger.debug("Inserting data at {}", TimeUtils.convertToHumanReadableString(ts));
        }

        try (BasicContext context = new BasicContext()) {
            assert storagePlugin != null;
            storagePlugin.appendData(context, pvName, strm);
        }
    }

    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void checkLowerLevelRetrieval(PlainStorageType plainStorageType) throws Exception {
        Path pbFilePath = filePath(plainStorageType, dataYear + "_05_30");

        try (BasicContext context = new BasicContext()) {
            EventStream strm = plainStorageType
                    .plainFileHandler()
                    .getTimeStream(
                            pvName,
                            pbFilePath,
                            ArchDBRTypes.DBR_SCALAR_DOUBLE,
                            Instant.parse(dataYear + "-05-30T23:59:00.00Z"),
                            Instant.parse(dataYear + "-06-01T00:00:00.00Z"),
                            false);
            int totalEvents = 0;
            for (Event ev : strm) {
                logger.debug(
                        "Got lower level data at {}", TimeUtils.convertToHumanReadableString(ev.getEventTimeStamp()));
                totalEvents++;
            }
            Assertions.assertTrue(totalEvents > 0, "Expected at least one event, got 0 events");
        }
    }

    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void checkRetrieval(PlainStorageType plainStorageType) throws Exception {
        PlainStoragePlugin storagePlugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pluginString(
                        plainStorageType,
                        "localhost",
                        "name=DataAroundPartitionEndTest&rootFolder=" + testFolder.getAbsolutePath()
                                + "&partitionGranularity=PARTITION_DAY"),
                DataAroundPartitionEndTest.configService);
        try (BasicContext context = new BasicContext()) {
            Instant rstart = generatedStartDate;
            Instant rend = rstart.plus(1, ChronoUnit.MINUTES);
            while (rend.isBefore(generatedEndDate.plus(14, ChronoUnit.DAYS))) {
                int totalEvents = 0;
                List<Callable<EventStream>> cstrms = storagePlugin.getDataForPV(context, pvName, rstart, rend);
                for (Callable<EventStream> cstrm : cstrms) {
                    EventStream st = cstrm.call();
                    for (Event ev : st) {
                        logger.debug("Data at {}", TimeUtils.convertToHumanReadableString(ev.getEventTimeStamp()));
                        totalEvents++;
                    }
                }
                if (rstart.isAfter(generatedStartDate)) {
                    Assertions.assertTrue(
                            totalEvents > 0,
                            "Did not receive any events for start "
                                    + TimeUtils.convertToHumanReadableString(rstart)
                                    + " and "
                                    + TimeUtils.convertToHumanReadableString(rend));
                    logger.info("Got " + totalEvents + " total events for "
                            + TimeUtils.convertToHumanReadableString(rstart)
                            + " and "
                            + TimeUtils.convertToHumanReadableString(rend));
                }

                rstart = rend;
                rend = rstart.plus(1, ChronoUnit.MINUTES);
            }
        }
    }

    @BeforeAll
    public static void setUp() throws Exception {
        try {
            FileUtils.deleteDirectory(testFolder);
            generateData(PlainStorageType.PB);
            generateData(PlainStorageType.PARQUET);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {
        FileUtils.deleteDirectory(testFolder);
    }
}
