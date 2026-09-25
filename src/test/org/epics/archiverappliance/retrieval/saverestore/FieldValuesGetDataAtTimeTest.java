package org.epics.archiverappliance.retrieval.saverestore;

import static org.epics.archiverappliance.utils.ui.URIUtils.pluginString;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import edu.stanford.slac.archiverappliance.plain.PlainStorageType;
import org.apache.commons.io.FileUtils;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.BiDirectionalIterable;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.FieldValues;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

class FieldValuesGetDataAtTimeTest {
    private static final ArchDBRTypes DBR_TYPE = ArchDBRTypes.DBR_SCALAR_DOUBLE;

    private ConfigServiceForTests configService;
    private File testFolder;

    @BeforeEach
    void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        testFolder = new File(
                ConfigServiceForTests.getDefaultPBTestFolder(), FieldValuesGetDataAtTimeTest.class.getSimpleName());
        FileUtils.deleteDirectory(testFolder);
        Assertions.assertTrue(testFolder.mkdirs() || testFolder.exists(), "Unable to create test folder");
    }

    @AfterEach
    void tearDown() throws IOException {
        ArchiveChannel.configureMetaDataPeriod(new Properties());
        FileUtils.deleteDirectory(testFolder);
    }

    @ParameterizedTest
    @EnumSource(
            value = PlainStorageType.class,
            names = {"PB", "PARQUET"})
    void backwardsLookupShouldUseLatestPriorFullRefreshAndFieldUpdates(PlainStorageType plainStorageType)
            throws Exception {
        String pvName =
                "backwards-metadata-refreshes-" + plainStorageType.name().toLowerCase();
        PlainStoragePlugin plugin = createPlugin(plainStorageType, "daily", PartitionGranularity.PARTITION_DAY);

        Instant partitionStart =
                TimeUtils.getStartOfYear(TimeUtils.getCurrentYear()).plus(180, ChronoUnit.DAYS);
        Instant firstFullMetadataTime = partitionStart.plus(1, ChronoUnit.HOURS);
        Instant firstHihiUpdateTime = partitionStart.plus(2, ChronoUnit.HOURS);
        Instant secondFullMetadataTime = partitionStart.plus(10, ChronoUnit.HOURS);
        Instant secondLoloUpdateTime = partitionStart.plus(11, ChronoUnit.HOURS);
        Instant sampleTime = partitionStart.plus(12, ChronoUnit.HOURS);

        appendEvents(
                plugin,
                pvName,
                event(
                        firstFullMetadataTime,
                        1.0,
                        Map.of(
                                "HIHI", "HIHI-full-1",
                                "LOLO", "LOLO-full-1",
                                "HIGH", "HIGH-full-1",
                                "LOW", "LOW-full-1")),
                event(firstHihiUpdateTime, 2.0, Map.of("HIHI", "HIHI-updated-before-second-full")),
                event(
                        secondFullMetadataTime,
                        3.0,
                        Map.of(
                                "HIHI", "HIHI-full-2",
                                "LOLO", "LOLO-full-2",
                                "HIGH", "HIGH-full-2",
                                "LOW", "LOW-full-2")),
                event(secondLoloUpdateTime, 4.0, Map.of("LOLO", "LOLO-updated-after-second-full")),
                event(sampleTime, 4.0, Map.of()));

        try (BasicContext context = new BasicContext()) {
            Event event = plugin.dataAtTime(
                    context,
                    pvName,
                    sampleTime.plus(1, ChronoUnit.MINUTES),
                    sampleTime.plus(6, ChronoUnit.MINUTES),
                    Period.parse("P1D"),
                    BiDirectionalIterable.IterationDirection.BACKWARDS);

            Assertions.assertNotNull(event, "Expected a sample from the partition");
            Assertions.assertEquals(sampleTime, event.getEventTimeStamp(), "Expected the selected sample");
            assertFields(
                    event,
                    Map.of(
                            "HIHI", "HIHI-full-2",
                            "LOLO", "LOLO-updated-after-second-full",
                            "HIGH", "HIGH-full-2",
                            "LOW", "LOW-full-2"));
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = PlainStorageType.class,
            names = {"PB", "PARQUET"})
    void forwardsLookupShouldUseLatestPriorRefreshWithoutFutureUpdates(PlainStorageType plainStorageType)
            throws Exception {
        String pvName = "forward-future-metadata-" + plainStorageType.name().toLowerCase();
        PlainStoragePlugin plugin = createPlugin(plainStorageType, "yearly", PartitionGranularity.PARTITION_YEAR);

        Instant baseTime = TimeUtils.getStartOfYear(TimeUtils.getCurrentYear())
                .plus(200, ChronoUnit.DAYS)
                .plus(1, ChronoUnit.HOURS);
        Instant firstFullMetadataTime = baseTime;
        Instant firstHihiUpdateTime = baseTime.plus(5, ChronoUnit.SECONDS);
        Instant secondFullMetadataTime = baseTime.plus(10, ChronoUnit.SECONDS);
        Instant secondLoloUpdateTime = baseTime.plus(15, ChronoUnit.SECONDS);
        Instant selectedSampleTime = baseTime.plus(20, ChronoUnit.SECONDS);
        Instant futureHihiUpdateTime = baseTime.plus(30, ChronoUnit.SECONDS);

        appendEvents(
                plugin,
                pvName,
                event(
                        firstFullMetadataTime,
                        10.0,
                        Map.of(
                                "HIHI", "HIHI-full-1",
                                "LOLO", "LOLO-full-1",
                                "HIGH", "HIGH-full-1",
                                "LOW", "LOW-full-1")),
                event(firstHihiUpdateTime, 10.5, Map.of("HIHI", "HIHI-updated-before-second-full")),
                event(
                        secondFullMetadataTime,
                        11.0,
                        Map.of(
                                "HIHI", "HIHI-full-2",
                                "LOLO", "LOLO-full-2",
                                "HIGH", "HIGH-full-2",
                                "LOW", "LOW-full-2")),
                event(secondLoloUpdateTime, 11.5, Map.of("LOLO", "LOLO-updated-after-second-full")),
                event(selectedSampleTime, 11.0, Map.of()),
                event(futureHihiUpdateTime, 12.0, Map.of("HIHI", "HIHI-future")));

        try (BasicContext context = new BasicContext()) {
            Event event = plugin.dataAtTime(
                    context,
                    pvName,
                    selectedSampleTime,
                    selectedSampleTime,
                    Period.parse("P1D"),
                    BiDirectionalIterable.IterationDirection.FORWARDS);

            Assertions.assertNotNull(event, "Expected the exact sample at the lookup time");
            Assertions.assertEquals(
                    selectedSampleTime,
                    event.getEventTimeStamp(),
                    "Expected forwards lookup to return the exact matching sample");
            assertFields(
                    event,
                    Map.of(
                            "HIHI", "HIHI-full-2",
                            "LOLO", "LOLO-updated-after-second-full",
                            "HIGH", "HIGH-full-2",
                            "LOW", "LOW-full-2"));
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = PlainStorageType.class,
            names = {"PB", "PARQUET"})
    void lookupShouldUseDailyReadWindowWhenConfiguredRefreshWindowIsShorter(PlainStorageType plainStorageType)
            throws Exception {
        Properties properties = new Properties();
        properties.setProperty(
                ArchiveChannel.SAVE_META_DATA_PERIOD_SECS_PROPERTY,
                Integer.toString(ArchiveChannel.MIN_SAVE_META_DATA_PERIOD_SECS));
        ArchiveChannel.configureMetaDataPeriod(properties);

        String pvName = "configured-refresh-window-" + plainStorageType.name().toLowerCase();
        PlainStoragePlugin plugin =
                createPlugin(plainStorageType, "configured-period", PartitionGranularity.PARTITION_DAY);
        Instant partitionStart =
                TimeUtils.getStartOfYear(TimeUtils.getCurrentYear()).plus(210, ChronoUnit.DAYS);
        Instant fullMetadataTime = partitionStart.plus(1, ChronoUnit.HOURS);
        Instant sampleTime = fullMetadataTime.plus(13, ChronoUnit.HOURS);

        appendEvents(
                plugin,
                pvName,
                event(
                        fullMetadataTime,
                        1.0,
                        Map.of("HIHI", "HIHI-full", "LOLO", "LOLO-full", "HIGH", "HIGH-full", "LOW", "LOW-full")),
                event(sampleTime, 2.0, Map.of()));

        try (BasicContext context = new BasicContext()) {
            Event event = plugin.dataAtTime(
                    context,
                    pvName,
                    sampleTime.plus(1, ChronoUnit.MINUTES),
                    sampleTime.plus(6, ChronoUnit.MINUTES),
                    Period.parse("P1D"),
                    BiDirectionalIterable.IterationDirection.BACKWARDS);

            Assertions.assertNotNull(event, "Expected a sample from the partition");
            Assertions.assertEquals(sampleTime, event.getEventTimeStamp(), "Expected the selected sample");
            assertFields(
                    event, Map.of("HIHI", "HIHI-full", "LOLO", "LOLO-full", "HIGH", "HIGH-full", "LOW", "LOW-full"));
        }
    }

    private PlainStoragePlugin createPlugin(
            PlainStorageType plainStorageType, String nameSuffix, PartitionGranularity granularity)
            throws IOException, ConfigException {
        return (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pluginString(
                        plainStorageType,
                        "localhost",
                        "name=" + FieldValuesGetDataAtTimeTest.class.getSimpleName() + "-" + plainStorageType.name()
                                + "-" + nameSuffix
                                + "&rootFolder=" + testFolder.getAbsolutePath()
                                + "&partitionGranularity=" + granularity.name()),
                configService);
    }

    private void appendEvents(PlainStoragePlugin plugin, String pvName, DBRTimeEvent... events) throws IOException {
        short year = TimeUtils.convertToYearSecondTimestamp(events[0].getEventTimeStamp())
                .getYear();
        ArrayListEventStream stream =
                new ArrayListEventStream(year, new RemotableEventStreamDesc(DBR_TYPE, pvName, year));
        for (DBRTimeEvent event : events) {
            stream.add(event);
        }
        try (BasicContext context = new BasicContext()) {
            plugin.appendData(context, pvName, stream);
        }
    }

    private DBRTimeEvent event(Instant timestamp, double value, Map<String, String> fieldValues) {
        DBRTimeEvent event =
                (DBRTimeEvent) new POJOEvent(DBR_TYPE, timestamp, new ScalarValue<>(value), 0, 0).makeClone();
        if (!fieldValues.isEmpty()) {
            event.setFieldValues(new HashMap<>(fieldValues), false);
        }
        return event;
    }

    private void assertFields(Event event, Map<String, String> expectedFieldValues) {
        Map<String, String> fields = ((FieldValues) event).getFields();
        Assertions.assertNotNull(fields, "Expected metadata fields");
        Assertions.assertEquals(expectedFieldValues, fields, "Unexpected metadata fields");
    }
}
