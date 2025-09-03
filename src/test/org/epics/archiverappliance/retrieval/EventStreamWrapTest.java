package org.epics.archiverappliance.retrieval;

import static org.epics.archiverappliance.utils.ui.URIUtils.pluginString;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import edu.stanford.slac.archiverappliance.plain.PlainStorageType;
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
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.retrieval.postprocessors.Mean;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessors;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Tests strategies for wrapping event streams
 * We create a couple of years worth of 1HZ DBR_DOUBLE data and then try to wrap Post Processors using various wrapping strategies.
 * @author mshankar
 *
 */
public class EventStreamWrapTest {
    private static final Logger logger = LogManager.getLogger(EventStreamWrapTest.class.getName());
    static String shortTermFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/EventStreamWrapTest";
    static ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
    private static final String pvName =
            ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "S_" + type.getPrimitiveName();
    static ConfigService configService;
    static PlainStoragePlugin storagePluginPB;
    static PlainStoragePlugin storagePluginParquet;

    @BeforeAll
    public static void setUp() throws Exception {
        configService = new ConfigServiceForTests(1);
        if (new File(shortTermFolderName).exists()) {
            FileUtils.deleteDirectory(new File(shortTermFolderName));
        }
        assert new File(shortTermFolderName).mkdirs();
        storagePluginPB = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pluginString(
                        PlainStorageType.PB,
                        "localhost",
                        "name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_MONTH"),
                configService);

        storagePluginParquet = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pluginString(
                        PlainStorageType.PARQUET,
                        "localhost",
                        "name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_MONTH"),
                configService);

        logger.info("Start insert data");
        insertData(storagePluginPB);
        logger.info("Start insert parquet data");
        insertData(storagePluginParquet);
        logger.info("Finished setup");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        if (new File(shortTermFolderName).exists()) {
            FileUtils.deleteDirectory(new File(shortTermFolderName));
        }
        configService.shutdownNow();
    }

    static void insertData(PlainStoragePlugin storagePlugin) throws IOException {
        short currentYear = TimeUtils.getCurrentYear();
        try (BasicContext context = new BasicContext()) {
            storagePlugin.appendData(
                    context,
                    pvName,
                    new SimulationEventStream(
                            type,
                            (type, secondsIntoYear) -> new ScalarValue<Double>(1.0),
                            TimeUtils.getStartOfYear(currentYear - 1),
                            TimeUtils.getStartOfYear(currentYear)
                                    .plusSeconds(PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk()),
                            1));
        }
    }

    static PlainStoragePlugin storagePlugin(PlainStorageType plainStorageType) {
        return switch (plainStorageType) {
            case PARQUET -> storagePluginParquet;
            case PB -> storagePluginPB;
        };
    }

    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void testSimpleWrapper(PlainStorageType plainStorageType) throws Exception {
        PlainStoragePlugin storageplugin = storagePlugin(plainStorageType);
        Instant end = TimeUtils.now();
        Instant start = TimeUtils.minusDays(end, 365);
        Mean mean_86400 = (Mean) PostProcessors.findPostProcessor("mean_86400");
        mean_86400.initialize("mean_86400", pvName);
        PVTypeInfo info = new PVTypeInfo();
        info.setComputedStorageRate(40);
        mean_86400.estimateMemoryConsumption(pvName, info, start, end, null);
        try (BasicContext context = new BasicContext()) {
            long t0 = System.currentTimeMillis();
            assert storageplugin != null;
            List<Callable<EventStream>> callables = storageplugin.getDataForPV(context, pvName, start, end, mean_86400);
            for (Callable<EventStream> callable : callables) {
                callable.call();
            }
            long eventCount = 0;
            EventStream consolidatedEventStream =
                    ((PostProcessorWithConsolidatedEventStream) mean_86400).getConsolidatedEventStream();
            // In cases where the data spans year boundaries, we continue with the same stream.
            boolean continueprocessing = true;
            while (continueprocessing) {
                try {
                    for (Event e : consolidatedEventStream) {
                        Assertions.assertEquals(
                                1.0,
                                e.getSampleValue().getValue().doubleValue(),
                                0.0,
                                "All values are 1 so mean should be 1. Instead we got "
                                        + e.getSampleValue().getValue().doubleValue() + " at " + eventCount + " for pv "
                                        + pvName);
                        eventCount++;
                    }
                    continueprocessing = false;
                } catch (ChangeInYearsException ex) {
                    logger.debug("Change in years");
                }
            }
            long t1 = System.currentTimeMillis();
            // We get 365 or 366 events based on what now() is
            Assertions.assertTrue(eventCount >= 366, "Expecting 366 values got " + eventCount + " for pv " + pvName);
            logger.info("Simple wrapper took " + (t1 - t0) + "(ms)");
        }
    }

    /**
     * We wrap a thread around each source event stream. Since the source data is generated using month partitions, we
     * should get about 12 source event streams.
     */
    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    void testMultiThreadWrapper(PlainStorageType plainStorageType) throws Exception {
        PlainStoragePlugin storageplugin = storagePlugin(plainStorageType);

        Instant end = TimeUtils.now();
        Instant start = TimeUtils.minusDays(end, 365);
        Mean mean_86400 = (Mean) PostProcessors.findPostProcessor("mean_86400");
        mean_86400.initialize("mean_86400", pvName);
        PVTypeInfo info = new PVTypeInfo();
        info.setComputedStorageRate(40);
        mean_86400.estimateMemoryConsumption(pvName, info, start, end, null);
        try (BasicContext context = new BasicContext()) {
            ExecutorService executors = Executors.newFixedThreadPool(2);
            long t0 = System.currentTimeMillis();
            assert storageplugin != null;
            List<Callable<EventStream>> callables = storageplugin.getDataForPV(context, pvName, start, end, mean_86400);
            List<Future<EventStream>> futures = new ArrayList<Future<EventStream>>();
            for (Callable<EventStream> callable : callables) {
                futures.add(executors.submit(callable));
            }

            for (Future<EventStream> future : futures) {
                try {
                    future.get();
                } catch (Exception ex) {
                    logger.error("Exception computing mean_86400", ex);
                }
            }

            long eventCount = 0;
            EventStream consolidatedEventStream =
                    ((PostProcessorWithConsolidatedEventStream) mean_86400).getConsolidatedEventStream();
            // In cases where the data spans year boundaries, we continue with the same stream.
            boolean continueprocessing = true;
            while (continueprocessing) {
                try {
                    for (Event e : consolidatedEventStream) {
                        Assertions.assertEquals(
                                1.0,
                                e.getSampleValue().getValue().doubleValue(),
                                0.0,
                                "All values are 1 so mean should be 1. Instead we got "
                                        + e.getSampleValue().getValue().doubleValue() + " at " + eventCount + " for pv "
                                        + pvName);
                        eventCount++;
                    }
                    continueprocessing = false;
                } catch (ChangeInYearsException ex) {
                    logger.debug("Change in years");
                }
                long t1 = System.currentTimeMillis();
                executors.shutdown();
                // assertTrue("Expecting 365 values got " + eventCount + " for pv " + pvName, eventCount == 365);
                logger.info("Multi threaded wrapper took " + (t1 - t0) + "(ms)");
            }
        }
    }

    // Note that not all the post processors are thread safe.
    // And they need to be made thread safe before adding to this test set
}
