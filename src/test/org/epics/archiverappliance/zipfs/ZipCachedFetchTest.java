package org.epics.archiverappliance.zipfs;

import edu.stanford.slac.archiverappliance.plain.CompressionMode;
import edu.stanford.slac.archiverappliance.plain.PathNameUtility;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import edu.stanford.slac.archiverappliance.plain.pb.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.plain.pb.MultiFilePBEventStream;
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
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.Serial;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;

/**
 * Unit test to test the performance of cached fetches from zip files
 * Create a years worth of data in a zip file.
 * Fetch a sparsifed set in serial and in parallel and compare the difference.
 * @author mshankar
 *
 */
public class ZipCachedFetchTest {
    private static final Logger logger = LogManager.getLogger(ZipCachedFetchTest.class.getName());
    String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ZipCachedFetchTest/";
    String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ZipCachedFetchTest";
    PlainStoragePlugin pbplugin;
    short currentYear = TimeUtils.getCurrentYear();
    private ConfigService configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        pbplugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=STS&rootFolder=" + rootFolderName
                        + "&partitionGranularity=PARTITION_DAY&compress=ZIP_PER_PV",
                configService);
        if (new File(rootFolderName).exists()) {
            FileUtils.deleteDirectory(new File(rootFolderName));
        }
        ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
        try (BasicContext context = new BasicContext()) {
            for (int day = 0; day < 365; day++) {
                ArrayListEventStream testData = new ArrayListEventStream(
                        PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                        new RemotableEventStreamDesc(type, pvName, currentYear));
                int startofdayinseconds = day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                for (int secondintoday = 0;
                        secondintoday < PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                        secondintoday++) {
                    testData.add(new SimulationEvent(
                            startofdayinseconds + secondintoday, currentYear, type, new ScalarValue<Double>((double)
                                    secondintoday)));
                }
                pbplugin.appendData(context, pvName, testData);
            }
        }
    }

    private void testSerialFetch(Instant startTime, Instant endTime, int months) throws Exception {
        try (BasicContext context = new BasicContext()) {
            long st0 = System.currentTimeMillis();
            Path[] paths = PathNameUtility.getPathsWithData(
                    context.getPaths(),
                    pbplugin.getRootFolder(),
                    pvName,
                    startTime,
                    endTime,
                    pbplugin.getExtensionString(),
                    pbplugin.getPartitionGranularity(),
                    pbplugin.getCompressionMode(),
                    configService.getPVNameToKeyConverter());
            long previousEpochSeconds = 0L;
            long eventCount = 0;
            try (EventStream st =
                    new MultiFilePBEventStream(paths, pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, startTime, endTime)) {
                for (Event e : st) {
                    long currEpochSeconds = e.getEpochSeconds();
                    if (currEpochSeconds - previousEpochSeconds
                            > PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk()) {
                        eventCount++;
                        previousEpochSeconds = currEpochSeconds;
                    }
                }
            }
            long st1 = System.currentTimeMillis();
            logger.info("Time takes for serial fetch is " + (st1 - st0) + "(ms) return " + eventCount + " events for "
                    + (months + 1) + " months");
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(rootFolderName));
    }

    @Test
    public void test() throws Exception {
        DecimalFormat format = new DecimalFormat("00");
        for (int months = 2; months <= 9; months++) {
            int startMonth = 2;
            int endMonth = startMonth + months;
            Instant startTime = TimeUtils.convertFromISO8601String(
                    currentYear + "-" + format.format(startMonth) + "-01T00:00:00.000Z");
            Instant endTime = TimeUtils.convertFromISO8601String(
                    currentYear + "-" + format.format(endMonth) + "-30T00:00:00.000Z");
            testParallelFetch(startTime, endTime, months);
            testSerialFetch(startTime, endTime, months);
        }
    }

    private void testParallelFetch(Instant startTime, Instant endTime, int months) throws Exception {
        ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors() / 2);
        logger.info("The parallelism in the pool is " + forkJoinPool.getParallelism());
        try (BasicContext context = new BasicContext()) {
            long st0 = System.currentTimeMillis();
            Path[] paths = PathNameUtility.getPathsWithData(
                    context.getPaths(),
                    pbplugin.getRootFolder(),
                    pvName,
                    startTime,
                    endTime,
                    pbplugin.getExtensionString(),
                    pbplugin.getPartitionGranularity(),
                    CompressionMode.NONE,
                    configService.getPVNameToKeyConverter());

            List<Future<EventStream>> futures = new LinkedList<Future<EventStream>>();
            for (Path path : paths) {
                ForkJoinTask<EventStream> submit = forkJoinPool.submit(new ZipCachedFetchEventStream(
                        new FileBackedPBEventStream(pvName, path, ArchDBRTypes.DBR_SCALAR_DOUBLE)));
                futures.add(submit);
            }

            long eventCount = 0;
            long serialTimeMs = 0;
            long longestWaitTime = 0;
            long totalWaitTime = 0;
            for (Future<EventStream> future : futures) {
                long st11 = System.currentTimeMillis();
                EventStream st = future.get();
                long st12 = System.currentTimeMillis();
                long waitDelta = st12 - st11;
                totalWaitTime += waitDelta;
                if (waitDelta > longestWaitTime) {
                    longestWaitTime = waitDelta;
                }
                for (Event e : st) {
                    e.getEpochSeconds();
                    eventCount++;
                }
                long st13 = System.currentTimeMillis();
                long delta = st13 - st11;
                serialTimeMs += delta;
                st.close();
            }

            long st1 = System.currentTimeMillis();
            logger.info("Time takes for parallel fetch is " + (st1 - st0) + "(ms) "
                    + " fetching " + eventCount + " events for " + (months + 1) + " months "
                    + " with time spent in serial ops " + serialTimeMs + " (ms) with a longest wait time of "
                    + longestWaitTime + " (ms) " + " and a total wait time of " + totalWaitTime + " (ms) ");

            forkJoinPool.shutdown();
        }
    }

    private static class ZipCachedFetchEventStream extends ArrayListEventStream implements Callable<EventStream> {
        @Serial
        private static final long serialVersionUID = 8076901507481457453L;

        EventStream srcStream;

        ZipCachedFetchEventStream(EventStream srcStream) {
            super(0, (RemotableEventStreamDesc) srcStream.getDescription());
            this.srcStream = srcStream;
        }

        @Override
        public EventStream call() {
            long previousEpochSeconds = 0L;
            for (Event e : srcStream) {
                long currEpochSeconds = e.getEpochSeconds();
                if (currEpochSeconds - previousEpochSeconds
                        > PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk()) {
                    this.add(e);
                    previousEpochSeconds = currEpochSeconds;
                }
            }
            try {
                srcStream.close();
            } catch (Exception ignored) {
            }
            return this;
        }
    }
}
