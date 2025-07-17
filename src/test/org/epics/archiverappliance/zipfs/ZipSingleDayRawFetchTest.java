package org.epics.archiverappliance.zipfs;

import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
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
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * Test performance and some functionality of fetching a single days worth of data from a ZIP_PER_PV
 * @author mshankar
 *
 */
public class ZipSingleDayRawFetchTest {
    private static final Logger logger = LogManager.getLogger(ZipSingleDayRawFetchTest.class.getName());
    String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ZipSingleDayRawFetch/";
    String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ZipSingleDayRawFetch";
    PlainPBStoragePlugin pbplugin;
    short currentYear = TimeUtils.getCurrentYear();
    private ConfigService configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        pbplugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
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

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(rootFolderName));
    }

    @Test
    public void testFetches() throws Exception {
        testFetchForSingleDay();
        testBypassSearchFetchForSingleDay();
    }

    private void testFetchForSingleDay() throws Exception {
        Random r = new Random();
        int day = 30 + r.nextInt(90);
        Instant startTime = TimeUtils.convertFromEpochSeconds(
                TimeUtils.getStartOfCurrentYearInSeconds()
                        + (long) day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                0);
        Instant endTime = TimeUtils.plusDays(startTime, 1);
        logger.info("Testing fetch betweeen " + TimeUtils.convertToHumanReadableString(startTime) + " and "
                + TimeUtils.convertToHumanReadableString(endTime));
        int eventCount = 0;
        long startEpochSeconds = TimeUtils.convertToEpochSeconds(startTime);
        long expectedEpochSeconds = startEpochSeconds - 1;
        long start = System.currentTimeMillis();
        try (BasicContext context = new BasicContext()) {
            List<Callable<EventStream>> callables = pbplugin.getDataForPV(context, pvName, startTime, endTime);
            try (EventStream strm = new CurrentThreadWorkerEventStream(pvName, callables)) {
                for (Event e : strm) {
                    long actualEpochSeconds = e.getEpochSeconds();
                    // The PlainPBStorage plugin will also yield the last event of the previous partition.
                    // We skip checking that as part of this test
                    if (actualEpochSeconds < startEpochSeconds - 1) continue;
                    if (expectedEpochSeconds != actualEpochSeconds) {
                        Assertions.fail(
                                "Expected timestamp " + TimeUtils.convertToHumanReadableString(expectedEpochSeconds)
                                        + " got " + TimeUtils.convertToHumanReadableString(actualEpochSeconds));
                    }
                    eventCount++;
                    expectedEpochSeconds++;
                }
            }
        }
        long end = System.currentTimeMillis();
        logger.info("Got " + eventCount + " events in " + (end - start) + "(ms)");
    }

    private void testBypassSearchFetchForSingleDay() throws Exception {
        Random r = new Random();
        int day = 30 + r.nextInt(90);
        Instant startTime = TimeUtils.getStartOfYear(TimeUtils.getCurrentYear())
                .plusSeconds((long) day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk());
        logger.info("Testing fetch for " + startTime);
        int eventCount = 0;
        Instant expectedTime = startTime;
        long start = System.currentTimeMillis();
        try (BasicContext context = new BasicContext()) {
            Path path = PlainPBPathNameUtility.getPathNameForTime(
                    pbplugin, pvName, startTime, context.getPaths(), configService.getPVNameToKeyConverter());
            for (Event e : new FileBackedPBEventStream(pvName, path, ArchDBRTypes.DBR_SCALAR_DOUBLE)) {
                Instant actualTime = e.getEventTimeStamp();
                // The PlainPBStorage plugin will also yield the last event of the previous partition.
                // We skip checking that as part of this test
                if (actualTime.isBefore(startTime.minusSeconds(1))) continue;
                Assertions.assertEquals(expectedTime, actualTime);

                eventCount++;
                expectedTime = expectedTime.plusSeconds(1);
            }
        }
        long end = System.currentTimeMillis();
        logger.info("Bypassing search, got " + eventCount + " events in " + (end - start) + "(ms)");
    }
}
