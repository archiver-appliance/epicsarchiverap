package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import edu.stanford.slac.archiverappliance.plain.pb.PBPlainFileHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.FirstSamplePP;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessors;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import javax.servlet.http.HttpServletRequest;

/**
 * Test the postprocessor caching functionality in ETL.
 * We generate some data; run ETL across a plugin configured with a post processor that reduces the number of events.
 * We then ask for data with and without the post processor and make sure that are significantly different.
 * However, the post processor is a DefaultRaw like processor masquerading as the post processor used in the ETL.
 * @author mshankar
 *
 */
public class ETLPostProcessorTest {
    private static final Logger logger = LogManager.getLogger(ETLPostProcessorTest.class.getName());
    private static ConfigService configService;
    final PostProcessor testPostProcessor = new FirstSamplePP();
    String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ETLPostProcessorTest";
    short currentYear = TimeUtils.getCurrentYear();
    ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;

    @BeforeAll
    public static void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        configService.shutdownNow();
    }

    private static int countAndValidateEvents(List<Callable<EventStream>> callables, String pvName) {
        int eventCount = 0;
        long previousEventEpochSeconds = 0;
        try (EventStream stream = new CurrentThreadWorkerEventStream(pvName, callables)) {
            for (Event e : stream) {
                long currentEpochSeconds = e.getEpochSeconds();
                Assertions.assertTrue(
                        currentEpochSeconds > previousEventEpochSeconds,
                        "Timestamps are not sequential current = "
                                + TimeUtils.convertToHumanReadableString(currentEpochSeconds)
                                + " previous = "
                                + TimeUtils.convertToHumanReadableString(previousEventEpochSeconds));
                previousEventEpochSeconds = currentEpochSeconds;
                eventCount++;
            }
        } catch (IOException e) {
            Assertions.fail("Error reading stream files", e);
        }
        return eventCount;
    }

    @Test
    public void testPostProcessorDuringETL() throws Exception {

        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETLPostProcessorTest";
        PlainStoragePlugin srcPlainPlugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                PBPlainFileHandler.DEFAULT_PB_HANDLER.pluginIdentifier() + "://localhost?name=STS&rootFolder="
                        + rootFolderName + "/src&partitionGranularity=PARTITION_HOUR",
                configService);
        PlainStoragePlugin destPlainPlugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                PBPlainFileHandler.DEFAULT_PB_HANDLER.pluginIdentifier() + "://localhost?name=MTS&rootFolder="
                        + rootFolderName + "/dest&partitionGranularity=PARTITION_DAY&pp="
                        + testPostProcessor.getExtension(),
                configService);
        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores =
                new String[] {srcPlainPlugin.getURLRepresentation(), destPlainPlugin.getURLRepresentation()};
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();

        for (int day = 0; day < 5; day++) {
            logger.debug("Generating data for day " + 1);
            int startOfDaySeconds = day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
            int runsPerDay = 12;
            int eventsPerRun = PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() / runsPerDay;
            for (int currentrun = 0; currentrun < runsPerDay; currentrun++) {
                try (BasicContext context = new BasicContext()) {
                    logger.debug("Generating data for run " + currentrun);
                    ArrayListEventStream testData = new ArrayListEventStream(
                            eventsPerRun, new RemotableEventStreamDesc(type, pvName, currentYear));
                    for (int secondsinrun = 0; secondsinrun < eventsPerRun; secondsinrun++) {
                        testData.add(new SimulationEvent(
                                startOfDaySeconds + currentrun * eventsPerRun + secondsinrun,
                                currentYear,
                                type,
                                new ScalarValue<Double>((double) secondsinrun)));
                    }
                    srcPlainPlugin.appendData(context, pvName, testData);
                    YearSecondTimestamp yts = new YearSecondTimestamp(
                            currentYear,
                            (day + 1) * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() + 30,
                            0);
                    Instant etlTime = TimeUtils.convertFromYearSecondTimestamp(yts);
                    logger.debug("Running ETL as if it were " + TimeUtils.convertToHumanReadableString(etlTime));
                    ETLExecutor.runETLs(configService, etlTime);
                }
                // Sleep for a couple of seconds so that the modification times are different.
                Thread.sleep(2);
            }

            logger.debug("Validating that ETL ran the post processors correctly.");
            try (BasicContext context = new BasicContext()) {
                List<Callable<EventStream>> callablesRaw = destPlainPlugin.getDataForPV(
                        context,
                        pvName,
                        TimeUtils.getStartOfYear(currentYear),
                        TimeUtils.getStartOfYear(currentYear + 1),
                        new DefaultRawPostProcessor());
                int eventCountRaw = countAndValidateEvents(callablesRaw, pvName);
                // Here's where we use a DefaultRawPostProcessor like post processor that pretends to be the same post
                // processor as that used during ETL.
                List<Callable<EventStream>> callablesReduced = destPlainPlugin.getDataForPV(
                        context,
                        pvName,
                        TimeUtils.getStartOfYear(currentYear),
                        TimeUtils.getStartOfYear(currentYear + 1),
                        new PostProcessor() {
                            @Override
                            public Callable<EventStream> wrap(Callable<EventStream> callable) {
                                return callable;
                            }

                            @Override
                            public void initialize(String userarg, String pvName) {}

                            @Override
                            public String getIdentity() {
                                return testPostProcessor.getIdentity();
                            }

                            @Override
                            public String getExtension() {
                                return testPostProcessor.getIdentity();
                            }

                            @Override
                            public long estimateMemoryConsumption(
                                    String pvName,
                                    PVTypeInfo typeInfo,
                                    Instant start,
                                    Instant end,
                                    HttpServletRequest req) {
                                return 0;
                            }
                        });
                int eventCountReduced = countAndValidateEvents(callablesReduced, pvName);
                int expectedReducedCount = eventCountRaw / PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;
                logger.info("On day " + day + " we got " + eventCountRaw + " raw events and " + eventCountReduced
                        + " reduced events and an expected reduced event count of " + expectedReducedCount);
                Assertions.assertTrue(eventCountReduced > 0, "No reduced events are being produced by ETL");
                Assertions.assertTrue(
                        eventCountRaw > eventCountReduced,
                        "We are getting the same (or more) events for raw and reduced. Raw = " + eventCountRaw
                                + " and reduced = " + eventCountReduced);
                Assertions.assertTrue(
                        Math.abs(eventCountReduced - expectedReducedCount) < 10,
                        "We expected a reduced eventcount of  " + expectedReducedCount + " and we got "
                                + eventCountReduced);
            }
        }
    }
}
