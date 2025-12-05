package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.plain.PathNameUtility;
import edu.stanford.slac.archiverappliance.plain.PathResolver;
import edu.stanford.slac.archiverappliance.plain.PlainCommonSetup;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import edu.stanford.slac.archiverappliance.plain.utils.ValidatePlainFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

/**
 * Test for the ChangeStore BPL action, which moves data for a PV from one storage plugin to another,
 */
class ChangeStoreTest {

    private static final Logger logger = LogManager.getLogger(ChangeStoreTest.class);
    static final PlainCommonSetup srcSetup = new PlainCommonSetup();
    static final PlainCommonSetup destSetup = new PlainCommonSetup();
    static ConfigServiceForTests configService;
    static List<ETLTestPlugins> etlPlugins;

    /**
     * Provides pairs of storage plugin types to test various combinations (e.g., PB -> Parquet).
     */
    static Stream<ETLTestPlugins> providePlugins() {
        return ETLTestPlugins.generatePlugins().stream();
    }

    @BeforeAll
    static void setUp() throws ConfigException {
        etlPlugins = ETLTestPlugins.generatePlugins();
        configService = new ConfigServiceForTests(new File("./bin"), 1);
    }

    @AfterAll
    static void tearDown() {
        if (configService != null) {
            configService.shutdownNow();
        }
    }

    @AfterEach
    void tearDownEach() throws IOException {
        srcSetup.deleteTestFolder();
        destSetup.deleteTestFolder();
    }

    /**
     * Tests moving data from a source store to a destination store, bypassing an intermediate store.
     * 1. Sets up a PV with one data store: [Source].
     * 2. Populates the Source store with sample data.
     * 3. Calls ETLExecutor.moveDataFromOneStorageToAnother to move data from Source to Destination.
     * 4. Verifies that the event count in the Destination matches the original count.
     * 5. Verifies that the Source store is now empty.
     */
    @ParameterizedTest
    @MethodSource("providePlugins")
    void testMoveDataAcrossTiers(ETLTestPlugins testPlugins) throws Exception {
        // Use a fixed granularity for simplicity, as the core logic being tested is the move itself.
        PartitionGranularity granularity = PartitionGranularity.PARTITION_DAY;

        // 1. Setup the three plugins and their storage folders
        PlainStoragePlugin srcPlugin = testPlugins.src();
        PlainStoragePlugin destPlugin = testPlugins.dest();

        srcSetup.setUpRootFolder(srcPlugin, "ChangeStoreTest_Src_" + srcPlugin.getPluginIdentifier(), granularity);
        destSetup.setUpRootFolder(destPlugin, "ChangeStoreTest_Dest_" + destPlugin.getPluginIdentifier(), granularity);

        logger.info(
                "Testing ChangeStore move from {} to {}",
                srcPlugin.getURLRepresentation(),
                destPlugin.getURLRepresentation());

        // 2. Configure the PV with all three data stores
        String pvName =
                ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + testPlugins.pvNamePrefix() + "ChangeStoreTest";
        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores = new String[] {
            srcPlugin.getURLRepresentation(),
        };
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests(); // Prevent regular ETL from running

        // 3. Generate sample data and write it only to the source store
        Instant startTime = TimeUtils.getStartOfYear(TimeUtils.getCurrentYear());
        Instant endTime = startTime.plusSeconds(granularity.getApproxSecondsPerChunk() * 5L); // 5 days of data
        SimulationEventStream simstream = new SimulationEventStream(
                ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0), startTime, endTime, 3600);
        try (BasicContext context = new BasicContext()) {
            srcPlugin.appendData(context, pvName, simstream);
        }
        logger.info("Done creating source data for PV {}", pvName);

        // 4. Count events in the source store before the move
        long beforeCount = countEvents(srcPlugin, pvName, startTime, endTime);
        Assertions.assertTrue(beforeCount > 0, "No data was generated in the source store.");
        logger.info("Found {} events in source store before move.", beforeCount);

        // 5. Execute the move operation
        ETLExecutor.moveDataFromOneStorageToAnother(
                configService, pvName, srcPlugin.getName(), destPlugin.getURLRepresentation());
        logger.info("Completed call to moveDataFromOneStorageToAnother.");

        // 6. Verify the results
        // a) Check that files were created in the destination and are valid
        validateDestinationFiles(destPlugin, pvName);

        // b) Count events in all stores after the move
        long afterCountDest = countEvents(destPlugin, pvName, startTime, endTime);
        long afterCountSrc = countEvents(srcPlugin, pvName, startTime, endTime);

        logger.info("Found {} events in destination store after move.", afterCountDest);
        logger.info("Found {} events remaining in source store after move.", afterCountSrc);

        // c) Assert the final state is correct
        Assertions.assertEquals(
                beforeCount, afterCountDest, "The number of events moved to the destination is incorrect.");
        Assertions.assertEquals(0, afterCountSrc, "Events were not deleted from the source store after the move.");
    }

    private long countEvents(PlainStoragePlugin plugin, String pvName, Instant start, Instant end) throws IOException {
        long count = 0;
        try (BasicContext context = new BasicContext()) {
            try (EventStream stream =
                    new CurrentThreadWorkerEventStream(pvName, plugin.getDataForPV(context, pvName, start, end))) {
                for (@SuppressWarnings("unused") Event e : stream) {
                    count++;
                }
            }
        }
        return count;
    }

    private void validateDestinationFiles(PlainStoragePlugin destPlugin, String pvName) throws IOException {
        Path[] allPaths = PathNameUtility.getAllPathsForPV(
                new ArchPaths(),
                destPlugin.getRootFolder(),
                pvName,
                destPlugin.getExtensionString(),
                PathResolver.BASE_PATH_RESOLVER,
                configService.getPVNameToKeyConverter());

        Assertions.assertNotNull(allPaths, "PathNameUtility returned null for getAllFilesForPV for " + pvName);
        Assertions.assertTrue(allPaths.length > 0, "No files were created in the destination store for " + pvName);

        for (Path destPath : allPaths) {
            Assertions.assertTrue(
                    ValidatePlainFile.validatePlainFile(destPath, true, destPlugin.getPlainFileHandler()),
                    "File validation failed for " + destPath.toAbsolutePath());
        }
    }
}
