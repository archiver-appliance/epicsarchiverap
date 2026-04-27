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
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/*
 * Complex test case for testing ETL using a GZTar as a ETLSource with a blackhole after it.
 * We create an ETL pipeline FileSystem -> GZTar -> Blackhole
 * ETL runs every so often and since the GZTar file is acting as an ETL source, we expect files to be deleted from it.
 * This should result in an optimization of the GZTar file.
 */

public class OptimizeTest {
    private static final Logger logger = LogManager.getLogger();
    private static final String pvNameBase = OptimizeTest.class.getSimpleName();
    private static final short startYear = 2010;
    private static final short endYear = 2020;
    private static final File ltsFolder = new File(System.getenv("ARCHAPPL_LONG_TERM_FOLDER") + "/OptimizeTest");
    private static final File xltsFolder = new File(System.getenv("ARCHAPPL_XLTS_TERM_FOLDER") + "/OptimizeTest");
    private static ConfigServiceForTests configService;

    @BeforeAll
    public static void setUp() throws Exception {
        configService = new ConfigServiceForTests(new File("./bin"), 1);
        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
        if (xltsFolder.exists()) {
            FileUtils.deleteDirectory(xltsFolder);
        }
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
        if (xltsFolder.exists()) {
            FileUtils.deleteDirectory(xltsFolder);
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {
        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
        if (xltsFolder.exists()) {
            FileUtils.deleteDirectory(xltsFolder);
        }
        configService.shutdownNow();
    }

    @ParameterizedTest
    @ValueSource(strings = {"pb", "parquet"})
    public void testOptimize(String plugin) throws Exception {
        String pvName = pvNameBase + ":" + plugin;
        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores = new String[] {
            "pb://localhost?name=LTS&rootFolder=" + ltsFolder.getAbsolutePath()
                    + "&partitionGranularity=PARTITION_YEAR",
            plugin + "://localhost?name=XLTS&rootFolder=" + ArchPaths.TAR_SCHEME + "://" + xltsFolder.getAbsolutePath()
                    + "&partitionGranularity=PARTITION_DAY&hold=730&gather=365",
            "blackhole://localhost?name=BLACKHOLE"
        };
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();

        // Generate data in the LTS
        StoragePlugin lts = StoragePluginURLParser.parseStoragePlugin(dataStores[0], configService);
        for (short year = startYear; year <= endYear; year++) {
            try (BasicContext context = new BasicContext()) {
                ArrayListEventStream inStream = new ArrayListEventStream(
                        1000, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
                for (short day = 0; day < 365; day++) {
                    long dayEpoch = TimeUtils.getStartOfYearInSeconds(year) + day * 86400;
                    POJOEvent event = new POJOEvent(
                            ArchDBRTypes.DBR_SCALAR_DOUBLE,
                            Instant.ofEpochSecond(dayEpoch),
                            new ScalarValue<Double>(42.0),
                            0,
                            0);
                    inStream.add(event);
                }
                lts.appendData(context, pvName, inStream);
            }
        }

        StoragePlugin xlts = StoragePluginURLParser.parseStoragePlugin(dataStores[1], configService);
        Path tarPath = Paths.get(xltsFolder.getAbsolutePath(), pvNameBase, plugin + ".tar");
        EAATar xltsTar = new EAATar(tarPath.toString());
        for (short year = startYear + 2; year <= endYear; year++) {
            logger.debug("Running ETL for year {}", year);
            ETLExecutor.runETLs(
                    configService,
                    TimeUtils.convertFromEpochSeconds(TimeUtils.getStartOfYearInSeconds(year) + 15 * 86400, 0));
            Assertions.assertTrue(Files.exists(tarPath), "Expecting tar file to be present after initial ETL");
            Map<String, TarEntry> catalog = xltsTar.loadCatalog();
            int catalogSize = catalog.size();
            logger.debug("Catalog size after ETL for year {}: {}", year, catalogSize);
            Assertions.assertTrue(
                    catalogSize > 0, "Expected entries in the XLTS GZTar after ETL Got " + catalogSize + " entries");
            int expectedNumFiles = (730 - (365 + 15)); // hold - gather and the the +15 above
            Assertions.assertTrue(
                    catalogSize >= expectedNumFiles,
                    "Expected at least " + expectedNumFiles + " days worth in the XLTS GZTar after ETL Got "
                            + catalogSize + " entries");
            Assertions.assertTrue(
                    catalogSize <= 4 * 365,
                    "Expected at most 4 years worth in the XLTS GZTar after ETL Got " + catalogSize + " entries");

            int eventCount = getEventCount(pvName, xlts);
            Assertions.assertTrue(
                    eventCount > 0, "Expected at least a few events from the xlts. Got " + eventCount + " events");
        }
    }

    private static int getEventCount(String pvName, StoragePlugin xlts) throws Exception {
        try (BasicContext context = new BasicContext()) {
            List<Callable<EventStream>> streams = xlts.getDataForPV(
                    context,
                    pvName,
                    TimeUtils.getStartOfYear(startYear - 1),
                    TimeUtils.getStartOfYear(endYear + 1),
                    new DefaultRawPostProcessor());
            int totalEvents = 0;
            for (Callable<EventStream> streamCallable : streams) {
                try (EventStream stream = streamCallable.call()) {
                    for (Event e : stream) {
                        totalEvents++;
                    }
                }
            }
            return totalEvents;
        }
    }
}
