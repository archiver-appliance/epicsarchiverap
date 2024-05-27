package org.epics.archiverappliance.zipfs;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
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
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.time.Instant;
import java.util.stream.Stream;

public class ZipETLTest {
    private static final Logger logger = LogManager.getLogger(ZipETLTest.class.getName());
    File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ZipETLTest");
    private ConfigService configService;

    private static Stream<Arguments> provideSource() {
        return Stream.of(Arguments.of(true), Arguments.of(false));
    }

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        if (testFolder.exists()) {
            FileUtils.deleteDirectory(testFolder);
        }
        assert testFolder.mkdirs();
    }

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(testFolder);
    }

    @ParameterizedTest
    @MethodSource("provideSource")
    public void testETLIntoZipPerPV(boolean compressSrc) throws Exception {
        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":ZipETLTest" + compressSrc;
        ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
        String srcRootFolder = testFolder.getAbsolutePath() + File.separator + "srcFiles";
        String srcPluginString =
                "pb://localhost?name=ZipETL&rootFolder=" + srcRootFolder + "&partitionGranularity=PARTITION_DAY";
        if (compressSrc) {
            srcPluginString = srcPluginString + "&compress=ZIP_PER_PV";
        }
        PlainStoragePlugin etlSrc =
                (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(srcPluginString, configService);
        logger.info(etlSrc.getURLRepresentation());

        String destRootFolder = testFolder.getAbsolutePath() + File.separator + "destFiles";
        PlainStoragePlugin etlDest = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=ZipETL&rootFolder=" + destRootFolder
                        + "&partitionGranularity=PARTITION_DAY&compress=ZIP_PER_PV",
                configService);
        logger.info(etlDest.getURLRepresentation());

        PVTypeInfo typeInfo = new PVTypeInfo(pvName, dbrType, true, 1);
        String[] dataStores = new String[] {etlSrc.getURLRepresentation(), etlDest.getURLRepresentation()};
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();

        int phasediffindegrees = 10;
        short currentYear = TimeUtils.getCurrentYear();
        Instant startTime = TimeUtils.getStartOfYear(currentYear);
        Instant endTime = startTime.plusSeconds(PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 7L);
        SimulationEventStream simstream =
                new SimulationEventStream(dbrType, new SineGenerator(phasediffindegrees), startTime, endTime, 1);
        try (BasicContext context = new BasicContext()) {
            etlSrc.appendData(context, pvName, simstream);
        }

        Instant timeETLruns = endTime.plusSeconds(PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 10L);

        ETLExecutor.runETLs(configService, timeETLruns);
        logger.info("Done performing ETL");

        File expectedZipFile = new File(destRootFolder + File.separator
                + configService.getPVNameToKeyConverter().convertPVNameToKey(pvName) + "_pb.zip");
        Assertions.assertTrue(expectedZipFile.exists(), "Zip file does not seem to exist " + expectedZipFile);

        logger.info("Testing retrieval for zip per pv");
        int srcEventCount = 0;
        try (BasicContext context = new BasicContext();
                EventStream strm = new CurrentThreadWorkerEventStream(
                        pvName, etlSrc.getDataForPV(context, pvName, startTime, endTime))) {
            for (@SuppressWarnings("unused") Event ev : strm) {
                srcEventCount++;
            }
        }
        int destEventCount = 0;
        try (BasicContext context = new BasicContext();
                EventStream strm = new CurrentThreadWorkerEventStream(
                        pvName, etlDest.getDataForPV(context, pvName, startTime, endTime))) {
            for (@SuppressWarnings("unused") Event ev : strm) {
                destEventCount++;
            }
        }
        logger.info("Got " + srcEventCount + " src events " + destEventCount + " dest events");
        Assertions.assertEquals(
                (simstream.getNumberOfEvents() - 1),
                srcEventCount + destEventCount,
                "Retrieval does not seem to return correct number of events " + srcEventCount + destEventCount);
    }
}
