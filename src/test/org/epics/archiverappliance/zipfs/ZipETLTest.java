package org.epics.archiverappliance.zipfs;

import static edu.stanford.slac.archiverappliance.plain.pb.PBPlainFileHandler.PB_PLUGIN_IDENTIFIER;
import static org.epics.archiverappliance.utils.ui.URIUtils.pluginString;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

@Tag("slow")
public class ZipETLTest {
    private static Logger logger = LogManager.getLogger(ZipETLTest.class.getName());
    File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ZipETLTest");
    private ConfigService configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        if (testFolder.exists()) {
            FileUtils.deleteDirectory(testFolder);
        }
        testFolder.mkdirs();
    }

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(testFolder);
    }

    @Test
    public void testETLIntoZipPerPV() throws Exception {
        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":ETLZipTest";
        String srcRootFolder = testFolder.getAbsolutePath() + File.separator + "srcFiles";
        PlainStoragePlugin etlSrc = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pluginString(
                        PB_PLUGIN_IDENTIFIER,
                        "localhost",
                        "name=ZipETL&rootFolder=" + srcRootFolder + "&partitionGranularity=PARTITION_DAY"),
                configService);
        logger.info(etlSrc.getURLRepresentation());
        ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
        int phasediffindegrees = 10;
        short currentYear = TimeUtils.getCurrentYear();
        SimulationEventStream simstream = new SimulationEventStream(
                dbrType,
                new SineGenerator(phasediffindegrees),
                TimeUtils.getStartOfYear(currentYear),
                TimeUtils.getEndOfYear(currentYear),
                1);
        try (BasicContext context = new BasicContext()) {
            etlSrc.appendData(context, pvName, simstream);
        }

        String destRootFolder = testFolder.getAbsolutePath() + File.separator + "destFiles";
        PlainStoragePlugin etlDest = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pluginString(
                        PB_PLUGIN_IDENTIFIER,
                        "localhost",
                        "name=ZipETL&rootFolder=" + destRootFolder
                                + "&partitionGranularity=PARTITION_DAY&compress=ZIP_PER_PV"),
                configService);
        logger.info(etlDest.getURLRepresentation());

        PVTypeInfo typeInfo = new PVTypeInfo(pvName, dbrType, true, 1);
        String[] dataStores = new String[] {etlSrc.getURLRepresentation(), etlDest.getURLRepresentation()};
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();

        Instant timeETLruns = TimeUtils.now();
        ZonedDateTime ts = ZonedDateTime.now(ZoneId.from(ZoneOffset.UTC));
        if (ts.getMonth() == Month.JANUARY) {
            // This means that we never test this in Jan but I'd rather have the null check than skip this.
            timeETLruns = TimeUtils.plusDays(timeETLruns, 35);
        }
        ETLExecutor.runETLs(configService, timeETLruns);
        logger.info("Done performing ETL");

        File expectedZipFile = new File(destRootFolder + File.separator
                + configService.getPVNameToKeyConverter().convertPVNameToKey(pvName) + "_pb.zip");
        Assertions.assertTrue(expectedZipFile.exists(), "Zip file does not seem to exist " + expectedZipFile);

        logger.info("Testing retrieval for zip per pv");
        int eventCount = 0;
        try (BasicContext context = new BasicContext();
                EventStream strm = new CurrentThreadWorkerEventStream(
                        pvName,
                        etlSrc.getDataForPV(
                                context,
                                pvName,
                                TimeUtils.getStartOfYear(TimeUtils.getCurrentYear()),
                                TimeUtils.getEndOfYear(TimeUtils.getCurrentYear())))) {
            if (strm != null) {
                for (@SuppressWarnings("unused") Event ev : strm) {
                    eventCount++;
                }
            }
        }
        try (BasicContext context = new BasicContext();
                EventStream strm = new CurrentThreadWorkerEventStream(
                        pvName,
                        etlDest.getDataForPV(
                                context,
                                pvName,
                                TimeUtils.getStartOfYear(TimeUtils.getCurrentYear()),
                                TimeUtils.getEndOfYear(TimeUtils.getCurrentYear())))) {
            for (@SuppressWarnings("unused") Event ev : strm) {
                eventCount++;
            }
        }
        logger.info("Got " + eventCount + " events");
        Assertions.assertTrue(
                eventCount >= (simstream.getNumberOfEvents() - 1),
                "Retrieval does not seem to return any events " + eventCount);
    }
}
