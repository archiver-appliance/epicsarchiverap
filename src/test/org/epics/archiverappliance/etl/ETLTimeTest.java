package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.plain.pb.PBCompressionMode;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.stream.Stream;

/**
 * An ETL benchmark. Generate some data for PVs and then time the movement to the next store.
 * Use the following exports to control ETL src and ETL dest.
 * <code><pre>
 * export ARCHAPPL_SHORT_TERM_FOLDER=/dev/shm/test
 * export ARCHAPPL_MEDIUM_TERM_FOLDER=/scratch/LargeDisk/ArchiverStore
 * </pre></code>
 *
 * @author mshankar
 *
 */
public class ETLTimeTest {
    private static final Logger logger = LogManager.getLogger(ETLTimeTest.class.getName());
    private static final int testSize = 1;
    String shortTermFolderName =
            ConfigServiceForTests.getDefaultShortTermFolder() + "/" + ETLTimeTest.class.getSimpleName() + "/shortTerm";
    String mediumTermFolderName =
            ConfigServiceForTests.getDefaultPBTestFolder() + "/" + ETLTimeTest.class.getSimpleName() + "/mediumTerm";
    ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
    private ConfigServiceForTests configService;

    private static Stream<Arguments> provideTestTime() {
        return Stream.of(
                Arguments.of(PBCompressionMode.NONE, PBCompressionMode.NONE),
                Arguments.of(
                        PBCompressionMode.NONE,
                        PBCompressionMode.valueOf("ZIP_PER_PV")),
                Arguments.of(
                        PBCompressionMode.valueOf("ZIP_PER_PV"),
                        PBCompressionMode.valueOf("ZIP_PER_PV")));
    }

    private static double getDataSizeInGBPerHour(CountFiles stsSizeVisitor) {
        return stsSizeVisitor.totalSize / (1024.0 * 1024.0 * 1024.0);
    }

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        if (new File(shortTermFolderName).exists()) {
            FileUtils.deleteDirectory(new File(shortTermFolderName));
        }
        if (new File(mediumTermFolderName).exists()) {
            FileUtils.deleteDirectory(new File(mediumTermFolderName));
        }

        assert new File(shortTermFolderName).mkdirs();
        assert new File(mediumTermFolderName).mkdirs();
    }

    @AfterEach
    public void tearDown() throws Exception {
        configService.shutdownNow();
    }

    @ParameterizedTest
    @MethodSource("provideTestTime")
    public void testTime(
        PBCompressionMode srcCompression, PBCompressionMode destCompression)
            throws Exception {
        PlainStoragePlugin stsStoragePlugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=STS&rootFolder="
                        + shortTermFolderName + "&partitionGranularity=PARTITION_HOUR&compress="
                        + srcCompression,
                configService);
        PlainStoragePlugin mtsStoragePlugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=MTS&rootFolder="
                        + mediumTermFolderName + "&partitionGranularity=PARTITION_YEAR&compress="
                        + destCompression,
                configService);
        short currentYear = TimeUtils.getCurrentYear();

        ArrayList<String> pvs = new ArrayList<String>();
        for (int i = 0; i < testSize; i++) {
            int tableName = 0;
            String pvName = "ArchUnitTest" + tableName + srcCompression + destCompression + ":ETLTimeTest" + i;
            PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
            String[] dataStores =
                    new String[] {stsStoragePlugin.getURLRepresentation(), mtsStoragePlugin.getURLRepresentation()};
            typeInfo.setDataStores(dataStores);
            configService.updateTypeInfoForPV(pvName, typeInfo);
            configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
            pvs.add(pvName);
        }
        configService.getETLLookup().manualControlForUnitTests();

        logger.info("Generating data for " + pvs.size() + " pvs");
        for (int m = 0; m < pvs.size(); m++) {
            String pvnameTemp = pvs.get(m);
            try (BasicContext context = new BasicContext()) {
                // Generate subset of data for one hour. We vary the amount of data we generate to mimic LCLS
                // distribution...
                int totalNum = 1;
                if (m < pvs.size() / 4) {
                    totalNum = 10 * PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk();
                } else if (m < pvs.size() / 2) {
                    totalNum = PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk();
                }

                ArrayListEventStream testData =
                        new ArrayListEventStream(totalNum, new RemotableEventStreamDesc(type, pvnameTemp, currentYear));
                for (int s = 0; s < totalNum; s++) {
                    testData.add(
                            new SimulationEvent(s * 10, currentYear, type, new ScalarValue<Double>((double) s * 10)));
                }
                stsStoragePlugin.appendData(context, pvnameTemp, testData);
            }
        }
        logger.info("Done generating data for " + pvs.size() + " pvs");
        CountFiles stsSizeVisitor = new CountFiles();
        Files.walkFileTree(Paths.get(shortTermFolderName), stsSizeVisitor);

        long time1 = System.currentTimeMillis();
        YearSecondTimestamp yts = new YearSecondTimestamp((short) (currentYear + 1), 6 * 60 * 24 * 10 + 100, 0);
        Instant etlTime = TimeUtils.convertFromYearSecondTimestamp(yts);
        logger.info("Running ETL as if it was " + TimeUtils.convertToHumanReadableString(etlTime));
        ETLExecutor.runETLs(configService, etlTime);

        for (int i = 0; i < 5; i++) {
            logger.info("Calling sync " + i);
            ProcessBuilder pBuilder = new ProcessBuilder("sync");
            pBuilder.inheritIO();
            int exitValue = pBuilder.start().waitFor();
            Assertions.assertEquals(0, exitValue, "Nonzero exit from sync " + exitValue);
        }

        long time2 = System.currentTimeMillis();
        DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");

        logEstimates(time2, time1, pvs, stsSizeVisitor, twoSignificantDigits);

        // No pb files should exist in short term folder...
        CountFiles postETLSrcVisitor = new CountFiles();
        Files.walkFileTree(Paths.get(shortTermFolderName), postETLSrcVisitor);
        logger.info("File size left in src folder " + getDataSizeInGBPerHour(postETLSrcVisitor));
        CountFiles postETLDestVisitor = new CountFiles();
        Files.walkFileTree(Paths.get(mediumTermFolderName), postETLDestVisitor);
        logger.info("File size left in dest folder " + getDataSizeInGBPerHour(postETLDestVisitor));

        Assertions.assertEquals(
                srcCompression != PBCompressionMode.ZIP_PER_PV ? 0 : pvs.size(),
                postETLSrcVisitor.filesPresent,
                "We have some files that have not moved " + postETLSrcVisitor.filesPresent);
        int expectedFiles = pvs.size();
        Assertions.assertEquals(
                postETLDestVisitor.filesPresent,
                expectedFiles,
                "Dest file count " + postETLDestVisitor.filesPresent + " is not the same as PV count " + pvs.size());

        if (postETLSrcVisitor.filesPresent == 0) {
            FileUtils.deleteDirectory(new File(shortTermFolderName));
            FileUtils.deleteDirectory(new File(mediumTermFolderName));
        }

        logger.info(configService.getETLLookup().getApplianceMetrics().details(configService));
    }

    private void logEstimates(
            long time2,
            long time1,
            ArrayList<String> pvs,
            CountFiles stsSizeVisitor,
            DecimalFormat twoSignificantDigits) {
        double testSizePVEstimateTimeSecs = ((time2 - time1) / 1000.0) * ((double) testSize / pvs.size());
        double dataSizeInGBPerHour = getDataSizeInGBPerHour(stsSizeVisitor);
        double fudgeFactor = 5.0; // Inner sectors; read/write; varying event rates etc.
        logger.info("Time for moving "
                + pvs.size() + " pvs"
                + " with data " + twoSignificantDigits.format(dataSizeInGBPerHour) + "(GB/Hr) and "
                + twoSignificantDigits.format(dataSizeInGBPerHour * 24) + "(GB/day)"
                + " from " + shortTermFolderName
                + " to " + mediumTermFolderName
                + " in " + (time2 - time1) + "(ms)."
                + " Estimated time for " + testSize + " PVs is "
                + twoSignificantDigits.format(testSizePVEstimateTimeSecs) + "(s)"
                + " Estimated capacity consumed = "
                + twoSignificantDigits.format(testSizePVEstimateTimeSecs * 100 * fudgeFactor / 3600.0));
    }

    static class CountFiles implements FileVisitor<Path> {
        public long filesPresent = 0;
        public long totalSize = 0;

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            filesPresent++;
            totalSize += Files.size(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }
    }
}
