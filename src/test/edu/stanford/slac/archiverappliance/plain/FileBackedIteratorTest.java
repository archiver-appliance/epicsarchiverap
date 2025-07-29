package edu.stanford.slac.archiverappliance.plain;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.EmptyEventIterator;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.Iterator;
import java.util.stream.Stream;

import static edu.stanford.slac.archiverappliance.plain.PlainPBStoragePlugin.pbFileExtension;
import static edu.stanford.slac.archiverappliance.plain.PlainPBStoragePlugin.pbFileSuffix;

/**
 * The FileBackedPBEventStream supports two iterators - one is a file-position based one and the other is a time based one.
 * For performance reasons, we should use the file-position based iterator in cases where the query start time is after the timestamp of the first sample; defaulting to the time based one in case of unexpected circumstances.
 * This test makes sure we have this behavior; these are the test cases we are expected to test.
 * <ol>
 * <li>FKTS - The timestamp of the first sample in the file</li>
 * <li>LKTS - The timestamp of the last sample in the file</li>
 * <li>QTS - The starttime of the query/request</li>
 * <li>QTE - The endtime of the query/request</li>
 * <li></li>
 * </ol>
 * <pre>
 *                 FKTS                                                      LKTS
 *  1 - QTS -- QTE  |                                                         |
 *  2 - QTS --------|-------- QTE                                             |
 *  3 - QTS --------|---------------------------------------------------------|-------------------------------- QTE
 *  4 -             |          QTS ----------------- QTE                      |
 *  5 -             |          QTS -------------------------------------------|-------------------------------- QTE
 *  6 -             |                                                         |           QTS ----------------- QTE
 *
 * </pre>
 *
 * @author mshankar
 *
 */
public class FileBackedIteratorTest {
    private static final Logger logger = LogManager.getLogger(FileBackedIteratorTest.class.getName());
    private static final File testFolder =
            new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "FileBackedIteratorTest");
    private static final String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":FileBackedIteratorTest";
    private static final short currentYear = TimeUtils.getCurrentYear();
    private static final Path pbFilePath = Paths.get(
            testFolder.getAbsolutePath(),
            pvName.replace(":", "/").replace("--", "") + ":" + currentYear + pbFileExtension);
    private static final ConfigService configService;

    static {
        try {
            configService = new ConfigServiceForTests(-1);
        } catch (ConfigException e) {
            throw new RuntimeException(e);
        }
    }

    ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;

    @BeforeAll
    public static void setUp() throws Exception {
        try {
            generateData();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {
        FileUtils.deleteDirectory(testFolder);
    }

    public static Stream<Arguments> provideCorrectIterator() {

        PBFileInfo fileInfo;
        try {
            fileInfo = new PBFileInfo(pbFilePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Instant FKTS = fileInfo.getFirstEvent().getEventTimeStamp();
        Instant LKTS = fileInfo.getLastEvent().getEventTimeStamp();
        logger.info("After generating data," + "FKTS = "
                + TimeUtils.convertToISO8601String(FKTS) + "LKTS = "
                + TimeUtils.convertToISO8601String(LKTS));

        var mainIteratorClass = FileBackedPBEventStreamPositionBasedIterator.class;
        return Stream.of(
                Arguments.of(
                        "Case 1",
                        TimeUtils.minusDays(FKTS, 5),
                        TimeUtils.minusDays(FKTS, 2),
                        TimeUtils.minusDays(FKTS, 4),
                        FKTS,
                        EmptyEventIterator.class),
                Arguments.of(
                        "Case 2",
                        TimeUtils.minusDays(FKTS, 5),
                        TimeUtils.minusDays(FKTS, 2),
                        TimeUtils.plusDays(FKTS, 1),
                        TimeUtils.plusDays(FKTS, 10),
                        mainIteratorClass),
                Arguments.of(
                        "Case 3",
                        TimeUtils.minusDays(FKTS, 5),
                        TimeUtils.minusDays(FKTS, 1),
                        TimeUtils.plusDays(LKTS, 1),
                        TimeUtils.plusDays(LKTS, 10),
                        mainIteratorClass),
                Arguments.of(
                        "Case 4",
                        FKTS,
                        TimeUtils.plusDays(FKTS, 5),
                        TimeUtils.minusDays(LKTS, 10),
                        TimeUtils.minusDays(LKTS, 1),
                        mainIteratorClass),
                Arguments.of(
                        "Case 5",
                        FKTS,
                        TimeUtils.plusDays(FKTS, 5),
                        LKTS,
                        TimeUtils.plusDays(LKTS, 10),
                        mainIteratorClass),
                Arguments.of(
                        "Case 6",
                        TimeUtils.plusDays(LKTS, 1),
                        TimeUtils.plusDays(LKTS, 10),
                        TimeUtils.plusDays(LKTS, 1),
                        TimeUtils.plusDays(LKTS, 10),
                        mainIteratorClass));
    }

    private static void generateData() throws IOException {
        logger.info("generate Data " + pbFileExtension + " to " + pbFilePath);
        PlainPBStoragePlugin storagePlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pbFileSuffix + "://localhost?name=FileBackedIteratorTest&rootFolder=" + testFolder.getAbsolutePath()
                        + "&partitionGranularity=PARTITION_YEAR",
                FileBackedIteratorTest.configService);

        // Add data with gaps every month
        DecimalFormat monthFmt = new DecimalFormat("00");
        ArrayListEventStream strm = new ArrayListEventStream(
                PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 12 * 10,
                new RemotableEventStreamDesc(
                        ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, FileBackedIteratorTest.currentYear));
        for (int month = 1; month < 12; month++) {
            long startOfMonthEpochSeconds = TimeUtils.convertToEpochSeconds(TimeUtils.convertFromISO8601String(
                    FileBackedIteratorTest.currentYear + "-" + monthFmt.format(month + 1) + "-01T08:00:00.000Z"));
            // Generate data for  10 days
            for (int day = 0; day < 10; day++) {
                for (int second = 0;
                        second < PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                        second += 15) {
                    strm.add(new POJOEvent(
                            ArchDBRTypes.DBR_SCALAR_DOUBLE,
                            TimeUtils.convertFromEpochSeconds(
                                    startOfMonthEpochSeconds
                                            + (long) day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk()
                                            + second,
                                    0),
                            new ScalarValue<>((double) second),
                            0,
                            0));
                }
            }
        }
        try (BasicContext context = new BasicContext()) {
            assert storagePlugin != null;
            storagePlugin.appendData(context, pvName, strm);
        }
    }

    /**
     * Make sure we get the expected iterator
     */
    @ParameterizedTest
    @MethodSource("provideCorrectIterator")
    public void makeSureWeGetCorrectIterator(
            String testCase,
            Instant minQTS,
            Instant maxQTS,
            Instant minQTE,
            Instant maxQTE,
            Class<? extends Iterator<Event>> expectedIteratorClass)
            throws IOException {
        for (Instant QTS = minQTS; QTS.isBefore(maxQTS); QTS = TimeUtils.plusDays(QTS, 1)) {
            for (Instant QTE = minQTE; QTE.isBefore(maxQTE); QTE = TimeUtils.plusDays(QTE, 1)) {
                if (QTS.equals(QTE) || QTS.isAfter(QTE)) {
                    continue;
                }
                logger.debug("Checking " + testCase + " for QTS " + TimeUtils.convertToISO8601String(QTS) + " and QTE "
                        + TimeUtils.convertToISO8601String(QTE));

                try (EventStream strm = FileStreamCreator.getTimeStream(pvName, pbFilePath, dbrType, QTS, QTE, false)) {
                    Assertions.assertSame(
                            expectedIteratorClass,
                            strm.iterator().getClass(),
                            "We are not getting the expected iterator " + expectedIteratorClass.getName()
                                    + " for " + testCase
                                    + " for QTS " + TimeUtils.convertToISO8601String(QTS)
                                    + " and QTE " + TimeUtils.convertToISO8601String(QTE));
                }
            }
        }
    }
}
