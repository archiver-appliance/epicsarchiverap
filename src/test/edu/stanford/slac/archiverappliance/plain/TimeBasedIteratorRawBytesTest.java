package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import org.apache.commons.io.FileUtils;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Regression test for a bug in FileBackedPBEventStreamTimeBasedIterator.Events.popEvent()
 * where this.clear() was called before returning the event.  clear() reset line1.len to 0,
 * so the returned event's getRawForm() produced zero bytes.  In a raw HTTP response, the
 * affected events appeared as empty lines — invisible to event-count checks but silently
 * discarded by PB clients.
 *
 * <p>This test forces use of the time-based iterator (skipSearch=true) and checks that
 * every returned event has a non-empty raw form and that the raw bytes contain no
 * unescaped line-separator bytes.
 */
public class TimeBasedIteratorRawBytesTest {

    static final String PV_NAME = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "TimeBasedIteratorRawBytesTest";
    static final ArchDBRTypes TYPE = ArchDBRTypes.DBR_SCALAR_DOUBLE;

    String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/TimeBasedIteratorRawBytesTest/";

    private ConfigService configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
    }

    @AfterEach
    public void tearDown() throws Exception {
        File root = new File(rootFolderName);
        if (root.exists()) FileUtils.deleteDirectory(root);
    }

    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void testTimeBasedIteratorRawBytesNonEmpty(PlainStorageType plainStorageType) throws Exception {
        PlainStoragePlugin storagePlugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                plainStorageType.plainFileHandler().pluginIdentifier()
                        + "://localhost?name=STS&rootFolder=" + rootFolderName
                        + "&partitionGranularity=PARTITION_HOUR",
                configService);

        File rootFolder = new File(storagePlugin.getRootFolder());
        if (rootFolder.exists()) FileUtils.deleteDirectory(rootFolder);

        // Write several events spread over a few seconds so the time-based search has
        // meaningful timestamps to work with.
        try (BasicContext context = new BasicContext()) {
            ArrayListEventStream testData = new ArrayListEventStream(
                    PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                    new RemotableEventStreamDesc(TYPE, PV_NAME, (short) 2013));
            for (int i = 1; i <= 5; i++) {
                YearSecondTimestamp ts = TimeUtils.convertToYearSecondTimestamp(
                        TimeUtils.convertFromISO8601String("2013-02-21T18:45:0" + i + ".000Z"));
                testData.add(new SimulationEvent(ts, TYPE, new ScalarValue<Double>((double) i)));
            }
            storagePlugin.appendData(context, PV_NAME, testData);
        }

        try (BasicContext context = new BasicContext()) {
            Path[] paths = PathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    rootFolderName,
                    PV_NAME,
                    storagePlugin.getExtensionString(),
                    storagePlugin.getPathResolver(),
                    configService.getPVNameToKeyConverter());
            Assertions.assertEquals(1, paths.length, "Expected one PB file, got " + paths.length);

            // skipSearch=true forces FileBackedPBEventStreamTimeBasedIterator
            try (EventStream strm = storagePlugin
                    .getPlainFileHandler()
                    .getTimeStream(
                            PV_NAME,
                            paths[0],
                            TYPE,
                            TimeUtils.convertFromISO8601String("2013-02-21T18:44:00.000Z"),
                            TimeUtils.convertFromISO8601String("2013-02-21T18:46:00.000Z"),
                            true /* skipSearch → time-based iterator */)) {

                int count = 0;
                for (Event e : strm) {
                    count++;
                    DBRTimeEvent dbre = (DBRTimeEvent) e;
                    byte[] raw = dbre.getRawForm().toBytes();

                    Assertions.assertNotNull(
                            raw,
                            "Event " + count + ": getRawForm().toBytes() is null — "
                                    + "likely caused by popEvent() resetting line1 before returning the event.");
                    Assertions.assertTrue(
                            raw.length > 0,
                            "Event " + count + ": getRawForm() returned empty bytes — "
                                    + "popEvent() cleared line1.len before the event was consumed.");

                    // Also assert no bare line-separator bytes leak into the raw form.
                    for (int i = 0; i < raw.length; i++) {
                        if (raw[i] == LineEscaper.NEWLINE_CHAR) {
                            Assertions.fail("Event " + count + ": unescaped LF at byte " + i
                                    + " in raw form — line-split clients would truncate this event.");
                        }
                        if (raw[i] == LineEscaper.CARRIAGERETURN_CHAR) {
                            Assertions.fail("Event " + count + ": unescaped CR at byte " + i + " in raw form.");
                        }
                        if (raw[i] == LineEscaper.ESCAPE_CHAR) {
                            i++; // skip valid escape suffix
                        }
                    }
                }

                Assertions.assertEquals(5, count, "Expected 5 events from time-based iterator, got " + count);
            }
        }
    }

    /**
     * Confirms the "consume before advance" contract for getRawForm().
     *
     * <p>Bytes captured via toBytes() immediately after next() are correct, and consecutive
     * events produce distinct raw forms. This documents that getRawForm() on a returned event
     * is valid only until the iterator next advances — callers that need to span events across
     * iterations must use event.makeClone() instead.
     */
    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void testRawFormCapturedBeforeAdvanceIsCorrect(PlainStorageType plainStorageType) throws Exception {
        PlainStoragePlugin storagePlugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                plainStorageType.plainFileHandler().pluginIdentifier()
                        + "://localhost?name=STS&rootFolder=" + rootFolderName
                        + "&partitionGranularity=PARTITION_HOUR",
                configService);

        File rootFolder = new File(storagePlugin.getRootFolder());
        if (rootFolder.exists()) FileUtils.deleteDirectory(rootFolder);

        try (BasicContext context = new BasicContext()) {
            ArrayListEventStream testData = new ArrayListEventStream(
                    PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                    new RemotableEventStreamDesc(TYPE, PV_NAME, (short) 2013));
            for (int i = 1; i <= 2; i++) {
                YearSecondTimestamp ts = TimeUtils.convertToYearSecondTimestamp(
                        TimeUtils.convertFromISO8601String("2013-02-21T18:45:0" + i + ".000Z"));
                testData.add(new SimulationEvent(ts, TYPE, new ScalarValue<Double>((double) i)));
            }
            storagePlugin.appendData(context, PV_NAME, testData);
        }

        try (BasicContext context = new BasicContext()) {
            Path[] paths = PathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    rootFolderName,
                    PV_NAME,
                    storagePlugin.getExtensionString(),
                    storagePlugin.getPathResolver(),
                    configService.getPVNameToKeyConverter());
            Assertions.assertEquals(1, paths.length, "Expected one PB file");

            try (EventStream strm = storagePlugin
                    .getPlainFileHandler()
                    .getTimeStream(
                            PV_NAME,
                            paths[0],
                            TYPE,
                            TimeUtils.convertFromISO8601String("2013-02-21T18:44:00.000Z"),
                            TimeUtils.convertFromISO8601String("2013-02-21T18:46:00.000Z"),
                            true /* skipSearch → time-based iterator */)) {

                Iterator<Event> it = strm.iterator();

                // Consume raw form of event A immediately — before calling hasNext()/next() again.
                // This mirrors what all production callers do (PBRAWResponse, PBEventFileWriter, etc.).
                Assertions.assertTrue(it.hasNext(), "Expected at least one event");
                byte[] rawA = ((DBRTimeEvent) it.next()).getRawForm().toBytes();
                Assertions.assertNotNull(rawA, "Event A: getRawForm().toBytes() is null when consumed before advance");
                Assertions.assertTrue(
                        rawA.length > 0, "Event A: getRawForm().toBytes() is empty when consumed before advance");

                // Advance to event B and similarly consume its raw form.
                Assertions.assertTrue(it.hasNext(), "Expected a second event");
                byte[] rawB = ((DBRTimeEvent) it.next()).getRawForm().toBytes();
                Assertions.assertNotNull(rawB, "Event B: getRawForm().toBytes() is null");
                Assertions.assertTrue(rawB.length > 0, "Event B: getRawForm().toBytes() is empty");

                // Events A and B encode different values (1.0 and 2.0); their raw forms must differ.
                Assertions.assertFalse(
                        Arrays.equals(rawA, rawB),
                        "Consecutive events with different values must have distinct raw forms");
            }
        }
    }
}
