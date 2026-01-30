package edu.stanford.slac.archiverappliance.plain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EventFileWriterTest {

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void testCreateAndWriteSingleEvent(PlainStorageType type) throws IOException {
        String pvName = "TestPV";

        short year = (short) TimeUtils.getCurrentYear();

        Path filePath = tempDir.resolve(pvName + type.plainFileHandler().getExtensionString());

        PlainFileHandler handler = type.plainFileHandler();

        try (EventFileWriter writer =
                handler.createEventFileWriter(pvName, filePath, ArchDBRTypes.DBR_SCALAR_DOUBLE, year)) {
            Event event = new SimulationEvent(0, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>(1.0));

            writer.append(event);
        }

        // Verify file exists

        assertTrue(Files.exists(filePath));

        assertTrue(Files.size(filePath) > 0);

        // Verify content

        int count = 0;

        try (EventStream stream = handler.getStream(pvName, filePath, ArchDBRTypes.DBR_SCALAR_DOUBLE)) {
            for (Event e : stream) {
                count++;

                assertEquals(1.0, ((ScalarValue) e.getSampleValue()).getValue());
            }
        }

        assertEquals(1, count);
    }

    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void testCreateAndWriteStream(PlainStorageType type) throws IOException {
        String pvName = "TestPVStream";

        short year = (short) TimeUtils.getCurrentYear();

        Path filePath = tempDir.resolve(pvName + type.plainFileHandler().getExtensionString());

        PlainFileHandler handler = type.plainFileHandler();

        List<Event> events = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            events.add(new SimulationEvent(i, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>((double) i)));
        }

        try (EventFileWriter writer =
                handler.createEventFileWriter(pvName, filePath, ArchDBRTypes.DBR_SCALAR_DOUBLE, year)) {
            EventStream stream = new EventStream() {
                @Override
                public org.epics.archiverappliance.EventStreamDesc getDescription() {
                    return null;
                }

                @Override
                public Iterator<Event> iterator() {
                    return events.iterator();
                }

                @Override
                public void close() throws IOException {}
            };

            writer.writeStreamToFile(stream);
        }

        // Verify content

        int count = 0;

        try (EventStream stream = handler.getStream(pvName, filePath, ArchDBRTypes.DBR_SCALAR_DOUBLE)) {
            for (Event e : stream) {
                assertEquals((double) count, ((ScalarValue) e.getSampleValue()).getValue());

                count++;
            }
        }

        assertEquals(100, count);
    }

    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void testClosingCausesNoProblems(PlainStorageType type) throws IOException {
        String pvName = "TestPVClose";

        short year = (short) TimeUtils.getCurrentYear();

        Path filePath = tempDir.resolve(pvName + type.plainFileHandler().getExtensionString());

        PlainFileHandler handler = type.plainFileHandler();

        EventFileWriter writer = handler.createEventFileWriter(pvName, filePath, ArchDBRTypes.DBR_SCALAR_DOUBLE, year);

        Event event = new SimulationEvent(0, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>(1.0));

        writer.append(event);

        writer.close();

        // Calling close again should be fine

        writer.close();

        // Data should be readable

        try (EventStream stream = handler.getStream(pvName, filePath, ArchDBRTypes.DBR_SCALAR_DOUBLE)) {
            assertTrue(stream.iterator().hasNext());
        }
    }

    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void testNotClosingCausesProblems(PlainStorageType type) throws IOException {
        // This test verifies that if we DON'T close the writer, the file is either empty, incomplete, or invalid.

        // NOTE: This behavior depends heavily on buffering and the specific implementation.

        String pvName = "TestPVNotClose";

        short year = (short) TimeUtils.getCurrentYear();

        Path filePath = tempDir.resolve(pvName + type.plainFileHandler().getExtensionString());

        PlainFileHandler handler = type.plainFileHandler();

        EventFileWriter writer = handler.createEventFileWriter(pvName, filePath, ArchDBRTypes.DBR_SCALAR_DOUBLE, year);

        // Write enough data to potentially fill some buffers, but maybe not flush all

        for (int i = 0; i < 1000; i++) {
            writer.append(new SimulationEvent(i, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>((double) i)));
        }

        // We explicitly DO NOT close the writer here.

        if (type == PlainStorageType.PARQUET) {
            // Parquet requires closing to write the footer. Without footer, it's invalid.
            // Depending on the reader implementation, it might throw an IOException or return empty.
            assertThrows(Exception.class, () -> {
                try (EventStream stream = handler.getStream(pvName, filePath, ArchDBRTypes.DBR_SCALAR_DOUBLE)) {
                    // Just trying to open it might fail, or iterating might fail
                    if (!stream.iterator().hasNext()) {
                        throw new IOException("Stream empty");
                    }
                    int count = 0;
                    for (Event e : stream) count++;
                    if (count != 1000) throw new IOException("Incomplete data");
                }
            });
        } else if (type == PlainStorageType.PB) {
            // PB is a stream format. It might be readable even if not closed, but data might be missing due to
            // buffering.
            writer.close();
        } else {
            writer.close();
        }
    }
}
