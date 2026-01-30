package edu.stanford.slac.archiverappliance.plain.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.stanford.slac.archiverappliance.plain.EventFileWriter;
import edu.stanford.slac.archiverappliance.plain.PlainFileHandler;
import edu.stanford.slac.archiverappliance.plain.PlainStorageType;
import edu.stanford.slac.archiverappliance.plain.URLKey;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.epics.archiverappliance.utils.simulation.SimulationValueGenerator;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ConvertFileTest {

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @CsvSource({"PB, PARQUET", "PARQUET, PB"})
    public void testConvertFile(PlainStorageType srcType, PlainStorageType destType) throws Exception {
        String pvName = "TestConvertPV_" + srcType + "_to_" + destType;
        short year = (short) TimeUtils.getCurrentYear();

        // 1. Create Source File with Data
        PlainFileHandler srcHandler = srcType.plainFileHandler();
        Path srcPath = tempDir.resolve(pvName + srcHandler.getExtensionString());

        int eventCount = 10;
        try (EventFileWriter writer =
                srcHandler.createEventFileWriter(pvName, srcPath, ArchDBRTypes.DBR_SCALAR_DOUBLE, year)) {
            for (int i = 0; i < eventCount; i++) {
                writer.append(
                        new SimulationEvent(i, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>((double) i)));
            }
        }
        assertTrue(Files.exists(srcPath), "Source file should exist");

        // 2. Perform Conversion
        PlainFileHandler destHandler = destType.plainFileHandler();
        Path destPath = tempDir.resolve(pvName + destHandler.getExtensionString());

        ConvertFile.convert(srcPath, destPath, destType);

        // 3. Verify Destination File
        assertTrue(Files.exists(destPath), "Destination file should exist");
        assertTrue(Files.size(destPath) > 0, "Destination file should not be empty");

        // 4. Verify Content Integrity
        int count = 0;
        try (EventStream stream = destHandler.getStream(pvName, destPath, ArchDBRTypes.DBR_SCALAR_DOUBLE)) {
            for (Event e : stream) {
                assertEquals(
                        (double) count,
                        ((ScalarValue) e.getSampleValue()).getValue(),
                        "Value mismatch at index " + count);
                count++;
            }
        }
        assertEquals(eventCount, count, "Event count mismatch");

        // 5. Verify Source File Still Exists (non-destructive)
        assertTrue(Files.exists(srcPath), "Source file should still exist after conversion");
    }

    @ParameterizedTest
    @CsvSource({"PB, PB", "PARQUET, PARQUET"})
    public void testSameTypeConversionThrowsException(PlainStorageType type) throws Exception {
        String pvName = "TestSameType_" + type;
        short year = (short) TimeUtils.getCurrentYear();

        PlainFileHandler handler = type.plainFileHandler();
        Path srcPath = tempDir.resolve(pvName + handler.getExtensionString());
        // Create dummy file
        try (EventFileWriter writer =
                handler.createEventFileWriter(pvName, srcPath, ArchDBRTypes.DBR_SCALAR_DOUBLE, year)) {
            writer.append(new SimulationEvent(0, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>(1.0)));
        }

        Path destPath = tempDir.resolve(pvName + "_copy" + handler.getExtensionString());

        assertThrows(IllegalArgumentException.class, () -> {
            ConvertFile.convert(srcPath, destPath, type);
        });
    }

    private static Stream<Arguments> provideConvertVariations() {
        List<Arguments> args = new ArrayList<>();
        int[] counts = {10, 1000, 10000, 100000};

        for (int count : counts) {
            // PB -> Parquet (Uncompressed)
            args.add(Arguments.of(
                    PlainStorageType.PB,
                    PlainStorageType.PARQUET,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    count,
                    "UNCOMPRESSED"));
            // PB -> Parquet (ZSTD)
            args.add(Arguments.of(
                    PlainStorageType.PB, PlainStorageType.PARQUET, ArchDBRTypes.DBR_SCALAR_DOUBLE, count, "ZSTD"));
            // PB -> Parquet (SNAPPY)
            args.add(Arguments.of(
                    PlainStorageType.PB, PlainStorageType.PARQUET, ArchDBRTypes.DBR_SCALAR_DOUBLE, count, "SNAPPY"));
            // Parquet -> PB
            args.add(Arguments.of(
                    PlainStorageType.PARQUET, PlainStorageType.PB, ArchDBRTypes.DBR_SCALAR_DOUBLE, count, "NONE"));

            // Varied Types (subset of counts to save time, or full range if desired)
            args.add(Arguments.of(
                    PlainStorageType.PB, PlainStorageType.PARQUET, ArchDBRTypes.DBR_WAVEFORM_DOUBLE, count, "ZSTD"));
            args.add(Arguments.of(
                    PlainStorageType.PARQUET, PlainStorageType.PB, ArchDBRTypes.DBR_SCALAR_STRING, count, "NONE"));
        }
        return args.stream();
    }

    @ParameterizedTest
    @MethodSource("provideConvertVariations")
    public void testConvertFileVariations(
            PlainStorageType srcType, PlainStorageType destType, ArchDBRTypes dbrType, int count, String compression)
            throws Exception {
        String pvName = "TestVar_" + srcType + "_" + destType + "_" + dbrType + "_" + count + "_" + compression;
        short year = (short) TimeUtils.getCurrentYear();

        // 1. Generate Data
        PlainFileHandler srcHandler = srcType.plainFileHandler();
        Path srcPath = tempDir.resolve(pvName + srcHandler.getExtensionString());

        SimulationValueGenerator generator = new SineGenerator(0);

        try (EventFileWriter writer = srcHandler.createEventFileWriter(pvName, srcPath, dbrType, year)) {
            for (int i = 0; i < count; i++) {
                SampleValue val = generator.getSampleValue(dbrType, i);
                writer.append(new SimulationEvent(i, year, dbrType, val));
            }
        }
        assertTrue(Files.exists(srcPath));

        // 2. Configure Destination Handler
        PlainFileHandler destHandler = destType.plainFileHandler();
        if (destType == PlainStorageType.PARQUET
                && compression != null
                && !compression.equals("NONE")
                && !compression.equals("UNCOMPRESSED")) {
            Map<String, String> options = new java.util.HashMap<>();
            options.put(URLKey.COMPRESS.key(), compression);
            destHandler.initCompression(options);
        }

        Path destPath = tempDir.resolve(pvName + destHandler.getExtensionString());

        // 3. Convert
        ConvertFile.convert(srcPath, destPath, destHandler);

        // 4. Verify
        assertTrue(Files.exists(destPath));

        int readCount = 0;
        try (EventStream stream = destHandler.getStream(pvName, destPath, dbrType)) {
            for (Event e : stream) {
                // Verify value match for Double
                if (dbrType == ArchDBRTypes.DBR_SCALAR_DOUBLE) {
                    org.epics.archiverappliance.data.SampleValue expectedVal =
                            generator.getSampleValue(dbrType, readCount);
                    assertEquals(
                            expectedVal.getValue().doubleValue(),
                            e.getSampleValue().getValue().doubleValue(),
                            0.0001,
                            "Value mismatch at index " + readCount);
                }
                readCount++;
            }
        }
        assertEquals(count, readCount, "Event count mismatch");
    }
}
