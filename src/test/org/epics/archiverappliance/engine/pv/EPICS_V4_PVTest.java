package org.epics.archiverappliance.engine.pv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.MetaInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.BitSet;
import java.util.Map;

/**
 * Unit tests for the static helpers of {@link EPICS_V4_PV}.
 */
public class EPICS_V4_PVTest {

    private static BitSet bits(int... indexes) {
        BitSet bitSet = new BitSet();
        for (int index : indexes) {
            bitSet.set(index);
        }
        return bitSet;
    }

    /**
     * The monitor handler only archives events whose changed-bitset shows a timestamp change;
     * everything else is treated as a property-only update and skipped. Per the PVA protocol,
     * bit 0 marks the <em>entire</em> structure as changed - that is how the initial monitor
     * snapshot on (re)connect arrives - so it must count as a timestamp change too, or the
     * first sample of every PVA channel is silently dropped.
     */
    @Test
    public void wholeStructureChangeCountsAsTimestampChange() {
        // Initial monitor snapshot on (re)connect: only bit 0 is set.
        assertTrue(EPICS_V4_PV.timeStampUpdated(bits(0), bits(4, 5, 6, 7)));
        // Even when the timestamp bits could not be determined.
        assertTrue(EPICS_V4_PV.timeStampUpdated(bits(0), bits()));
    }

    @Test
    public void timestampFieldChangeCountsAsTimestampChange() {
        assertTrue(EPICS_V4_PV.timeStampUpdated(bits(2, 5), bits(4, 5, 6, 7)));
    }

    @Test
    public void propertyOnlyChangeIsNotATimestampChange() {
        assertFalse(EPICS_V4_PV.timeStampUpdated(bits(9), bits(4, 5, 6, 7)));
        assertFalse(EPICS_V4_PV.timeStampUpdated(bits(), bits(4, 5, 6, 7)));
    }

    @Test
    public void unknownTimestampBitsArchiveNothingButTheSnapshot() {
        assertFalse(EPICS_V4_PV.timeStampUpdated(bits(3), bits()));
    }

    @Test
    public void metaInfoToStoreKeepsUnitAndNonZeroPrecision() {
        MetaInfo metaInfo = new MetaInfo();
        metaInfo.setUnit("kHz");
        metaInfo.setPrecision(3);
        assertEquals(Map.of("EGU", "kHz", "PREC", "3"), EPICS_V4_PV.metaInfoToStore(metaInfo));
    }

    @Test
    public void metaInfoToStoreSkipsMissingUnitAndZeroPrecision() {
        assertEquals(Map.of(), EPICS_V4_PV.metaInfoToStore(new MetaInfo()));
        assertEquals(Map.of(), EPICS_V4_PV.metaInfoToStore(null));

        MetaInfo unitOnly = new MetaInfo();
        unitOnly.setUnit("V");
        assertEquals(Map.of("EGU", "V"), EPICS_V4_PV.metaInfoToStore(unitOnly));

        MetaInfo precisionOnly = new MetaInfo();
        precisionOnly.setPrecision(2);
        assertEquals(Map.of("PREC", "2"), EPICS_V4_PV.metaInfoToStore(precisionOnly));
    }

    @ParameterizedTest
    @CsvSource({
        "string,      DBR_SCALAR_STRING",
        "double,      DBR_SCALAR_DOUBLE",
        "int,         DBR_SCALAR_INT",
        "uint,        DBR_SCALAR_INT",
        "byte,        DBR_SCALAR_BYTE",
        "ubyte,       DBR_SCALAR_BYTE",
        "float,       DBR_SCALAR_FLOAT",
        "short,       DBR_SCALAR_SHORT",
        "ushort,      DBR_SCALAR_SHORT",
        "enum_t,      DBR_SCALAR_ENUM",
        "string[],    DBR_WAVEFORM_STRING",
        "double[],    DBR_WAVEFORM_DOUBLE",
        "int[],       DBR_WAVEFORM_INT",
        "uint[],      DBR_WAVEFORM_INT",
        "byte[],      DBR_WAVEFORM_BYTE",
        "ubyte[],     DBR_WAVEFORM_BYTE",
        "float[],     DBR_WAVEFORM_FLOAT",
        "short[],     DBR_WAVEFORM_SHORT",
        "ushort[],    DBR_WAVEFORM_SHORT",
        "enum_t[],    DBR_WAVEFORM_ENUM",
    })
    public void determineDBRTypeMapsValueTypes(String valueTypeId, ArchDBRTypes expected) {
        assertEquals(
                expected, EPICS_V4_PV.determineDBRType("epics:nt/NTScalar:1.0", valueTypeId, valueTypeId + " value"));
    }

    @Test
    public void determineDBRTypeFallsBackToGenericBytes() {
        assertEquals(ArchDBRTypes.DBR_V4_GENERIC_BYTES, EPICS_V4_PV.determineDBRType(null, "double", "double value"));
        assertEquals(ArchDBRTypes.DBR_V4_GENERIC_BYTES, EPICS_V4_PV.determineDBRType("some_struct", null, null));
        assertEquals(
                ArchDBRTypes.DBR_V4_GENERIC_BYTES,
                EPICS_V4_PV.determineDBRType("some_struct", "unknown_type", "unknown_type value"));
    }

    @Test
    public void determineDBRTypeInspectsStructureFormats() {
        // An enum is a structure whose format starts with enum.
        assertEquals(
                ArchDBRTypes.DBR_SCALAR_ENUM,
                EPICS_V4_PV.determineDBRType("epics:nt/NTEnum:1.0", "structure", "enum_t value"));
        assertEquals(
                ArchDBRTypes.DBR_WAVEFORM_ENUM,
                EPICS_V4_PV.determineDBRType("epics:nt/NTScalarArray:1.0", "structure[]", "enum_t[] value"));
        // Any other structure is archived as generic bytes.
        assertEquals(
                ArchDBRTypes.DBR_V4_GENERIC_BYTES,
                EPICS_V4_PV.determineDBRType("some_struct", "structure", "structure value"));
        assertEquals(
                ArchDBRTypes.DBR_V4_GENERIC_BYTES,
                EPICS_V4_PV.determineDBRType("some_struct", "structure[]", "structure[] value"));
    }
}
