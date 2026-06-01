package edu.stanford.slac.archiverappliance.PB.data;

import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBR_TIME_Double;
import gov.aps.jca.dbr.DBR_TIME_Enum;
import gov.aps.jca.dbr.DBR_TIME_Float;
import gov.aps.jca.dbr.DBR_TIME_Int;
import gov.aps.jca.dbr.DBR_TIME_Short;
import gov.aps.jca.dbr.DBR_TIME_String;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the two invariants of the PB on-disk line-framing format:
 *
 * <ul>
 *   <li><b>Write:</b> {@code getRawForm()} must not contain bare LF, CR, or ESC bytes.
 *       Any such bytes in protobuf field encodings must be escaped so that
 *       {@code readLine()} can split safely on bare LF.</li>
 *   <li><b>Read:</b> if a record is truncated at a bare LF (hardware corruption,
 *       external data, etc.), the read path must recover the event rather than
 *       drop it silently.</li>
 * </ul>
 */
public class PBLineFramingTest {

    /** Field 5 (status), wire type 0 (varint), with no value byte — what readLine() leaves behind. */
    private static final byte[] STATUS_TAG_ONLY = {0x28};

    /**
     * For all 14 PB types: feed a record consisting of only the status field tag byte
     * ({@code [0x28]}, simulating readLine() having consumed the bare LF value byte) and
     * confirm that status=10 is recovered rather than a {@code PBParseException} being thrown.
     */
    @ParameterizedTest(name = "{0}")
    @EnumSource(
            value = ArchDBRTypes.class,
            names = {
                "DBR_SCALAR_ENUM",
                "DBR_SCALAR_DOUBLE",
                "DBR_SCALAR_INT",
                "DBR_SCALAR_FLOAT",
                "DBR_SCALAR_SHORT",
                "DBR_SCALAR_BYTE",
                "DBR_SCALAR_STRING",
                "DBR_WAVEFORM_DOUBLE",
                "DBR_WAVEFORM_INT",
                "DBR_WAVEFORM_FLOAT",
                "DBR_WAVEFORM_SHORT",
                "DBR_WAVEFORM_BYTE",
                "DBR_WAVEFORM_STRING",
                "DBR_WAVEFORM_ENUM",
                "DBR_V4_GENERIC_BYTES"
            })
    public void testReadRecovery(ArchDBRTypes type) throws Exception {
        short year = (short) TimeUtils.getCurrentYear();
        DBRTimeEvent event = DBR2PBTypeMapping.getPBClassFor(type)
                .getUnmarshallingFromByteArrayConstructor()
                .newInstance(year, new ByteArray(STATUS_TAG_ONLY.clone()));
        Assertions.assertEquals(10, event.getStatus(), type + ": expected status=10 recovered from truncated record");
    }

    /**
     * JCA ingestion path for ScalarEnum: confirms the primary Channel-Access write path
     * escapes status values whose varint encoding is a line-framing special byte.
     */
    @ParameterizedTest(name = "ScalarEnum status={0}")
    @ValueSource(ints = {10, 13})
    public void testScalarEnumJCAEscaping(int status) throws Exception {
        DBR_TIME_Enum dbr = new DBR_TIME_Enum(new short[] {0});
        dbr.setTimeStamp(new gov.aps.jca.dbr.TimeStamp(TimeUtils.getStartOfCurrentYearInSeconds() + 100));
        dbr.setSeverity(1);
        dbr.setStatus(status);

        DBRTimeEvent event = (DBRTimeEvent) EPICS2PBTypeMapping.getPBClassFor(ArchDBRTypes.DBR_SCALAR_ENUM)
                .getJCADBRConstructor()
                .newInstance(dbr);

        assertProperlyEscaped(event.getRawForm(), "DBR_SCALAR_ENUM");
        Assertions.assertEquals(status, event.getStatus(), "getStatus() round-trip failed for status=" + status);
    }

    /** Same JCA escaping invariant across several V3 scalar types. */
    @ParameterizedTest(name = "multi-type status={0}")
    @ValueSource(ints = {10, 13})
    public void testScalarJCAEscaping(int status) throws Exception {
        DBR[] dbrs = {
            makeDouble(100, 1, status),
            makeInt(100, 1, status),
            makeFloat(100, 1, status),
            makeShort(100, 1, status),
            makeString(100, 1, status),
        };
        ArchDBRTypes[] types = {
            ArchDBRTypes.DBR_SCALAR_DOUBLE,
            ArchDBRTypes.DBR_SCALAR_INT,
            ArchDBRTypes.DBR_SCALAR_FLOAT,
            ArchDBRTypes.DBR_SCALAR_SHORT,
            ArchDBRTypes.DBR_SCALAR_STRING,
        };

        for (int i = 0; i < dbrs.length; i++) {
            DBRTimeEvent event = (DBRTimeEvent) EPICS2PBTypeMapping.getPBClassFor(types[i])
                    .getJCADBRConstructor()
                    .newInstance(dbrs[i]);
            assertProperlyEscaped(event.getRawForm(), types[i].name());
            Assertions.assertEquals(
                    status, event.getStatus(), types[i] + ": getStatus() round-trip failed for status=" + status);
        }
    }

    private static void assertProperlyEscaped(ByteArray raw, String typeName) {
        Assertions.assertNotNull(raw, typeName + ": getRawForm() returned null");
        byte[] bytes = raw.toBytes();
        Assertions.assertNotNull(bytes, typeName + ": getRawForm().toBytes() returned null");
        Assertions.assertTrue(bytes.length > 0, typeName + ": getRawForm() is empty");

        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            if (b == LineEscaper.NEWLINE_CHAR) {
                Assertions.fail(typeName + ": unescaped LF (0x0a) at byte " + i);
            }
            if (b == LineEscaper.CARRIAGERETURN_CHAR) {
                Assertions.fail(typeName + ": unescaped CR (0x0d) at byte " + i);
            }
            if (b == LineEscaper.ESCAPE_CHAR) {
                Assertions.assertTrue(i + 1 < bytes.length, typeName + ": ESC at end of byte array");
                byte next = bytes[i + 1];
                Assertions.assertTrue(
                        next == LineEscaper.ESCAPE_ESCAPE_CHAR
                                || next == LineEscaper.NEWLINE_ESCAPE_CHAR
                                || next == LineEscaper.CARRIAGERETURN_ESCAPE_CHAR,
                        typeName + ": ESC followed by invalid byte 0x" + Integer.toHexString(next & 0xFF) + " at byte "
                                + i);
                i++;
            }
        }
    }

    private static DBR_TIME_Double makeDouble(int secondsIntoYear, int severity, int status) {
        DBR_TIME_Double d = new DBR_TIME_Double(new double[] {0.0});
        d.setTimeStamp(new gov.aps.jca.dbr.TimeStamp(TimeUtils.getStartOfCurrentYearInSeconds() + secondsIntoYear));
        d.setSeverity(severity);
        d.setStatus(status);
        return d;
    }

    private static DBR_TIME_Int makeInt(int secondsIntoYear, int severity, int status) {
        DBR_TIME_Int d = new DBR_TIME_Int(new int[] {0});
        d.setTimeStamp(new gov.aps.jca.dbr.TimeStamp(TimeUtils.getStartOfCurrentYearInSeconds() + secondsIntoYear));
        d.setSeverity(severity);
        d.setStatus(status);
        return d;
    }

    private static DBR_TIME_Float makeFloat(int secondsIntoYear, int severity, int status) {
        DBR_TIME_Float d = new DBR_TIME_Float(new float[] {0.0f});
        d.setTimeStamp(new gov.aps.jca.dbr.TimeStamp(TimeUtils.getStartOfCurrentYearInSeconds() + secondsIntoYear));
        d.setSeverity(severity);
        d.setStatus(status);
        return d;
    }

    private static DBR_TIME_Short makeShort(int secondsIntoYear, int severity, int status) {
        DBR_TIME_Short d = new DBR_TIME_Short(new short[] {0});
        d.setTimeStamp(new gov.aps.jca.dbr.TimeStamp(TimeUtils.getStartOfCurrentYearInSeconds() + secondsIntoYear));
        d.setSeverity(severity);
        d.setStatus(status);
        return d;
    }

    private static DBR_TIME_String makeString(int secondsIntoYear, int severity, int status) {
        DBR_TIME_String d = new DBR_TIME_String(new String[] {"0"});
        d.setTimeStamp(new gov.aps.jca.dbr.TimeStamp(TimeUtils.getStartOfCurrentYearInSeconds() + secondsIntoYear));
        d.setSeverity(severity);
        d.setStatus(status);
        return d;
    }
}
