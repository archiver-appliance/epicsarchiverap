package edu.stanford.slac.archiverappliance.PB.data;

import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;

import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Centralises protobuf parse-and-recover logic for all 14 PB event type classes.
 *
 * <p>The PB on-disk format uses newline (LF, 0x0a) as the record separator.
 * {@code LineByteStream.readLine()} splits on bare 0x0a bytes; write-time escaping via
 * {@code LineEscaper.escapeNewLines()} is the guarantee that no 0x0a appears unescaped within
 * a record body under normal conditions. If a record nonetheless contains a bare 0x0a — due to
 * hardware corruption, data from an external source, or any other cause — {@code readLine()}
 * truncates the record at that byte, leaving an orphaned varint field tag in the
 * {@code ByteArray}. A plain {@code mergeFrom()} then fails with
 * {@code InvalidProtocolBufferException}.
 *
 * <p>Recovery: if the initial parse fails and the last unescaped byte is an orphaned varint
 * field tag (wire type 0, non-zero), append 0x0a (the byte consumed by {@code readLine()}) and
 * retry with {@code buildPartial()}. This reconstructs the truncated field value and avoids a
 * silent event drop.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class PBEventRecovery {

    private static final Logger logger = LogManager.getLogger(PBEventRecovery.class);

    private static final int PROTO_WIRE_TYPE_MASK = 0x07;
    private static final int PROTO_WIRE_TYPE_VARINT = 0x00;

    private PBEventRecovery() {} // private constructor to prevent instantiation

    /**
     * Unescapes {@code bar} and parses it with the supplied builder factory. If the first parse
     * fails and the last byte looks like an orphaned varint field tag (wire type 0, non-zero),
     * appends 0x0a (the LF byte that {@code readLine()} consumed as a record separator) and
     * retries once with {@code buildPartial()}. Throws {@link PBParseException} if neither
     * attempt succeeds.
     *
     * <p>Usage in each PB event class:
     * <pre>
     *   dbevent = PBEventRecovery.parseWithRecovery(EPICSEvent.ScalarEnum::newBuilder, bar);
     * </pre>
     */
    public static <M> M parseWithRecovery(
            Supplier<com.google.protobuf.AbstractMessageLite.Builder> builderFactory, ByteArray bar) {
        bar.inPlaceUnescape();
        try {
            return (M) builderFactory
                    .get()
                    .mergeFrom(bar.unescapedData, bar.off, bar.unescapedLen)
                    .build();
        } catch (Exception ex) {
            int len = bar.unescapedLen;
            if (len > 0) {
                byte last = bar.unescapedData[bar.off + len - 1];
                // readLine() may have consumed the 0x0a value byte of a varint field, leaving an
                // orphaned field tag. Protobuf wire type 0 (varint) has its low 3 bits clear;
                // re-appending 0x0a restores the byte that readLine() consumed.
                if (last != 0 && (last & PROTO_WIRE_TYPE_MASK) == PROTO_WIRE_TYPE_VARINT) {
                    byte[] recovered = Arrays.copyOfRange(bar.unescapedData, bar.off, bar.off + len + 1);
                    recovered[len] = LineEscaper.NEWLINE_CHAR;
                    try {
                        M result = (M) builderFactory.get().mergeFrom(recovered).buildPartial();
                        logger.warn(
                                "Recovered PB event truncated by bare LF: varint tag 0x{} had no value byte",
                                String.format("%02x", last & 0xFF));
                        return result;
                    } catch (Exception ignored) {
                        // fall through to PBParseException
                    }
                }
            }
            throw new PBParseException(bar.toBytes(), ex);
        }
    }
}
