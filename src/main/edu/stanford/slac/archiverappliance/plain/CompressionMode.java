package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.plain.pb.PBCompressionMode;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Class to wrap the parquet and pb compression options.
 */
public class CompressionMode {
    public static final CompressionMode NONE =
            new CompressionMode(PBCompressionMode.NONE, CompressionCodecName.UNCOMPRESSED);

    private final CompressionCodecName parquetCompressionCodec;
    private final PBCompressionMode pbCompressionMode;

    public CompressionMode(PBCompressionMode pbCompressionMode, CompressionCodecName parquetCompressionCodec) {
        this.pbCompressionMode = pbCompressionMode;
        this.parquetCompressionCodec = parquetCompressionCodec;
    }

    public static CompressionMode valueOf(String compress) {
        if (compress == null || compress.isEmpty()) {
            return NONE;
        }
        try {
            PBCompressionMode possPBCompressionMode = PBCompressionMode.valueOf(compress);
            return new CompressionMode(possPBCompressionMode, CompressionCodecName.UNCOMPRESSED);
        } catch (IllegalArgumentException ignored) {
            return new CompressionMode(PBCompressionMode.NONE, CompressionCodecName.valueOf(compress));
        }
    }

    public String toURLString() {
        if (pbCompressionMode != PBCompressionMode.NONE) {
            return pbCompressionMode.toString();
        }
        if (parquetCompressionCodec != CompressionCodecName.UNCOMPRESSED) {
            return parquetCompressionCodec.toString();
        }
        return "";
    }

    @Override
    public String toString() {
        return "CompressionMode{" + "parquetCompressionCodec="
                + parquetCompressionCodec + ", pbCompressionMode="
                + pbCompressionMode + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressionMode that = (CompressionMode) o;

        if (getParquetCompressionCodec() != that.getParquetCompressionCodec()) return false;
        return pbCompressionMode == that.pbCompressionMode;
    }

    @Override
    public int hashCode() {
        int result = getParquetCompressionCodec().hashCode();
        result = 31 * result + pbCompressionMode.hashCode();
        return result;
    }

    public PBCompressionMode getPbCompression() {
        return pbCompressionMode;
    }

    public CompressionCodecName getParquetCompressionCodec() {
        return parquetCompressionCodec;
    }
}
