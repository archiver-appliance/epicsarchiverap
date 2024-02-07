package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.plain.pb.PBCompressionMode;

import java.util.Objects;

/**
 * Class to wrap the pb compression options.
 */
public class CompressionMode {
    public static final CompressionMode NONE =
            new CompressionMode(PBCompressionMode.NONE);

    private final PBCompressionMode pbCompressionMode;

    public CompressionMode(PBCompressionMode pbCompressionMode) {
        this.pbCompressionMode = pbCompressionMode;
    }

    public static CompressionMode valueOf(String compress) {
        if (compress == null || compress.isEmpty()) {
            return NONE;
        }
        try {
            PBCompressionMode possPBCompressionMode = PBCompressionMode.valueOf(compress);
            return new CompressionMode(possPBCompressionMode);
        } catch (IllegalArgumentException ignored) {
            return new CompressionMode(PBCompressionMode.NONE);
        }
    }

    public String toURLString() {
        if (pbCompressionMode != PBCompressionMode.NONE) {
            return pbCompressionMode.toString();
        }
        return "";
    }

    @Override
    public String toString() {
        return "CompressionMode{" + "pbCompressionMode="
                + pbCompressionMode + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(pbCompressionMode);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressionMode that = (CompressionMode) o;

        return pbCompressionMode == that.pbCompressionMode;
    }


    public PBCompressionMode getPbCompression() {
        return pbCompressionMode;
    }

}
