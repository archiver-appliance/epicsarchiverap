package edu.stanford.slac.archiverappliance.plain.parquet;

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;

/**
 * A Parquet writer for EPICS Archiver Appliance data.
 *
 * @param <T> The type of EPICSMessage to write.
 * @author Sky Brewer
 */
public class EpicsParquetWriter<T extends Message> extends ParquetWriter<T> {
    /**
     * The builder for {@link EpicsParquetWriter}.
     *
     * @param file                 The file to write to.
     * @param writeSupport         The write support to use.
     * @param compressionCodecName The compression codec to use.
     * @param blockSize            The block size to use.
     * @param pageSize             The page size to use.
     * @throws IOException If there is an error accessing the file.
     */
    public EpicsParquetWriter(
            Path file,
            WriteSupport<T> writeSupport,
            CompressionCodecName compressionCodecName,
            int blockSize,
            int pageSize)
            throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize);
    }

    /**
     * The builder for {@link EpicsParquetWriter}.
     *
     * @param file The file to write to.
     * @param <T>  The type of EPICSMessage to write.
     * @return The builder.
     */
    public static <T> EpicsParquetWriter.Builder<T> builder(Path file) {
        return new EpicsParquetWriter.Builder<>(file);
    }

    public static <T> EpicsParquetWriter.Builder<T> builder(OutputFile file) {
        return new EpicsParquetWriter.Builder<>(file);
    }

    private static <T extends Message> EpicsWriteSupport<T> writeSupport(
            Class<? extends Message> messageClass, String pvName, short year, ArchDBRTypes archDBRTypes) {
        return new EpicsWriteSupport<>(messageClass, pvName, year, archDBRTypes);
    }

    public static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {
        Class<? extends Message> messageClass = null;
        String pvName;
        short year;
        ArchDBRTypes archDBRTypes;

        private Builder(Path file) {
            super(file);
        }

        private Builder(OutputFile file) {
            super(file);
        }

        protected Builder<T> self() {
            return this;
        }

        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            return (WriteSupport<T>)
                    EpicsParquetWriter.writeSupport(this.messageClass, this.pvName, this.year, this.archDBRTypes);
        }

        public EpicsParquetWriter.Builder<T> withMessage(Class<? extends Message> messageClass) {
            this.messageClass = messageClass;
            return this;
        }

        public EpicsParquetWriter.Builder<T> withPVName(String pvName) {
            this.pvName = pvName;
            return this;
        }

        public EpicsParquetWriter.Builder<T> withYear(short year) {
            this.year = year;
            return this;
        }

        public EpicsParquetWriter.Builder<T> withType(ArchDBRTypes archDBRTypes) {
            this.archDBRTypes = archDBRTypes;
            return this;
        }
    }
}
