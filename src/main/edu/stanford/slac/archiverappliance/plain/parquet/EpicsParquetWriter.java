package edu.stanford.slac.archiverappliance.plain.parquet;

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;

/**
 * A {@link ParquetWriter} for EPICS Archiver Appliance data.
 *
 * @author Sky Brewer
 */
public class EpicsParquetWriter extends ParquetWriter<Message> {
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
            WriteSupport<Message> writeSupport,
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
     * @return The builder.
     */
    public static EpicsParquetWriter.Builder builder(OutputFile file) {
        return new EpicsParquetWriter.Builder(file);
    }

    private static EpicsWriteSupport<Message> writeSupport(
            Class<? extends Message> messageClass, String pvName, short year, ArchDBRTypes archDBRTypes) {
        return new EpicsWriteSupport<>(messageClass, pvName, year, archDBRTypes);
    }

    public static class Builder extends ParquetWriter.Builder<Message, Builder> {
        Class<? extends Message> messageClass = null;
        String pvName;
        short year;
        ArchDBRTypes archDBRTypes;

        private Builder(OutputFile file) {
            super(file);
        }

        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<Message> getWriteSupport(Configuration conf) {
            return getWriteSupport((ParquetConfiguration) null);
        }

        @Override
        protected WriteSupport<Message> getWriteSupport(ParquetConfiguration conf) {
            return EpicsParquetWriter.writeSupport(this.messageClass, this.pvName, this.year, this.archDBRTypes);
        }

        public EpicsParquetWriter.Builder withMessage(Class<? extends Message> messageClass) {
            this.messageClass = messageClass;
            return this;
        }

        public EpicsParquetWriter.Builder withPVName(String pvName) {
            this.pvName = pvName;
            return this;
        }

        public EpicsParquetWriter.Builder withYear(short year) {
            this.year = year;
            return this;
        }

        public EpicsParquetWriter.Builder withType(ArchDBRTypes archDBRTypes) {
            this.archDBRTypes = archDBRTypes;
            return this;
        }
    }
}
