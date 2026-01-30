package edu.stanford.slac.archiverappliance.plain.parquet;

import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBMessageTypeMapping;
import edu.stanford.slac.archiverappliance.plain.EventFileWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ParquetEventFileWriter implements EventFileWriter {

    private static final Logger logger = LogManager.getLogger(ParquetEventFileWriter.class.getName());
    private final EpicsParquetWriter.Builder writerBuilder;
    private ParquetWriter<Message> writer;

    public ParquetEventFileWriter(
            String pvName, Path path, ArchDBRTypes type, short year, CompressionCodecName compressionCodecName)
            throws IOException {
        if (Files.exists(path)) {
            if (Files.size(path) == 0) {
                Files.delete(path);
            } else {
                throw new IOException("Trying to write a header into a file that exists " + path.toAbsolutePath());
            }
        }

        var localOutputFile = new LocalOutputFile(path);
        var messageClass = DBR2PBMessageTypeMapping.getMessageClass(type);
        if (messageClass == null) {
            throw new IOException("Cannot determine message class for type " + type);
        }

        this.writerBuilder = EpicsParquetWriter.builder(localOutputFile)
                .withMessage(messageClass)
                .withPVName(pvName)
                .withYear(year)
                .withType(type)
                .withCompressionCodec(compressionCodecName);
    }

    @Override
    public void append(Event event) throws IOException {
        if (event.getProtobufMessage() == null) {
            logger.error("event {} is null", event);
            throw new IOException("Event protobuf message is null");
        }
        if (this.writer == null) {
            this.writer = this.writerBuilder.build();
        }
        writer.write(event.getProtobufMessage());
    }

    @Override
    public void close() throws IOException {
        if (this.writer != null) {
            this.writer.close();
        }
    }
}
