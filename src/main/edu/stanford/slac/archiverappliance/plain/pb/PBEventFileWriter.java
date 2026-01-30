package edu.stanford.slac.archiverappliance.plain.pb;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import edu.stanford.slac.archiverappliance.plain.EventFileWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PBEventFileWriter implements EventFileWriter {

    private static final Logger logger = LogManager.getLogger(PBEventFileWriter.class.getName());
    private final OutputStream os;

    public PBEventFileWriter(String pvName, Path path, ArchDBRTypes type, short year, boolean append)
            throws IOException {
        if (!append && Files.exists(path) && Files.size(path) > 0) {
            throw new IOException("Trying to write a header into a file that exists " + path.toAbsolutePath());
        }

        StandardOpenOption[] options = append
                ? new StandardOpenOption[] {
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND,
                }
                : new StandardOpenOption[] {
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                };

        this.os = new BufferedOutputStream(Files.newOutputStream(path, options));

        if (!append) {
            byte[] headerBytes = LineEscaper.escapeNewLines(PayloadInfo.newBuilder()
                    .setPvname(pvName)
                    .setType(type.getPBPayloadType())
                    .setYear(year)
                    .build()
                    .toByteArray());
            this.os.write(headerBytes);
            this.os.write(LineEscaper.NEWLINE_CHAR);
        }
    }

    public PBEventFileWriter(String pvName, Path path, ArchDBRTypes type, short year) throws IOException {
        this(pvName, path, type, year, false);
    }

    @Override
    public void append(Event event) throws IOException {
        ByteArray val = event.getRawForm();
        this.os.write(val.data, val.off, val.len);
        this.os.write(LineEscaper.NEWLINE_CHAR);
    }

    @Override
    public void close() throws IOException {
        this.os.close();
    }
}
