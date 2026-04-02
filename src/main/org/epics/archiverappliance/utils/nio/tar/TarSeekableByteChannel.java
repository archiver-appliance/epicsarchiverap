package org.epics.archiverappliance.utils.nio.tar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/*
 * Wraps a SeekableByteChannel and takes some actions when the channel is closed.
 * For reads, this is mostly cleanup
 * For writes, this updates the tar file and then does cleanup.
 */
public class TarSeekableByteChannel implements SeekableByteChannel {
    static final Logger logger = LogManager.getLogger(TarSeekableByteChannel.class.getName());

    Closeable streamToClose; // The stream to close when the channel closes, typically the FileInputStream or
    // FileOutputStream from which proxied below is derived
    FileChannel proxied; // The channel to proxy; all SeekableByteChannel are delegated to this.
    TarEntry tarEntry; // The tarEntry to replace; only the entryName matters here. Can be null in which case we only
    // close the stream
    TarFileSystem gfs; // The Tar file to manipulate
    File fileToAppend; // The file whose contents will become the new data for the tarentry
    Path deletFileOnClose; // Delete this file on close ( typically this is a temp file )
    boolean somethingWritten;

    public TarSeekableByteChannel(
            Closeable streamToClose,
            FileChannel proxied,
            TarEntry tarEntry,
            TarFileSystem gfs,
            File fileToAppend,
            Path deletFileOnClose) {
        this.streamToClose = streamToClose;
        this.proxied = proxied;
        this.tarEntry = tarEntry;
        this.gfs = gfs;
        this.fileToAppend = fileToAppend;
        this.deletFileOnClose = deletFileOnClose;
        this.somethingWritten = false;
    }

    @Override
    public boolean isOpen() {
        return proxied.isOpen();
    }

    @Override
    public void close() throws IOException {
        proxied.close();
        streamToClose.close();
        try {
            if (this.somethingWritten && this.tarEntry != null) {
                tarEntry = gfs.lookupTarEntry(tarEntry.entryName(), tarEntry);
                boolean lastFilenInTar = false;
                if (Files.exists(this.gfs.getTarPath())
                        && tarEntry.dataoffset() + tarEntry.size() >= Files.size(this.gfs.getTarPath())) {
                    logger.debug("{} is the last entry in the tar file", tarEntry.entryName());
                    lastFilenInTar = true;
                }
                if (this.tarEntry.isInsideTar()) {
                    if (!lastFilenInTar) {
                        this.gfs.tarFile.markFileAsDeleted(this.tarEntry);
                        logger.debug("Done deleting any existing tar entries for {}", this.tarEntry.entryName());
                    }
                }
                if (!Files.exists(this.gfs.getTarPath())
                        && !Files.exists(this.gfs.getTarPath().getParent())) {
                    logger.debug(
                            "Creating parent directory {} for tar file {}",
                            this.gfs.getTarPath().getParent(),
                            this.gfs.getTarPath());
                    Files.createDirectories(this.gfs.getTarPath().getParent());
                }

                TarEntry appendTarEntry = new TarEntry(tarEntry.entryName(), this.fileToAppend);
                if (lastFilenInTar) {
                    this.gfs.tarFile.appendFiles(Arrays.asList(appendTarEntry), tarEntry);
                } else {
                    this.gfs.tarFile.appendFiles(Arrays.asList(appendTarEntry));
                }
                this.gfs.reloadCatalog();

                logger.debug(
                        "Done appending new content for {} from temp file {} into tar file {} ",
                        this.tarEntry.entryName(),
                        this.fileToAppend.getAbsolutePath(),
                        this.gfs.tarFile.getTarFileName());
            }
            if (this.deletFileOnClose != null) {
                Files.deleteIfExists(this.deletFileOnClose);
            }
        } catch (Exception ex) {
            logger.error("Exception appending to tar file ", ex);
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return proxied.read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        somethingWritten = true;
        return proxied.write(src);
    }

    @Override
    public long position() throws IOException {
        return proxied.position();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        return proxied.position(newPosition);
    }

    @Override
    public long size() throws IOException {
        return proxied.size();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        return proxied.truncate(size);
    }
}
