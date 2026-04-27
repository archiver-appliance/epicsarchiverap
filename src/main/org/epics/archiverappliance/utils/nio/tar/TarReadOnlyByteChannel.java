package org.epics.archiverappliance.utils.nio.tar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;

/**
 * Wraps a SeekableByteChannel to present a read only view of the tar entry.
 * tar.dataoffset() is the offset of the start of the data in the tar file, and tar.size() is the size of the data.
 * The position of the channel is relative to the start of the tar file, so we need to adjust the position and limit of the channel to present a view of the tar entry data.
 **/
public class TarReadOnlyByteChannel implements SeekableByteChannel {
    static final Logger logger = LogManager.getLogger(TarReadOnlyByteChannel.class.getName());
    final TarEntry tarEntry;
    final File tarFile;
    final RandomAccessFile rf;
    final FileChannel proxied;

    public TarReadOnlyByteChannel(String tarFileName, TarEntry tarEntry) throws IOException {
        this.tarFile = new File(tarFileName);
        this.tarEntry = tarEntry;
        this.rf = new RandomAccessFile(tarFile, "r");
        this.proxied = rf.getChannel();
        this.proxied.position(tarEntry.dataoffset());
    }

    @Override
    public boolean isOpen() {
        return proxied.isOpen();
    }

    @Override
    public void close() throws IOException {
        proxied.close();
        rf.close();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        long remaining = tarEntry.dataoffset() + tarEntry.size() - proxied.position();
        if (remaining <= 0) {
            return -1;
        }
        if ((dst.position() + dst.remaining()) > remaining) {
            dst.limit((int) (dst.position() + remaining));
        }
        return proxied.read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException("Write operation is not supported on TarReadOnlyByteChannel");
    }

    @Override
    public long position() throws IOException {
        return proxied.position() - tarEntry.dataoffset();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if (newPosition < 0) {
            throw new IOException("Cannot set position before the start of the tar entry data");
        }
        if (newPosition > tarEntry.size()) {
            throw new IOException("Cannot set position beyond the end of the tar entry data");
        }
        proxied.position(tarEntry.dataoffset() + newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        return tarEntry.size();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException("Truncate operation is not supported on TarReadOnlyByteChannel");
    }
}
