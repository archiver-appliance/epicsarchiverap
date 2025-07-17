/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.utils;

import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.utils.nio.ArchPaths;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * A modification of LineByteStream optimized for reading a PB chunk backwards in time
 * Based off of dpetruha's answer here - https://stackoverflow.com/questions/8664705/how-to-read-file-from-end-to-start-in-reverse-order-in-java
 * @author mshankar
 *
 */
public class ReverseLineByteStream implements Closeable {
    public static final int MAX_LINE_SIZE = 16 * 1024;
    private SeekableByteChannel byteChannel = null;
    private Path path = null;
    private long stopAtPosition = 0L;

    /* This channel has been read until this position */
    private long chReadUntil;

    private byte[] buffer;
    private int currentBufferPos;

    private byte[] currentLine;
    private int currentLineWritePos = 0;
    private int currentLineReadPos = 0;
    private boolean lineBuffered = false;

    // Used for testing
    private long totalBytesRead = 0L;

    public ReverseLineByteStream(Path path) throws IOException {
        this.path = path;
        this.byteChannel = ArchPaths.newByteChannel(path, StandardOpenOption.READ);
        buffer = new byte[MAX_LINE_SIZE];
        currentLine = new byte[MAX_LINE_SIZE];
        currentLine[0] = LineEscaper.NEWLINE_CHAR;
        chReadUntil = this.byteChannel.size();

        fillBuffer();
        fillLineBuffer();
    }

    public ReverseLineByteStream(Path path, long startPosition, long endPosition) throws IOException {
        this(path);
        if (endPosition > 0 && endPosition < path.toFile().length()) {
            this.seekToBeforePreviousLine(endPosition);
        }
        this.stopAtPosition = startPosition;
    }

    public void seekToBeforePreviousLine(long posn) throws IOException {
        this.byteChannel.position(posn);
        chReadUntil = this.byteChannel.position();
        fillBuffer();
        fillLineBuffer();
        // Read a line and ignore it...
        ByteArray bar = new ByteArray(ReverseLineByteStream.MAX_LINE_SIZE);
        this.readLine(bar);
    }

    public boolean readLine(ByteArray bar) throws IOException {
        bar.reset();
        if (chReadUntil <= stopAtPosition && currentBufferPos < 0 && currentLineReadPos < 0) {
            return false;
        }

        if (chReadUntil + currentBufferPos < stopAtPosition) {
            // logger.info("Reached the stopAtPosition {}", stopAtPosition);
            return false;
        }

        if (!lineBuffered) {
            fillLineBuffer();
        }

        if (lineBuffered) {
            if (currentLineReadPos <= 0) {
                // logger.info("Nothing read; escape hatch");
                lineBuffered = false;
                return false;
            }

            // logger.info("Picked up a line {}", currentLineReadPos);

            bar.reset();
            while (currentLineReadPos >= 0) {
                bar.data[bar.len++] = currentLine[currentLineReadPos--];
                // logger.info("Char at {} is {}", currentLineReadPos+1, Character.toString(bar.data[bar.len-1]));
            }
            lineBuffered = false;
            return true;
        }
        return false;
    }

    private boolean fillBuffer() throws IOException {
        if (chReadUntil <= stopAtPosition) {
            return false;
        }
        ByteBuffer bbuf = ByteBuffer.wrap(buffer);
        bbuf.clear();

        if (chReadUntil < buffer.length) {
            // logger.debug("Reached Start of file Channel Read Until {} Total bytes read {}", chReadUntil,
            // totalBytesRead);
            this.byteChannel.position(0);
            bbuf.limit((int) chReadUntil);
            int bytesRead = this.byteChannel.read(bbuf);
            assert (bytesRead == chReadUntil);
            bbuf.flip();
            currentBufferPos = bytesRead - 1;
            totalBytesRead += bytesRead;
            chReadUntil = chReadUntil - bytesRead;
            // logger.debug("Reached Start BytesRead {} Channel Read Until {} Total bytes read {}", bytesRead,
            // chReadUntil, totalBytesRead);
        } else {
            long currentFilePos = chReadUntil - buffer.length;
            this.byteChannel.position(currentFilePos);
            int bytesRead = this.byteChannel.read(bbuf);
            assert (bytesRead == buffer.length);
            bbuf.flip();
            currentBufferPos = bytesRead - 1;
            totalBytesRead += bytesRead;
            chReadUntil = chReadUntil - bytesRead;
            // logger.debug("BytesRead {} Channel Read Until {} Total bytes read {}", bytesRead, chReadUntil,
            // totalBytesRead);
        }
        return true;
    }

    private boolean fillLineBuffer() throws IOException {
        currentLineWritePos = 0;

        while (true) {

            // we've read all the buffer - need to fill it again
            if (currentBufferPos < 0) {
                // logger.debug("Empty buffer; filling it again");
                fillBuffer();

                if (currentBufferPos < 0) {
                    // logger.debug("Nothing buffered as we have reached the beginning of the file");
                    currentLineReadPos = currentLineWritePos - 1;
                    lineBuffered = true;
                    return false;
                }
            }

            byte b = buffer[currentBufferPos--];

            // newline is found - line fully buffered
            if (b == LineEscaper.NEWLINE_CHAR) {
                if ((currentLineWritePos - 1) <= 0) {
                    currentLineWritePos = 0;
                    // logger.info("Encountered a new line and a blank line Currentbufferpos {} chReadUntil {}
                    // linelength {}", currentBufferPos, chReadUntil, currentLineWritePos);
                    continue;
                }
                // logger.info("Encountered a new line Currentbufferpos {} chReadUntil {} linelength {}",
                // currentBufferPos, chReadUntil, currentLineWritePos);
                currentLineReadPos = currentLineWritePos - 1;
                lineBuffered = true;
                return lineBuffered;
            } else {
                if (currentLineWritePos == MAX_LINE_SIZE) {
                    throw new IOException("file has a line exceeding " + MAX_LINE_SIZE + " bytes");
                }

                // write the current line bytes in reverse order - reading from
                // the end will produce the correct line
                currentLine[currentLineWritePos++] = b;
                // logger.info("Writing char at {} is {}", currentLineWritePos-1, Character.toString(b));
            }
        }
    }

    public void safeClose() {
        try {
            this.close();
        } catch (Throwable t) {
            // Safe close...
        }
    }

    public String getAbsolutePath() {
        return this.path.toAbsolutePath().toString();
    }

    @Override
    public void close() throws IOException {
        if (this.byteChannel != null) this.byteChannel.close();
        this.byteChannel = null;
        chReadUntil = 0;
        buffer = null;
        currentBufferPos = 0;
        currentLine = null;
        currentLineWritePos = 0;
        currentLineReadPos = 0;
        lineBuffered = false;
    }

    /**
     * Mainly used in the unit tests
     */
    public long getTotalBytesRead() {
        return totalBytesRead;
    }

    /**
     * Should only be used from the unit tests
     */
    boolean testFillBuffer() throws IOException {
        return this.fillBuffer();
    }

    /**
     * Should only be used from the unit tests
     */
    boolean testFillLineBuffer() throws IOException {
        return this.fillLineBuffer();
    }
}
