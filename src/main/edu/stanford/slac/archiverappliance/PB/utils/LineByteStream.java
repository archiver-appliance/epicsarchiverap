/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.utils;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.utils.nio.ArchPaths;

/**
 * This class wraps a RandomAccessFile and returns byte arrays separated by lines.
 * In addition it also maintains a count of the bytes read.
 * We expect the file channel to be positioned correctly for the initial read.
 * After each read, the channel is positioned just after the newline.
 * @author mshankar
 *
 */
public class LineByteStream implements Closeable {
	private static Logger logger = Logger.getLogger(LineByteStream.class.getName());
	public static int MAX_LINE_SIZE = 16 * 1024;
	public static int MAX_ITERATIONS_TO_DETERMINE_LINE = 1024;
	
	private SeekableByteChannel byteChannel = null;
	private Path path = null;
	byte[] buf = null;
	int bytesRead = 0;
	int currentReadPosition = 0;
	long lastReadPointer = 0;
	long startPosition = 0;
	long endPosition = 0;
	ByteBuffer byteBuf = null;

	public LineByteStream(Path path) throws IOException {
		this(path, 0, Long.MAX_VALUE);
	}
	
	public LineByteStream(Path path, long startPosition) throws IOException {
		this(path, startPosition, Long.MAX_VALUE);
	}
	
	public LineByteStream(Path path, long startPosition, long endPosition) throws IOException {
		this.path = path;
		this.startPosition = startPosition;
		this.endPosition = endPosition;
		this.byteChannel = ArchPaths.newByteChannel(path, StandardOpenOption.READ);
		buf = new byte[MAX_LINE_SIZE];
		byteBuf = ByteBuffer.allocate(MAX_LINE_SIZE);
		
		seekTo(startPosition);
	}

	private void seekTo(long position) throws IOException {
		byteChannel.position(position);
		
		lastReadPointer = position;
		bytesRead = 0;
		
		readNextBatch();
	}
	
	private void readNextBatch() throws IOException {
		// Assume the current bunch has been read.
		// Note, this works OK even for repeated calls at the end.
		lastReadPointer += bytesRead;
		bytesRead = 0;
		currentReadPosition = 0;
		
		// If we're past the endPosition, don't read any further.
		if (lastReadPointer >= endPosition) {
			return;
		}
		
		// Read some data.
		byteBuf.clear();
		bytesRead = this.byteChannel.read(byteBuf);
		byteBuf.flip();
		if (bytesRead > 0) { 
			byteBuf.get(buf, 0, bytesRead);
		} else {
			bytesRead = 0; // -1 is EOF, don't want negative values
		}
		
		// Fixup bytesRead so that we ignore any data beyond endPosition.
		bytesRead = (int)Math.min((long)bytesRead, endPosition - lastReadPointer);
	}

	
	public byte[] readLine() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int loopcount = 0;
		while(loopcount++ < MAX_ITERATIONS_TO_DETERMINE_LINE) {
			// Look for a newline in the current buffer.
			int start = currentReadPosition;
			while(currentReadPosition < bytesRead) {
				if(buf[currentReadPosition++] == LineEscaper.NEWLINE_CHAR) { 
					// Found the newline. Add any data we still have before the newline,
					// then return this line.
					int linelength = (currentReadPosition - start) - 1;
					out.write(buf, start, linelength);
					return out.toByteArray();
				}
			}
			
			// No newline found here. Append the partial data from this buffer.
			int linelength = bytesRead - start;
			out.write(buf, start, linelength);
			
			// Read another batch of data.
			readNextBatch();
			if(bytesRead <= 0) {
				// End of file reached and we have not found a newline.
				// we cannot return what we have as we'll get PBParseExceptions upstream.
				return null;
			}
		}
		
		throw new LineTooLongException("Unable to determine end of line within iteration count " + MAX_ITERATIONS_TO_DETERMINE_LINE);
	}
	
	
	/**
	 * Optimize the readline by offering the abilty to reuse the same memory allocation.
	 * This does not escape the bytes as it is reading the line.
	 * While this is optimal to do, it means for raw responses, we'll be redoing some of the work and raw responses are 90% of the requests.
	 * If in future, we determine that unescaping here is more optimal, this method has an unescape version in version control history.
	 * Returns the same byte array as the input.
	 * @param bar
	 */
	public ByteArray readLine(ByteArray bar) throws IOException {
		bar.reset();

		int loopcount = 0;
		while(loopcount++ < MAX_ITERATIONS_TO_DETERMINE_LINE) {
			try {
				// Look for a newline in the current buffer while adding characters to the outbut.
				while (currentReadPosition < bytesRead) {
					byte b = buf[currentReadPosition++];
					if (b == LineEscaper.NEWLINE_CHAR) {
						return bar;
					}
					bar.data[bar.len++] = b; // expecting ArrayIndexOutOfBoundsException, see below
				}
				
				// No newline here, read next batch.
				readNextBatch();
				if (bytesRead == 0) {
					// We have not found a new line; we cannot return what we have as we'll get PBParseExceptions upstream.
					bar.reset();
					return bar;
				}
			} catch(ArrayIndexOutOfBoundsException ex) {
				// We have incremeted these indexes, so decrement it them back..
				bar.len--; 
				currentReadPosition--;
				loopcount--;
				logger.debug("ByteBuffer is too small, doubling it to accomodate longer lines.");
				bar.doubleBufferSize();
			}
		}
		
		throw new LineTooLongException("Unable to determine end of line within iteration count " + MAX_ITERATIONS_TO_DETERMINE_LINE);
	}
	
	/**
	 * Seeks to the first new line after the current position in the rndAccFile.
	 * The file pointer is located just after the first newline.
	 */
	public void seekToFirstNewLine() throws IOException {
		if(lastReadPointer < 1L) {
			// If we are at the start of the file then we return right away.
			return;
		}
		readLine();
	}
	
	/**
	 * Look for the right-most newline character before posn.
	 * Seeks and positions the pointer to just after the found newline character,
	 * so that one can do a readLine. Returns true if a newline was found and
	 * false if the beginning of file was reached.
	 * Note that this method is not efficient at all; so use with care.
	 * @param posn
	 * @throws IOException
	 */
	public boolean seekToBeforePreviousLine(long posn) throws IOException {
		long seekPos = posn;
		
		int loopcount = 0;
		while(loopcount++ < MAX_ITERATIONS_TO_DETERMINE_LINE) {
			seekPos = Math.max(0, seekPos - MAX_LINE_SIZE);
			seekTo(seekPos);
			
			int limitedBytesRead = (int)Math.min((long)bytesRead, posn - seekPos);
			
			for (int i = limitedBytesRead - 1; i >= 0; i--) {
				if (buf[i] == LineEscaper.NEWLINE_CHAR) {
					currentReadPosition = i + 1;
					return true;
				}
			}
			
			if(seekPos == 0) { 
				logger.debug("Is it possible that the file has only line? We have come to the beginning of the file and this should be definitely before the last line.");
				return false;
			}
		}
		
		throw new LineTooLongException("Unable to determine end of line within iteration count " + MAX_ITERATIONS_TO_DETERMINE_LINE);
	}

	/**
	 * Shortcut for seekToBeforePreviousLine(getFileSize() - 1).
	 * Effectively this locates the last line, either complete
	 * (terminated with a newline) or incomplete.
	 */
	public boolean seekToBeforeLastLine() throws IOException {
		return seekToBeforePreviousLine(getFileSize() - 1);
	}

	public long getCurrentPosition() throws IOException {
		return lastReadPointer + currentReadPosition;
	}
	
	public long getFileSize() throws IOException {
		return byteChannel.size();
	}
	
	public void safeClose() {
		try {
			this.close();
		} catch(Throwable t) {
			// Safe close...
		}
	}
	
	public String getAbsolutePath() {
		return this.path.toAbsolutePath().toString();
	}

	@Override
	public void close() throws IOException {
		if(this.byteChannel != null) this.byteChannel.close();
		this.byteChannel = null;
		buf = null;
		bytesRead = 0;
		currentReadPosition = 0;
	}
}
