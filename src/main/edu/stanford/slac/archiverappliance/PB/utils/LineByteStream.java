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
	public static int MAX_ITERATIONS_TO_DETERMINE_LINE = 16 * 1024;
	private SeekableByteChannel byteChannel = null;
	private Path path = null;
	byte[] buf = null;
	int bytesRead = 0;
	int currentReadPosition = 0;
	long lastReadPointer = 0;
	long totalBytesToRead = Long.MAX_VALUE;
	long totalBytesReadSoFar = 0L;
	ByteBuffer byteBuf = null;
	
	public LineByteStream(Path path) throws IOException {
		this.path = path;
		this.byteChannel = ArchPaths.newByteChannel(path, StandardOpenOption.READ);
		buf = new byte[MAX_LINE_SIZE];
		lastReadPointer = byteChannel.position();
		byteBuf = ByteBuffer.allocate(MAX_LINE_SIZE); 
		readNextBatch();
	}

	public LineByteStream(Path path, long startPosition) throws IOException {
		this.path = path;
		this.byteChannel = ArchPaths.newByteChannel(path, StandardOpenOption.READ);
		this.byteChannel.position(startPosition);
		buf = new byte[MAX_LINE_SIZE];
		lastReadPointer = byteChannel.position();
		byteBuf = ByteBuffer.allocate(MAX_LINE_SIZE); 
		readNextBatch();
	}

	public LineByteStream(Path path, long startPosition, long endPosition) throws IOException {
		this.path = path;
		this.byteChannel = ArchPaths.newByteChannel(path, StandardOpenOption.READ);
		this.byteChannel.position(startPosition);
		totalBytesToRead = endPosition - startPosition + 1;
		buf = new byte[MAX_LINE_SIZE];
		lastReadPointer = byteChannel.position();
		byteBuf = ByteBuffer.allocate(MAX_LINE_SIZE); 
		readNextBatch();
	}

	private void readNextBatch() throws IOException {
		if(totalBytesReadSoFar >= totalBytesToRead) {
			bytesRead = 0;
			return;
		}
		
		lastReadPointer = lastReadPointer+bytesRead;
		byteBuf.clear();
		bytesRead = this.byteChannel.read(byteBuf);
		byteBuf.flip();
		if(bytesRead > 0) { 
			byteBuf.get(buf, 0, bytesRead);
		}
		currentReadPosition = 0;
		
		long lastTotalBytes = totalBytesReadSoFar; 
		totalBytesReadSoFar += bytesRead;
		if(totalBytesReadSoFar >= totalBytesToRead) {
			// The downcasting to int should be safe as the most we'll read over the limit is MAX_LINE_SIZE
			int resetBytesRead = (int) (totalBytesToRead - lastTotalBytes);
			// We find the first new line and stop there.
			while(resetBytesRead < bytesRead && buf[resetBytesRead] != LineEscaper.NEWLINE_CHAR) resetBytesRead++;
			if(resetBytesRead <= bytesRead) { 
				bytesRead = resetBytesRead;
			} else {
				if(logger.isDebugEnabled()) { 
					logger.debug("Cannot find newline at tail end of file. resetBytesRead = " + resetBytesRead + " bytesRead=" + bytesRead + " totalBytesReadSoFar=" + totalBytesReadSoFar + "totalBytesToRead=" + totalBytesToRead);
				}
			}
		}
		// We leave totalBytesReadSoFar so far at the higher value so the next readNextBatch will terminate at the first if statement.
	}

	
	public byte[] readLine() throws IOException {
		if(bytesRead <= 0) return null;

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int loopcount = 0;
		while(loopcount < MAX_ITERATIONS_TO_DETERMINE_LINE) {
			int start = currentReadPosition;
			int posnofnewlinechar = -1;
			while(currentReadPosition < bytesRead) {
				if(buf[currentReadPosition++] == LineEscaper.NEWLINE_CHAR) { 
					posnofnewlinechar = currentReadPosition-1;
					break;
				}
			}
			
			if(posnofnewlinechar == -1) {
				int linelength = (bytesRead - start);
				out.write(buf, start, linelength);
				readNextBatch();
				start = currentReadPosition;
				if(bytesRead <= 0) {
					// End of file reached and we have not found a newline.
					// we cannot return what we have as we'll get PBParseExceptions upstream.
					return null;
				}				
			} else {
				int linelength = (currentReadPosition - start) - 1;
				out.write(buf, start, linelength);
				return out.toByteArray();
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
	 * @param bar ByteArray
	 * @return bar ByteArray
	 * @throws IOException  &emsp;
	 */
	public ByteArray readLine(ByteArray bar) throws IOException {
		bar.reset();
		if(bytesRead <= 0 || currentReadPosition >= bytesRead) { 
			return bar;
		}

		int loopcount = 0;
		while(loopcount++ < MAX_ITERATIONS_TO_DETERMINE_LINE) {
			try {
				while(currentReadPosition < bytesRead) {
					assert(currentReadPosition < buf.length);
					byte b = buf[currentReadPosition];
					if(b == LineEscaper.NEWLINE_CHAR) {  
						if(currentReadPosition >= bytesRead - 1) {
							readNextBatch();
						} else { 
							currentReadPosition++;
						}
						return bar;
					} else {
						bar.data[bar.len++] = b;
					}
					if(currentReadPosition >= bytesRead - 1) {
						readNextBatch();
						if(bytesRead <= 0 || currentReadPosition >= bytesRead) {
							// We have not found a new line; we cannot return what we have as we'll get PBParseExceptions upstream.
							bar.reset();
							return bar;
						}
					} else { 
						currentReadPosition++;
					}
				}
			} catch(ArrayIndexOutOfBoundsException ex) {
				// We would have incremeted the pointer; so decrement it back..
				bar.len--; 
				logger.debug("ByteBuffer is too small, doubling it to accomodate longer lines.");
				bar.doubleBufferSize();
			}
		}
		
		throw new LineTooLongException("Unable to determine end of line within iteration count " + MAX_ITERATIONS_TO_DETERMINE_LINE);
	}
	
	/**
	 * Seeks to the first new line after the current position in the rndAccFile.
	 * The file pointer is located just after the first newline.
	 * @throws IOException &emsp;
	 */
	public void seekToFirstNewLine() throws IOException {
		if(lastReadPointer < 1L) {
			// If we are at the start of the file then we return right away.
			return;
		}
		readLine();
	}
	
	/**
	 * Seeks and positions the pointer to to the last line in the file.
	 * The file pointer is located just before the last line so that readLine gets a valid line.
	 * About the only thing once can do after this is to read a line and stop...	 
	 * @throws IOException  &emsp;
	 */
	public void seekToBeforeLastLine() throws IOException {
		buf = new byte[MAX_LINE_SIZE];
		long seekPos = this.byteChannel.size() - MAX_LINE_SIZE;
		int loopcount = 0;
		while(loopcount < MAX_ITERATIONS_TO_DETERMINE_LINE) {
			if(seekPos < 0) seekPos = 0L;
			this.byteChannel.position(seekPos);
			lastReadPointer = seekPos;
			readNextBatch();
			// We are shaving off 2 bytes from the end to skip the last newline if indeed the last line is terminated by a newline.
			for(int i = bytesRead-2; i >= 0; i--) {
				if(buf[i] == LineEscaper.NEWLINE_CHAR) {
					currentReadPosition = i+1;
					return;
				}
			}
			if(seekPos == 0) { 
				logger.debug("Is it possible that the file has only line? We have come to the beginning of the file and this should be definitely before the last line.");
				return;
			}
			seekPos = seekPos - MAX_LINE_SIZE;
			loopcount++;
		}
		throw new LineTooLongException("Unable to determine end of line within iteration count " + MAX_ITERATIONS_TO_DETERMINE_LINE);
	}
	
	/**
	 * Seeks and positions the pointer to line previous to the specified position.
	 * The file pointer is located just so that one can do a readline.
	 * Note that this method is not efficient at all; so use with care.
	 * @param posn  &emsp;
	 * @throws IOException &emsp;
	 */
	public void seekToBeforePreviousLine(long posn) throws IOException {
		// This is a variation of seekToBeforeLastLine
		buf = new byte[MAX_LINE_SIZE];
		long seekPos = posn - MAX_LINE_SIZE;
		int loopcount = 0;
		while(loopcount < MAX_ITERATIONS_TO_DETERMINE_LINE) {
			if(seekPos < 0) seekPos = 0L;
			this.byteChannel.position(seekPos);
			readNextBatch();
			lastReadPointer = seekPos;
			
			// If we are reading the first block, we read more than what we need; so adjust what we read to where we need to be.
			if(posn < bytesRead) { 
				bytesRead = (int) posn;
			}
			
			// We are shaving off 2 bytes from the end to skip the last newline if indeed the last line is terminated by a newline.
			for(int i = bytesRead-2; i >= 0; i--) {
				if(buf[i] == LineEscaper.NEWLINE_CHAR) {
					currentReadPosition = i+1;
					return;
				}
			}
			if(seekPos == 0) { 
				logger.debug("Is it possible that the file has only line? We have come to the beginning of the file and this should be definitely before the last line.");
				return;
			}
			seekPos = seekPos - MAX_LINE_SIZE;
			loopcount++;
		}
		throw new LineTooLongException("Unable to determine end of line within iteration count " + MAX_ITERATIONS_TO_DETERMINE_LINE);
	}


	public long getCurrentPosition() throws IOException {
		return lastReadPointer + currentReadPosition;
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
