package org.epics.archiverappliance.utils.nio;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Compressed files typically do not let us seek arbitrarily within the stream.
 * This is a inefficient workaround for this. 
 * This can be improved with ZRan like functionality later if needed.
 * This implements a read only seekable byte channel over a one way input stream.
 * @author mshankar
 *
 */
public class WrappedSeekableByteChannel implements SeekableByteChannel {
	private Path srcPath = null;
	private InputStream backingIs = null;
	long position = 0;
	
	public WrappedSeekableByteChannel(Path path) throws IOException {
		this.srcPath = path;
		backingIs = Files.newInputStream(srcPath, StandardOpenOption.READ);
	}

	@Override
	public void close() throws IOException {
		if( backingIs != null) backingIs.close();
		backingIs = null;
	}

	@Override
	public boolean isOpen() {
		return backingIs != null;
	}

	@Override
	public long position() throws IOException {
		return position;
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		if(newPosition == position) {
			// No need to do anything...
		} else if(newPosition > position) {
			backingIs.skip(newPosition - position);
			this.position = newPosition;
		} else { 
			// Here's the inefficient part, we close the stream and open another
			backingIs.close();
			backingIs = null;
			backingIs = Files.newInputStream(srcPath, StandardOpenOption.READ);
			backingIs.skip(newPosition);
			this.position = newPosition;
		}
		return this;
	}

	@Override
	public int read(ByteBuffer byteBuf) throws IOException {
		int bufPotential = byteBuf.remaining();
		byte[] buf = new byte[bufPotential];
		int bytesRead = backingIs.read(buf);
		if(bytesRead > 0) {
			byteBuf.put(buf, 0, bytesRead);
			position = position + bytesRead;
			return bytesRead;
		}
		return -1;
	}

	@Override
	public long size() throws IOException {
		return Files.size(srcPath);
	}

	@Override
	public SeekableByteChannel truncate(long arg0) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int write(ByteBuffer arg0) throws IOException {
		throw new UnsupportedOperationException();
	}

}
