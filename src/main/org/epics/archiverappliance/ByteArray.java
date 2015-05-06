package org.epics.archiverappliance;

import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import edu.stanford.slac.archiverappliance.PB.utils.LineTooLongException;

/**
 * A version of byte[] that is used to provide some optimization for data retrieval and the like.
 * This is very similar to NIO's ByteBuffer but has all its internal's exposed and is meant to pass a byte[]+offset+length around within the appliance archiver.
 * Many event streams reuse the same ByteArray across events in order to minimize memory allocation costs.
 * And as we reuse ByteArray instances so we have a reset method. 
 * The convention for ByteArrays within Events is that they stored the escaped bytes (@see LineEscaper) and the inPlaceUnescape method is used to unescape these bytes.
 * For example, in summary, the PB code will 
 * <code>
 * ByteArray bar = new ByteArray();
 * LineByteStream lbs = new LineByteStream(path...);
 * lbs.readLine(bar);
 * while(!bar.isEmpty()) {
 *   SomeType.newBuilder().mergeFrom(bar.inPlaceUnescape().unescapedData, bar.off, bar.unescapedLen).build();
 *   lbs.readLine(bar);
 * }
 * </code>
 * The readLine logic in lbs has the code necessary to increase the length of the data field as required.
 * 
 * Because many event streams reuse the same ByteArray across events, life becomes interesting as we need to remember to use makeClone anytime we want to span the Event across the iterator that generated it.
 * Spoke with Luofeng about this and we both came to the conclusion that we should try to make this work as much as possible as the GC gains itself are quite a bit.
 * @author mshankar
 *
 */
public class ByteArray {
	public byte[] data = null;
	public int off = 0;
	public int len = 0;
	public byte[] unescapedData = null;
	public int unescapedLen = 0;
	
	public ByteArray(int size) {
		data = new byte[size];
		off = 0;
		len = 0;
		unescapedLen = 0;
	}
	
	public ByteArray(byte[] src) {
		data = src;
		off = 0;
		len = src.length;
		unescapedLen = 0;
	}
	
	public void reset() { 
		off = 0;
		len = 0;
		unescapedLen = 0;
	}
	
	/**
	 * Use this for unit tests and the like...
	 * @return
	 */
	public byte[] toBytes() {
		if(len == 0) return null;
		byte[] ret = new byte[len];
		System.arraycopy(data, off, ret, 0, len);
		return ret;
	}
	
	public byte[] unescapedBytes() {
		if(unescapedLen == 0) return null;
		byte[] ret = new byte[unescapedLen];
		System.arraycopy(unescapedData, off, ret, 0, unescapedLen);
		return ret;
	}
	
	private static final int MAX_BUFFER_SIZE = (LineByteStream.MAX_LINE_SIZE * LineByteStream.MAX_ITERATIONS_TO_DETERMINE_LINE) + 1024;
	/**
	 * Increases the size of the array to twice what it is currently subject to a maximum
	 * @return
	 */
	public void doubleBufferSize() throws LineTooLongException {
		int newSize = data.length * 2;
		if(newSize > MAX_BUFFER_SIZE) throw new LineTooLongException("Current size is " + data.length + ". Doubling it will cause us to reach over the limit.");
		byte[] newData = new byte[newSize];
		System.arraycopy(data, off, newData, 0, len);
		data = newData;
	}
	
	public boolean isEmpty() {
		return len == 0;
	}
	
	public ByteArray inPlaceUnescape() {
		if(len == 0) return this;
		if(unescapedData == null || unescapedData.length != data.length) unescapedData = new byte[data.length];
		unescapedLen = 0;
		for(int i = off; i < len; i++) {
			byte b = data[i];
			if(b == LineEscaper.ESCAPE_CHAR) {
				i++;
				b = data[i];
				switch(b) {
				case LineEscaper.ESCAPE_ESCAPE_CHAR: unescapedData[unescapedLen++] = LineEscaper.ESCAPE_CHAR;break;
				case LineEscaper.NEWLINE_ESCAPE_CHAR: unescapedData[unescapedLen++] = LineEscaper.NEWLINE_CHAR;break;
				case LineEscaper.CARRIAGERETURN_ESCAPE_CHAR: unescapedData[unescapedLen++] = LineEscaper.CARRIAGERETURN_CHAR;break;
				default: unescapedData[unescapedLen++] = b;break;
				}
			} else {
				unescapedData[unescapedLen++] = b;
			}
		}
		return this;
	}
}
