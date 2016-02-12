package org.epics.archiverappliance.data;

import java.nio.ByteBuffer;
import java.util.List;

import org.epics.archiverappliance.data.SampleValue;

/**
 * A sample value that encapsulates a byte buffer.
 * This is principally as the "value" of a EPICS V4 PVStructure that is not mapped to any of the standard DBR types.
 * @author mshankar
 *
 */
public class ByteBufSampleValue implements SampleValue {
	ByteBuffer buf;
	public ByteBufSampleValue(ByteBuffer buf) { 
		this.buf = buf;
	}

	@Override
	public ByteBuffer getValueAsBytes() {
		return buf.asReadOnlyBuffer();
	}

	@Override
	public boolean equals(Object obj) {
		ByteBufSampleValue other = (ByteBufSampleValue) obj;
		return this.buf.equals(other.getValueAsBytes());
	}

	@Override
	public int hashCode() {
		return this.buf.hashCode();
	}

	@Override
	public String toJSONString() {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List getValues() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Number getValue(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Number getValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getStringValue(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getElementCount() {
		return 1;
	}
}