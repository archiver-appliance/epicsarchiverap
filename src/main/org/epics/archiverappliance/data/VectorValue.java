/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.data;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * An implementation of SampleValue for vector numbers.
 * @author mshankar
 *
 * @param <T>
 */
public class VectorValue <T extends Number> implements SampleValue {
	private List<T> values;
	
	public VectorValue(List<T> vals) {
		this.values = vals;
	}

	/* (non-Javadoc)
	 * @see org.epics.archiverappliance.data.SampleValue#getValue()
	 * For the scalar getValue, we always return the first value
	 */
	@Override
	public Number getValue() {
		return values.get(0);
	}

	@Override
	public int getElementCount() {
		return values.size();
	}

	@Override
	public Number getValue(int index) {
		return values.get(index);
	}

	/* (non-Javadoc)
	 * The toString for vectors generates a JSON vector...
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		boolean first = true;
		StringWriter buf = new StringWriter();
		buf.append('[');
		for(T value : values) {
			if(first) { first = false; } else { buf.append(","); }
			buf.append(value.toString());
		}
		buf.append(']');
		return buf.toString();
	}

	
	@Override
	public String getStringValue(int index) {
		return getValue(index).toString();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List getValues() {
		return values;
	}

	@Override
	public int hashCode() {
		return values.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		@SuppressWarnings("unchecked")
		VectorValue<T> other = (VectorValue<T>) obj; 
		return values.equals(other.getValues());
	}

	@Override
	public String toJSONString() {
		boolean first = true;
		StringWriter buf = new StringWriter();
		buf.append('[');
		for(T value : values) {
			if(first) { first = false; } else { buf.append(","); }
			buf.append(value.toString());
		}
		buf.append(']');
		return buf.toString();
	}

	@Override
	public ByteBuffer getValueAsBytes() {
		throw new UnsupportedOperationException();
	}
}
