/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.data;

import java.io.StringWriter;
import java.util.List;

import org.json.simple.JSONValue;

/**
 * An implementation of SampleValue for vector strings.
 * @author mshankar
 *
 */
public class VectorStringSampleValue implements SampleValue {
	private List<String> values;

	public VectorStringSampleValue(List<String> vals) {
		this.values = vals;
	}
	
	/* (non-Javadoc)
	 * The toString for vectors generates a a JSON vector...
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		boolean first = true;
		StringWriter buf = new StringWriter();
		buf.append('[');
		for(String value : values) {
			if(first) { first = false; } else { buf.append(","); }
			buf.append(value);
		}
		buf.append(']');
		return buf.toString();		
	}

	@Override
	public Number getValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getElementCount() {
		return values.size();
	}

	@Override
	public Number getValue(int index) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String getStringValue(int index) {
		return values.get(index);
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
		VectorStringSampleValue other = (VectorStringSampleValue) obj; 
		return values.equals(other.getValues());
	}

	@Override
	public String toJSONString() {
		boolean first = true;
		StringWriter buf = new StringWriter();
		buf.append('[');
		for(String value : values) {
			if(!value.isEmpty()) { 
				if(first) { first = false; } else { buf.append(","); }
				buf.append("\"");
				buf.append(JSONValue.escape(value));
				buf.append("\"");
			}
		}
		buf.append(']');
		return buf.toString();
	}
}
