/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.data;

import java.util.List;

/**
 * An implementation of SampleValue for scalar numbers.
 * @author mshankar
 * @param <T>
 */
public class ScalarValue<T extends Number> implements SampleValue {
	private T value;
	public ScalarValue(T val) {
		this.value = val;
	}

	@Override
	public Number getValue() {
		return value;
	}
	
	@Override
	public String toString() {
		return value.toString();
	}

	@Override
	public int getElementCount() {
		return 1;
	}

	/* (non-Javadoc)
	 * @see org.epics.archiverappliance.data.SampleValue#getValue(int)
	 * We return the same value for all indices
	 * Perhaps we can throw an exception here for invalid indices
	 */
	@Override
	public Number getValue(int index) {
		assert(index >= 0 && index < 1);
		return value;
	}
	
	@Override
	public String getStringValue(int index) {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List getValues() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		@SuppressWarnings("unchecked")
		ScalarValue<T> other = (ScalarValue<T>) obj; 
		return value.equals(other.getValue());
	}
}
