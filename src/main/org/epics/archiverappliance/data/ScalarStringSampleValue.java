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
 * An implementation of SampleValue for scalar strings.
 * @author mshankar
 *
 */
public class ScalarStringSampleValue implements SampleValue {
	private String value;

	public ScalarStringSampleValue(String val) {
		this.value = val;
	}
	
	public String toString() {
		return value;
	}

	@Override
	public Number getValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getElementCount() {
		return 1;
	}

	@Override
	public Number getValue(int index) {
		throw new UnsupportedOperationException();
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
		ScalarStringSampleValue other = (ScalarStringSampleValue) obj; 
		return value.equals(other.toString());
	}

}
