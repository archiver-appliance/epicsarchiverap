/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.data;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Get the value of this event.
 * Within the archiver appliance, we only ask some form of converting to a string and perhaps a Number.
 * The toString for vectors generates a JSON form of the vector...
 * @author mshankar
 */
public interface SampleValue {
	public String toString();
	public int getElementCount();
	public Number getValue();
	public Number getValue(int index);
	public String getStringValue(int index);
	public String toJSONString();
	@SuppressWarnings("rawtypes")
	public List getValues();
	/**
	 * Return the value as a ByteBuffer that is ready to read.
	 * @return ByteBuffer  &emsp;
	 */
	public ByteBuffer getValueAsBytes();
}
