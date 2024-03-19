/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

/**
 * External integrations may not have the same contract as PB the plugin re year partitions.
 * This exception is used to communicate year transition info OOB info to folks interested in it.
 * This matters only in the context of the server and only for the RAW retrieval mime type.
 * Others may happily ignore this exception.
 * Unfortunately, the iterator interface does not allow for custom exceptions so we have to make this a RuntimeException.
 * Therefore, others <b>must</b> happily ignore this exception.
 * @author mshankar
 *
 */
public class ChangeInYearsException extends RuntimeException {
	private static final long serialVersionUID = 379741610493225449L;
	private short previousYear;
	private short currentYear;
	
	public ChangeInYearsException(short previousYear, short currentYear) { 
		this.previousYear = previousYear;
		this.currentYear = currentYear;
	}

	/**
	 * @return the previousYear
	 */
	public short getPreviousYear() {
		return previousYear;
	}

	/**
	 * @return the currentYear
	 */
	public short getCurrentYear() {
		return currentYear;
	}
}
