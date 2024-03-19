/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import java.io.IOException;

import org.epics.archiverappliance.EventStream;

/**
 * We can potentially get a large number of ETLInfo objects when performing ETL for the first time. 
 * If we had an EventStream in each of there, we could potentially run into issues with "too many open files"
 * So, each ETLInfo instead has something that can generate the stream when needed.
 *  
 * @author mshankar
 *
 */
public interface ETLStreamCreator {
	public EventStream getStream() throws IOException ;
}
