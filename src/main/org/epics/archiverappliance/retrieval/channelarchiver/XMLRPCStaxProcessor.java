/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.channelarchiver;

import java.io.IOException;

/**
 * Our layer on top of STAX; similar to DefaultHandler with the exception of the boolean to indicate pause/resume processing.
 * @author mshankar
 *
 */
public interface XMLRPCStaxProcessor {
	
	public boolean startElement(String localName) throws IOException;

	public boolean endElement(String localName, String value) throws IOException;

}
