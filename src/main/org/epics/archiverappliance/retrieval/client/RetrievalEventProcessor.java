/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import org.epics.archiverappliance.EventStreamDesc;

/**
 * The DataRetrieval interface is a streaming interface with out-of-band events on the stream being passed as events on this interface.
 * @author mshankar
 *
 */
public interface RetrievalEventProcessor {
	/**
	 * Called when we are transitioning into another PV's data in the stream.
	 * This is where we can perform actions like switching buffers, reseting the vertical transforms, moveTo's to the origin etc.  
	 * A newPVOnStream is also send on the first PV in the stream as well.
	 * @param desc EventStreamDesc
	 */
	public void newPVOnStream(EventStreamDesc desc);
}
