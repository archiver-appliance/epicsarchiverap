/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import java.io.IOException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.epics.archiverappliance.config.ConfigService;

/**
 * A very simple struts like action for business processes. 
 * Responses are typically JSON though this is not enforced.
 * We are not too far away from the servlet container here.
 * A handle to the configservice is passed in as part of the execute method.
 * The BPLAction is extected to handle all servlet container traffic like HTTP error codes etc. 
 * If an exception is thrown, the servlet that calls BPLActions will send a Internal Server Error to the caller.
 * @author mshankar
 *
 */
public interface BPLAction {
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException;
}
