/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.ui;


import edu.stanford.slac.archiverappliance.plain.PlainPBStoragePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;

/**
 * This is currently the code that remotes an event stream, for example from the engine etc.
 * Only raw PB responses are supported. The DataRetrievalServlet is expected to merge/dedup this stream into a final stream
 * @author mshankar
 *
 */
@SuppressWarnings("serial")
public class RetrievalServlet  extends HttpServlet {
	private static Logger logger = LogManager.getLogger(RetrievalServlet.class.getName());
	String pbRootFolder = null;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		PlainPBStoragePlugin storagePlugin = new PlainPBStoragePlugin();
		storagePlugin.setRootFolder(pbRootFolder);
		logger.info("Initialized the root folder to " + pbRootFolder);

		String PV = req.getParameter("pv");
		String startTimeStr = req.getParameter("from"); 
		String endTimeStr = req.getParameter("to");
		
		if(PV == null || startTimeStr == null || endTimeStr == null) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		Instant start = TimeUtils.convertFromISO8601String(startTimeStr);
		Instant end = TimeUtils.convertFromISO8601String(endTimeStr);
		// resp.addHeader("Transfer-Encoding", "chunked");
		
		try(OutputStream os = resp.getOutputStream(); 
				BasicContext context = new BasicContext(); 
				EventStream st = new CurrentThreadWorkerEventStream(PV, storagePlugin.getDataForPV(context, PV, start, end, new DefaultRawPostProcessor()))) {
			long s = System.currentTimeMillis();
			int totalEvents = StreamPBIntoOutput.streamPBIntoOutputStream(st, os, start, end);
			long e = System.currentTimeMillis();
			logger.info("Found a total of " + totalEvents + " in " + (e-s) + "(ms)");
		}
	}

	@Override
	public void init() throws ServletException {
	}
	
	/**
	 * Should only be used by the unit tests for setup...
	 * @param rootFolder  &emsp; 
	 * @return this  &emsp; 
	 */  
	public RetrievalServlet setpbRootFolder(String rootFolder) {
		pbRootFolder = rootFolder;
		return this;
	}
}
