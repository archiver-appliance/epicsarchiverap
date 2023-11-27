/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/


package org.epics.archiverappliance.etl.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.HashMap;
/**
 * consolidate PB files for one pv before one storage
 * @author Luofeng  Li 
 *
 */
public class ConsolidatePBFilesForOnePV implements BPLAction {
	private static final Logger logger = LogManager.getLogger(ConsolidatePBFilesForOnePV.class);
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		String storageName=req.getParameter("storage");
		if(pvName == null || pvName.equals("") || storageName==null || storageName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		Instant oneYearLaterTimeStamp = TimeUtils.convertFromEpochSeconds(TimeUtils.getCurrentEpochSeconds() + 365 * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(), 0);
		try {
			ETLExecutor.runPvETLsBeforeOneStorage(configService, oneYearLaterTimeStamp, pvName, storageName);
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			HashMap<String, Object> infoValues = new HashMap<String, Object>();
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("status", "ok");
				infoValues.put("desc", "Successfully consolidated PV " + pvName + " into " + storageName);
				out.println(JSONValue.toJSONString(infoValues));
			}
		} catch (IOException e) {
			logger.error("Exception consolidating the partitions for pv: " + pvName + " into store: " + storageName, e);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
	}
}
