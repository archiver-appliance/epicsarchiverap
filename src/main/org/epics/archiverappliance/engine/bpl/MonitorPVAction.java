/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.pv.EPICSV4.ArchiveEngine_EPICSV4;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

/**
 * BPL for archiving a PV.
 * @author mshankar
 *
 */
public class MonitorPVAction implements BPLAction {
	private static final Logger logger = Logger.getLogger(MonitorPVAction.class);


	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			logger.error("No PV specified when asking engine to archive");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String DBRTypeStr = req.getParameter("dbrtype");
		if(DBRTypeStr == null || DBRTypeStr.equals("")) {
			logger.error("We need to know the DBR type when requesing the engine to archive a pv");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		ArchDBRTypes dbrType = ArchDBRTypes.valueOf(DBRTypeStr);
		
		String firstDestURL = req.getParameter("dest");
		if(firstDestURL == null || firstDestURL.equals("")) {
			logger.error("No first destination specified when asking engine to archive");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		StoragePlugin firstDest = StoragePluginURLParser.parseStoragePlugin(firstDestURL, configService);
		
		String samplingMethodStr = req.getParameter("samplingmethod");
		if(samplingMethodStr == null || samplingMethodStr.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		SamplingMethod samplingMethod = SamplingMethod.valueOf(samplingMethodStr);
		
		String samplingPeriodStr = req.getParameter("samplingperiod");
		float samplingPeriod = PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD;
		if(samplingPeriodStr != null && !samplingPeriodStr.equals("")) {
			samplingPeriod = Float.parseFloat(samplingPeriodStr);
			logger.debug("Using sampling period from the HTTP request. " + samplingPeriod);
		} else {
			logger.debug("Using default sampling period " + samplingPeriod);
		}
		
		String secondsToBufferStr = req.getParameter("secondstobuffer");
		int secondsToBuffer = PVTypeInfo.DEFAULT_BUFFER_INTERVAL;
		if(secondsToBufferStr != null && !secondsToBufferStr.equals("")) {
			secondsToBuffer = Integer.parseInt(secondsToBufferStr);
			logger.debug("Using seconds to buffer from the HTTP request. " + secondsToBuffer);
		} else {
			logger.debug("Using default seconds to buffer " + secondsToBuffer);
		}
		
		String lastKnownTimestampStr = req.getParameter("lastknowntimestamp");
		Timestamp lastKnownTimestamp = null;
		if(lastKnownTimestampStr != null) {
			lastKnownTimestamp = TimeUtils.convertFromISO8601String(lastKnownTimestampStr);
			logger.debug("Last known timestamp from ETL stores is " + TimeUtils.convertToHumanReadableString(lastKnownTimestamp));
		}
		
		String controllingPV = req.getParameter("controllingPV");
		if(controllingPV != null) {
			logger.debug("PV " + pvName + " is being conditionally archived using controlling pv " + controllingPV);
		}

		// We no longer use archiveFields per Ernest
		String[] archiveFields = null;
		
		logger.info("Archiving PV " + pvName + "using " + samplingMethod.toString() + " with a sampling period of "+ samplingPeriod + "(s)");
		try {
			if(!dbrType.isV3Type()) {
				ArchiveEngine_EPICSV4.archivePV(pvName, samplingPeriod, samplingMethod, secondsToBuffer, firstDest, configService, dbrType);
				resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
				try(PrintWriter out = resp.getWriter()) {
					out.println("{ \"pvName\": \"" + pvName + "\", \"status\": \"Archive request has been submitted\" }");
				}
			} else {
				ArchiveEngine.archivePV(pvName, samplingPeriod, samplingMethod, secondsToBuffer, firstDest, configService, dbrType,lastKnownTimestamp, controllingPV, archiveFields, null); 
				resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
				try(PrintWriter out = resp.getWriter()) {
					out.println("{ \"pvName\": \"" + pvName + "\", \"status\": \"Archive request has been submitted\" }");
				}
			}
		} catch (Exception e) {
			logger.error("Exception establishing monitor ", e);
			throw new IOException(e);
		}
	}
}
