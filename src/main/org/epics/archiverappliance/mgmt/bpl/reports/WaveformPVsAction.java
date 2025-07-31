/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

/**
 * All the waveforms that are currently being archived.
 * 
 * @epics.BPLAction - Get a list of waveform PVs that are currently being archived. 
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class WaveformPVsAction implements BPLAction {
	private static final Logger logger = LogManager.getLogger(WaveformPVsAction.class);

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Getting a list of waveform PVs for this cluster");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		LinkedList<String> waveFormURLs = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			waveFormURLs.add(info.getEngineURL() + "/getArchivedWaveforms");
		}		
		try (PrintWriter out = resp.getWriter()) {
			JSONArray neverConnPVs = GetUrlContent.combineJSONArrays(waveFormURLs);
			out.println(JSONValue.toJSONString(neverConnPVs));
		}
	}
}
