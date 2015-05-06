/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Reports for PVs that never connected.
 * @author mshankar
 *
 */
public class NeverConnectedPVsAction implements BPLAction {
	private static final Logger logger = Logger.getLogger(NeverConnectedPVsAction.class);
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		logger.info("Getting the status of pvs that never connected since the start of this appliance");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		EngineContext engineContext = configService.getEngineContext();
		LinkedList<HashMap<String, String>> result = new LinkedList<HashMap<String, String>>(); 
		try (PrintWriter out = resp.getWriter()) {
			for(ArchiveChannel channel : engineContext.getChannelList().values()) {
				PVMetrics pvMetrics = channel.getPVMetrics();
				if(pvMetrics.getConnectionFirstEstablishedEpochSeconds() == 0L) {
					HashMap<String, String> pvStatus = new HashMap<String, String>();
					result.add(pvStatus);
					pvStatus.put("pvName", pvMetrics.getPvName());
					pvStatus.put("requestTime", TimeUtils.convertToHumanReadableString(pvMetrics.getConnectionRequestMadeEpochSeconds()));
				}
			}
			out.println(JSONValue.toJSONString(result));
		}
	}
}
