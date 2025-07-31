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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.model.MonitoredArchiveChannel;
import org.epics.archiverappliance.engine.model.ScannedArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * List of all the waveforms being archived.
 * @author mshankar
 *
 */
public class WaveformPVsAction implements BPLAction {
	private static final Logger logger = LogManager.getLogger(WaveformPVsAction.class);
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Getting a list of waveform PV's");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		EngineContext engineContext = configService.getEngineContext();
		LinkedList<HashMap<String, String>> result = new LinkedList<HashMap<String, String>>(); 
		try (PrintWriter out = resp.getWriter()) {
			for(ArchiveChannel channel : engineContext.getChannelList().values()) {
				PVMetrics pvMetrics = channel.getPVMetrics();
				if(pvMetrics.getArchDBRTypes().isWaveForm()) {
					HashMap<String, String> pvStatus = new HashMap<String, String>();
					result.add(pvStatus);
					pvStatus.put("pvName", pvMetrics.getPvName());
					SamplingMethod samplingMethod = SamplingMethod.DONT_ARCHIVE;
					if(channel instanceof MonitoredArchiveChannel) { 
						samplingMethod = SamplingMethod.MONITOR;
					} else if(channel instanceof ScannedArchiveChannel) {
						samplingMethod = SamplingMethod.SCAN;
					} else { 
						logger.warn("Unknown sampling mode for pv " + channel.getName());
					}
					pvStatus.put("samplingmethod", samplingMethod.toString());
					pvStatus.put("samplingperiod", Double.toString(pvMetrics.getSamplingPeriod()));
					pvStatus.put("elementCount", Integer.toString(pvMetrics.getElementCount()));
					pvStatus.put("dbrtype", pvMetrics.getArchDBRTypes().toString());
				}
			}
			out.println(JSONValue.toJSONString(result));
		}
	}
}
