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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Report for PVs by event rate.
 * @author mshankar
 *
 */
public class EventRateReport implements BPLAction {
	private static class PVEventRate {
		String pvName;
		double eventRate;
		
		PVEventRate(String pvName, double eventRate) {
			this.pvName = pvName;
			this.eventRate = eventRate;
		}
	}
	
	private static final Logger logger = LogManager.getLogger(EventRateReport.class);
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		String limit = req.getParameter("limit");
		logger.info("Event rate report for " + (limit == null ? "default limit " : ("limit " + limit)));
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		List<PVEventRate> eventRates = getEventRates(configService, limit);
		LinkedList<HashMap<String, String>> result = new LinkedList<HashMap<String, String>>();
		try (PrintWriter out = resp.getWriter()) {
			for(PVEventRate eventRate : eventRates) {
				HashMap<String, String> pvStatus = new HashMap<String, String>();
				result.add(pvStatus);
				pvStatus.put("pvName", eventRate.pvName);
				pvStatus.put("eventRate", Double.toString(eventRate.eventRate));
			}
			out.println(JSONValue.toJSONString(result));
		}
	}
	
	private static List<PVEventRate> getEventRates(ConfigService configService, String limit) {
		ArrayList<PVEventRate> eventRates = new ArrayList<PVEventRate>(); 
		EngineContext engineContext = configService.getEngineContext();
		for(ArchiveChannel channel : engineContext.getChannelList().values()) {
			eventRates.add(new PVEventRate(channel.getName(), channel.getPVMetrics().getEventRate()));
		}
		
		Collections.sort(eventRates, new Comparator<PVEventRate>() {
			@Override
			public int compare(PVEventRate o1, PVEventRate o2) {
				if(o1.eventRate == o2.eventRate) return 0;
				return (o1.eventRate < o2.eventRate) ? 1 : -1; // We want a descending sort
			}
		});
		
		if(limit == null) {
			return eventRates;
		}
		
		int limitNum = Integer.parseInt(limit);
		return eventRates.subList(0, Math.min(limitNum, eventRates.size()));
		
		
	}

}
