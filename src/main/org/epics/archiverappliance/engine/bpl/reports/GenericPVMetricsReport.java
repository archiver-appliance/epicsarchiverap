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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * We collect many metrics during archiving in the PVMetrics objects. Most of these are doubles/longs. 
 * We should be able to report on these metrics using a generic framework. 
 * This is the superclass for such a framework.
 * Subclass this and then register the subclass in the BPL...
 * @author mshankar
 *
 */
public class GenericPVMetricsReport<T extends Number> implements BPLAction {
	private static final Logger logger = Logger.getLogger(GenericPVMetricsReport.class);
	private Function<PVMetrics, T> getFn;
	private final String metricName; 
	private String applianceName;

	public GenericPVMetricsReport(Function<PVMetrics, T> getFn, String metricName) { 
		this.getFn = getFn;
		this.metricName = metricName;
	}
	
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		String limit = req.getParameter("limit");
		logger.info(metricName + " report for " + (limit == null ? "default limit " : ("limit " + limit)));
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		List<HashMap<String, String>> result = applyMetric(configService, limit);
		try (PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(result));
		}
	}
	
	private List<HashMap<String, String>> applyMetric(ConfigService configService, String limitStr) {
		this.applianceName = configService.getMyApplianceInfo().getIdentity();
		HashMap<String, T> pvs2Rate = new HashMap<String, T>();
		EngineContext engineContext = configService.getEngineContext();
		for(ArchiveChannel channel : engineContext.getChannelList().values()) {
			pvs2Rate.put(channel.getName(), this.getFn.apply(channel.getPVMetrics()));
		}
		int limitNum = pvs2Rate.size();
		if(limitStr != null) {
			limitNum = Integer.parseInt(limitStr);
		}

		List<HashMap<String, String>>  retVal = pvs2Rate.entrySet()
        .stream()
        .filter(new Predicate<Entry<String, T>>() { // Get rid of entries that have 0
			@Override
			public boolean test(Entry<String, T> t) {
				return t.getValue().doubleValue() > 0;
			}
		})
        .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
        .limit(limitNum)
        .map(new Function<Entry<String, T>, HashMap<String, String>>() {
			@Override
			public HashMap<String, String> apply(Entry<String, T> t) {
				HashMap<String, String> ret = new HashMap<String, String>();
				ret.put("pvName", t.getKey());
				ret.put("instance", GenericPVMetricsReport.this.applianceName);
				ret.put(metricName, t.getValue().toString());
				return ret;
			}
		})
        .collect(Collectors.toList());
		
		return retVal;		
	}

}
