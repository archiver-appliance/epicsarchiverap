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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Report for PV by storage rate
 * @author mshankar
 *
 */
public class StorageRateReport implements BPLAction {
	private static final Logger logger = LogManager.getLogger(StorageRateReport.class);
	private static class PVStorageRate {
		String pvName;
		double storageRate;
		
		PVStorageRate(String pvName, double storageRate) {
			this.pvName = pvName;
			this.storageRate = storageRate;
		}
	}
	

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		String limit = req.getParameter("limit");
		logger.info("Storage rate report for " + (limit == null ? "default limit " : ("limit " + limit)));
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		List<PVStorageRate> storageRates = getStorageRates(configService, limit);
		LinkedList<HashMap<String, String>> result = new LinkedList<HashMap<String, String>>();
		try (PrintWriter out = resp.getWriter()) {
			for(PVStorageRate storageRate : storageRates) {
				HashMap<String, String> pvStatus = new HashMap<String, String>();
				result.add(pvStatus);
				pvStatus.put("pvName", storageRate.pvName);
				pvStatus.put("storageRate_KBperHour", Double.toString(storageRate.storageRate*60*60/1024));
				pvStatus.put("storageRate_MBperDay", Double.toString(storageRate.storageRate*60*60*24/(1024*1024)));
				pvStatus.put("storageRate_GBperYear", Double.toString(storageRate.storageRate*60*60*24*365/(1024*1024*1024)));
			}
			out.println(JSONValue.toJSONString(result));
		}
	}

	private static List<PVStorageRate> getStorageRates(ConfigService configService, String limit) {
		ArrayList<PVStorageRate> storageRates = new ArrayList<PVStorageRate>(); 
		EngineContext engineContext = configService.getEngineContext();
		for(ArchiveChannel channel : engineContext.getChannelList().values()) {
			PVMetrics pvMetrics = channel.getPVMetrics();
			storageRates.add(new PVStorageRate(pvMetrics.getPvName(), pvMetrics.getStorageRate()));
		}

		Collections.sort(storageRates, new Comparator<PVStorageRate>() {
			@Override
			public int compare(PVStorageRate o1, PVStorageRate o2) {
				if(o1.storageRate == o2.storageRate) return 0;
				return (o1.storageRate < o2.storageRate) ? 1 : -1; // We want a descending sort
			}
		});

		if(limit == null) {
			return storageRates;
		}

		int limitNum = Integer.parseInt(limit);
		return storageRates.subList(0, Math.min(limitNum, storageRates.size()));
	}
}