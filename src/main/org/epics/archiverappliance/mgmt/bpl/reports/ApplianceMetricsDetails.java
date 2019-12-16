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
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceAggregateInfo;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningData;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

/**
 * Detailed metrics for an appliance
 * @author mshankar
 *
 */
public class ApplianceMetricsDetails implements BPLAction {
	private static Logger logger = Logger.getLogger(ApplianceMetricsDetails.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String applianceIdentity = req.getParameter("appliance");
		logger.info("Getting the detailed metrics for the appliance " + applianceIdentity);
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		String applianceDetailsURLSnippet = "/getApplianceMetricsForAppliance?appliance=" + URLEncoder.encode(applianceIdentity, "UTF-8");
		ApplianceInfo info = configService.getAppliance(applianceIdentity);
		try (PrintWriter out = resp.getWriter()) {
			DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
			DecimalFormat noSignificantDigits = new DecimalFormat("###,###,###,###,###,###");
			LinkedList<Map<String, String>> result = new LinkedList<Map<String, String>>();
			addDetailedStatus(result, "Appliance Identity", applianceIdentity);
			
			logger.debug("Asking engine using " + info.getEngineURL() + applianceDetailsURLSnippet);
			JSONArray engineStatusVars = GetUrlContent.getURLContentAsJSONArray(info.getEngineURL() + applianceDetailsURLSnippet );
			if(engineStatusVars == null) {
				logger.warn("No status vars from engine using URL " + info.getEngineURL() + applianceDetailsURLSnippet);
			} else {
				GetUrlContent.combineJSONArrays(result, engineStatusVars);
			}
			
			logger.debug("Asking ETL using " + info.getEtlURL() + applianceDetailsURLSnippet);
			JSONArray etlStatusVars = GetUrlContent.getURLContentAsJSONArray(info.getEtlURL() + applianceDetailsURLSnippet );
			if(etlStatusVars == null) {
				logger.warn("No status vars from ETL using URL " + info.getEtlURL() + applianceDetailsURLSnippet);
			} else {
				GetUrlContent.combineJSONArrays(result, etlStatusVars);
			}

			logger.debug("Asking retrieval using " + info.getEngineURL() + applianceDetailsURLSnippet);
			JSONArray retrievalStatusVars = GetUrlContent.getURLContentAsJSONArray(info.getRetrievalURL() + applianceDetailsURLSnippet);
			if(retrievalStatusVars == null) {
				logger.warn("No status vars from retrieval using URL " + info.getRetrievalURL() + applianceDetailsURLSnippet);
			} else {
				GetUrlContent.combineJSONArrays(result, retrievalStatusVars);
			}
			
			logger.debug("Computing local stats " + info.getEngineURL() + applianceDetailsURLSnippet);

			addDetailedStatus(result, "PVs in archive workflow", Integer.toString(configService.getMgmtRuntimeState().getPVsPendingInWorkflow()));

			CapacityPlanningData capacityPlanningMetrics = CapacityPlanningData.getMetricsForAppliances(configService).cpApplianceMetrics.get(configService.getMyApplianceInfo());
			ApplianceAggregateInfo applianceAggregateDifferenceFromLastFetch = capacityPlanningMetrics.getApplianceAggregateDifferenceFromLastFetch(configService);
			addDetailedStatus(result, "Capacity planning last update", capacityPlanningMetrics.getStaticDataLastUpdated());
			addDetailedStatus(result, "Engine write thread usage", twoSignificantDigits.format(capacityPlanningMetrics.getEngineWriteThreadUsage(PVTypeInfo.DEFAULT_BUFFER_INTERVAL)));
			addDetailedStatus(result, "Aggregated appliance storage rate (in GB/year)", twoSignificantDigits.format((configService.getAggregatedApplianceInfo(info).getTotalStorageRate()*60*60*24*365)/(1024*1024*1024)));
			addDetailedStatus(result, "Aggregated appliance event rate (in events/sec)", twoSignificantDigits.format(configService.getAggregatedApplianceInfo(info).getTotalEventRate()));
			addDetailedStatus(result, "Aggregated appliance PV count", noSignificantDigits.format(configService.getAggregatedApplianceInfo(info).getTotalPVCount()));
			addDetailedStatus(result, "Incremental appliance storage rate (in GB/year)", twoSignificantDigits.format((applianceAggregateDifferenceFromLastFetch.getTotalStorageRate()*60*60*24*365)/(1024*1024*1024)));
			addDetailedStatus(result, "Incremental appliance event rate (in events/sec)", twoSignificantDigits.format(applianceAggregateDifferenceFromLastFetch.getTotalEventRate()));
			addDetailedStatus(result, "Incremental appliance PV count", noSignificantDigits.format(applianceAggregateDifferenceFromLastFetch.getTotalPVCount()));
			
			out.println(JSONValue.toJSONString(result));
		}
	}

	private static void addDetailedStatus(LinkedList<Map<String, String>> statuses, String name, String value) {
		Map<String, String> obj = new LinkedHashMap<String, String>();
		obj.put("name", name);
		obj.put("value", value);
		obj.put("source", "mgmt");
		statuses.add(obj);
	}
}