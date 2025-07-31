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
import java.util.function.Function;

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
 * Generic reporting class to call all the appliances with a particular URL and then return the results.
 * Subclass this class and then register with the BPLServlet.
 * Do no forget to add some documentation so that it shows up in the BPL docs
 * 
 * @author mshankar
 *
 */
public class GenericMultiApplianceReport implements BPLAction {
	private static final Logger logger = LogManager.getLogger(GenericMultiApplianceReport.class);
	private Function<ApplianceInfo,String> urlPrefixFn;
	private final String urlSuffix; 
	private final String reportName; 
	
	public GenericMultiApplianceReport(Function<ApplianceInfo,String> urlPrefixFn, String urlSuffix, String reportName) {
		this.urlPrefixFn = urlPrefixFn;
		this.urlSuffix = urlSuffix;
		this.reportName = reportName;
	}
	
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		String limit = req.getParameter("limit");
		logger.info(reportName + " report for " + (limit == null ? "default limit " : ("limit " + limit)));
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		LinkedList<String> reportURLs = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			reportURLs.add(this.urlPrefixFn.apply(info) + this.urlSuffix + (limit == null ? "" : ("?limit=" + limit)));
		}		
		try (PrintWriter out = resp.getWriter()) {
			JSONArray retVal = GetUrlContent.combineJSONArrays(reportURLs);
			out.println(JSONValue.toJSONString(retVal));
		}
	}

}
