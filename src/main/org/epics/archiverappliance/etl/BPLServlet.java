/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.BasicDispatcher;
import org.epics.archiverappliance.common.GetVersion;
import org.epics.archiverappliance.common.ProcessMetricsReport;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.bpl.ConsolidatePBFilesForOnePV;
import org.epics.archiverappliance.etl.bpl.DeletePV;
import org.epics.archiverappliance.etl.bpl.GetLastKnownEventTimeStamp;
import org.epics.archiverappliance.etl.bpl.reports.ApplianceMetrics;
import org.epics.archiverappliance.etl.bpl.reports.ApplianceMetricsDetails;
import org.epics.archiverappliance.etl.bpl.reports.InstanceReportDetails;
import org.epics.archiverappliance.etl.bpl.reports.PVDetails;
import org.epics.archiverappliance.etl.bpl.reports.PVsByStorageConsumed;
import org.epics.archiverappliance.etl.bpl.reports.StorageDetailsForAppliance;
import org.epics.archiverappliance.etl.bpl.reports.StorageMetricsForAppliance;

/**
 * The main business logic servlet for ETL. All BPLActions are registered here.
 * @author mshankar
 *
 */
@SuppressWarnings("serial")
public class BPLServlet extends HttpServlet {
	private static final Logger logger = LogManager.getLogger(BPLServlet.class);

	private static HashMap<String, Class<? extends BPLAction>> getActions = new HashMap<String, Class<? extends BPLAction>>();
	static {
		getActions.put("/getPVDetails", PVDetails.class);
		getActions.put("/getApplianceMetrics", ApplianceMetrics.class);
		getActions.put("/getApplianceMetricsForAppliance", ApplianceMetricsDetails.class);
		getActions.put("/getStorageMetricsForAppliance", StorageMetricsForAppliance.class);
		getActions.put("/getStorageDetailsForAppliance", StorageDetailsForAppliance.class);
		getActions.put("/getPVsByStorageConsumed", PVsByStorageConsumed.class);
		getActions.put("/getInstanceMetricsForAppliance", InstanceReportDetails.class);
		getActions.put("/getLastKnownEvent", GetLastKnownEventTimeStamp.class);
		getActions.put("/consolidateDataForPV", ConsolidatePBFilesForOnePV.class);
		getActions.put("/deletePV", DeletePV.class);
		getActions.put("/getProcessMetrics", ProcessMetricsReport.class);
		getActions.put("/getVersion", GetVersion.class);
	}



	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		BasicDispatcher.dispatch(req, resp, configService, getActions);
	}	

	@Override
	public void init() throws ServletException {
		super.init();
		configService = (ConfigService) getServletConfig().getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
		logger.info("Done initializing ETL servlet");
	}

	private ConfigService configService;
}
