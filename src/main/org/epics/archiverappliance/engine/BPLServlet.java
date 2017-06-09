/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine;

import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.BasicDispatcher;
import org.epics.archiverappliance.common.GetVersion;
import org.epics.archiverappliance.common.ProcessMetricsReport;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.bpl.ArchiveChannelObjectDetailsAction;
import org.epics.archiverappliance.engine.bpl.ChangeArchivalParamsAction;
import org.epics.archiverappliance.engine.bpl.CleanUpAnyImmortalChannels;
import org.epics.archiverappliance.engine.bpl.DeletePV;
import org.epics.archiverappliance.engine.bpl.GetEngineDataAction;
import org.epics.archiverappliance.engine.bpl.GetLatestMetaDataAction;
import org.epics.archiverappliance.engine.bpl.PVStatusAction;
import org.epics.archiverappliance.engine.bpl.PauseArchivingPV;
import org.epics.archiverappliance.engine.bpl.PausePVsOnShutdown;
import org.epics.archiverappliance.engine.bpl.ResumeArchivingPV;
import org.epics.archiverappliance.engine.bpl.reports.ApplianceMetrics;
import org.epics.archiverappliance.engine.bpl.reports.ApplianceMetricsDetails;
import org.epics.archiverappliance.engine.bpl.reports.ConnectedPVCountForAppliance;
import org.epics.archiverappliance.engine.bpl.reports.CurrentlyDisconnectedPVsAction;
import org.epics.archiverappliance.engine.bpl.reports.DroppedEventsBufferOverflowReport;
import org.epics.archiverappliance.engine.bpl.reports.DroppedEventsTimestampReport;
import org.epics.archiverappliance.engine.bpl.reports.DroppedEventsTypeChangeReport;
import org.epics.archiverappliance.engine.bpl.reports.EventRateReport;
import org.epics.archiverappliance.engine.bpl.reports.InstanceReportDetails;
import org.epics.archiverappliance.engine.bpl.reports.ListAllChannels;
import org.epics.archiverappliance.engine.bpl.reports.LostConnectionsReport;
import org.epics.archiverappliance.engine.bpl.reports.NeverConnectedPVsAction;
import org.epics.archiverappliance.engine.bpl.reports.PVDetails;
import org.epics.archiverappliance.engine.bpl.reports.SilentPVReport;
import org.epics.archiverappliance.engine.bpl.reports.StorageRateReport;
import org.epics.archiverappliance.engine.bpl.reports.WaveformPVsAction;

/**
 * The main business logic servlet for the engine. All BPLActions are registered here.
 * @author mshankar
 *
 */
@SuppressWarnings("serial")
public class BPLServlet extends HttpServlet {
	private static final Logger logger = Logger.getLogger(BPLServlet.class);
	private static HashMap<String, Class<? extends BPLAction>> getActions = new HashMap<String, Class<? extends BPLAction>>();
	static {
		getActions.put("/getData.raw", GetEngineDataAction.class);
		getActions.put("/getMetadata", GetLatestMetaDataAction.class);
		getActions.put("/status", PVStatusAction.class);
		getActions.put("/getNeverConnectedPVsForThisAppliance", NeverConnectedPVsAction.class);
		getActions.put("/getCurrentlyDisconnectedPVsForThisAppliance", CurrentlyDisconnectedPVsAction.class);
		getActions.put("/getEventRateReport", EventRateReport.class);
		getActions.put("/getStorageRateReport", StorageRateReport.class);
		getActions.put("/getPVDetails", PVDetails.class);
		getActions.put("/getApplianceMetrics", ApplianceMetrics.class);
		getActions.put("/getApplianceMetricsForAppliance", ApplianceMetricsDetails.class);
		getActions.put("/getConnectedPVCountForAppliance", ConnectedPVCountForAppliance.class);
		getActions.put("/changeArchivalParameters", ChangeArchivalParamsAction.class);
		getActions.put("/getInstanceMetricsForAppliance", InstanceReportDetails.class);
		getActions.put("/getLostConnectionsReport", LostConnectionsReport.class);
		getActions.put("/getSilentPVsReport", SilentPVReport.class);
		getActions.put("/getPVsByDroppedEventsTimestamp",DroppedEventsTimestampReport.class);
		getActions.put("/getPVsByDroppedEventsBuffer", DroppedEventsBufferOverflowReport.class);
		getActions.put("/getPVsByDroppedEventsTypeChange", DroppedEventsTypeChangeReport.class);
		getActions.put("/pauseArchivingPV", PauseArchivingPV.class);
		getActions.put("/resumeArchivingPV", ResumeArchivingPV.class);
		getActions.put("/deletePV", DeletePV.class);
		getActions.put("/listAllChannels", ListAllChannels.class);
		getActions.put("/getProcessMetrics", ProcessMetricsReport.class);
		getActions.put("/pausePVsonShutdown", PausePVsOnShutdown.class);
		getActions.put("/cleanUpAnyImmortalChannels", CleanUpAnyImmortalChannels.class);
		getActions.put("/getVersion", GetVersion.class);
		getActions.put("/getArchivedWaveforms", WaveformPVsAction.class);
		getActions.put("/getArchiveChannelObjectDetails", ArchiveChannelObjectDetailsAction.class);
		
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String path = req.getPathInfo();
		logger.info("Beginning request into Engine servlet " + path);
		BasicDispatcher.dispatch(req, resp, configService, getActions);
	}
	
	private static HashMap<String, Class<? extends BPLAction>> postActions = new HashMap<String, Class<? extends BPLAction>>();
	static {
		postActions.put("/status", PVStatusAction.class);
		postActions.put("/pauseArchivingPV", PauseArchivingPV.class);
		postActions.put("/resumeArchivingPV", ResumeArchivingPV.class);
	}
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		logger.info("Beginning POST request into Engine servlet " + req.getPathInfo());
		BasicDispatcher.dispatch(req, resp, configService, postActions);
	}

	private ConfigService configService;
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		configService = (ConfigService) getServletConfig().getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
	}
}
