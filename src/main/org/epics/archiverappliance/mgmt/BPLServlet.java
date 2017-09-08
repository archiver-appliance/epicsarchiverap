/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.BasicDispatcher;
import org.epics.archiverappliance.common.ProcessMetricsChartData;
import org.epics.archiverappliance.common.ProcessMetricsReport;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.bpl.AbortArchiveRequest;
import org.epics.archiverappliance.mgmt.bpl.AbortArchiveRequestForAppliance;
import org.epics.archiverappliance.mgmt.bpl.AddAliasAction;
import org.epics.archiverappliance.mgmt.bpl.AddExternalArchiverServer;
import org.epics.archiverappliance.mgmt.bpl.AddExternalArchiverServerArchives;
import org.epics.archiverappliance.mgmt.bpl.AggregatedApplianceInfo;
import org.epics.archiverappliance.mgmt.bpl.ArchivePVAction;
import org.epics.archiverappliance.mgmt.bpl.ArchivedPVsAction;
import org.epics.archiverappliance.mgmt.bpl.ArchivedPVsNotInListAction;
import org.epics.archiverappliance.mgmt.bpl.ChangeArchivalParamsAction;
import org.epics.archiverappliance.mgmt.bpl.ChangeTypeForPV;
import org.epics.archiverappliance.mgmt.bpl.ChannelArchiverListView;
import org.epics.archiverappliance.mgmt.bpl.ConsolidatePBFilesForOnePV;
import org.epics.archiverappliance.mgmt.bpl.DeletePV;
import org.epics.archiverappliance.mgmt.bpl.ExportConfig;
import org.epics.archiverappliance.mgmt.bpl.ExportConfigForThisInstance;
import org.epics.archiverappliance.mgmt.bpl.GetAllExpandedPVNames;
import org.epics.archiverappliance.mgmt.bpl.GetAllPVs;
import org.epics.archiverappliance.mgmt.bpl.GetApplianceInfo;
import org.epics.archiverappliance.mgmt.bpl.GetAppliancesInCluster;
import org.epics.archiverappliance.mgmt.bpl.GetMatchingPVsForAppliance;
import org.epics.archiverappliance.mgmt.bpl.GetPVStatusAction;
import org.epics.archiverappliance.mgmt.bpl.GetPVTypeInfo;
import org.epics.archiverappliance.mgmt.bpl.GetPVsForThisAppliance;
import org.epics.archiverappliance.mgmt.bpl.GetPausedPVsForThisAppliance;
import org.epics.archiverappliance.mgmt.bpl.GetStoresForPV;
import org.epics.archiverappliance.mgmt.bpl.GetVersions;
import org.epics.archiverappliance.mgmt.bpl.ImportChannelArchiverConfigAction;
import org.epics.archiverappliance.mgmt.bpl.ImportConfig;
import org.epics.archiverappliance.mgmt.bpl.ImportConfigForAppliance;
import org.epics.archiverappliance.mgmt.bpl.ImportDataFromPlugin;
import org.epics.archiverappliance.mgmt.bpl.ModifyMetaFieldsAction;
import org.epics.archiverappliance.mgmt.bpl.NamedFlagsGet;
import org.epics.archiverappliance.mgmt.bpl.NamedFlagsSet;
import org.epics.archiverappliance.mgmt.bpl.PauseArchivingPV;
import org.epics.archiverappliance.mgmt.bpl.PutPVTypeInfo;
import org.epics.archiverappliance.mgmt.bpl.RefreshPVDataFromChannelArchivers;
import org.epics.archiverappliance.mgmt.bpl.RemoveAliasAction;
import org.epics.archiverappliance.mgmt.bpl.RemoveExternalArchiverServer;
import org.epics.archiverappliance.mgmt.bpl.RenamePVAction;
import org.epics.archiverappliance.mgmt.bpl.ReshardPV;
import org.epics.archiverappliance.mgmt.bpl.ResumeArchivingPV;
import org.epics.archiverappliance.mgmt.bpl.SkipAliasCheckAction;
import org.epics.archiverappliance.mgmt.bpl.UnarchivedPVsAction;
import org.epics.archiverappliance.mgmt.bpl.UploadChannelArchiverConfigAction;
import org.epics.archiverappliance.mgmt.bpl.cahdlers.CompareWithChannelArchiver;
import org.epics.archiverappliance.mgmt.bpl.reports.ApplianceMetrics;
import org.epics.archiverappliance.mgmt.bpl.reports.ApplianceMetricsDetails;
import org.epics.archiverappliance.mgmt.bpl.reports.CreationTimeReportForAppliance;
import org.epics.archiverappliance.mgmt.bpl.reports.CurrentlyDisconnectedPVs;
import org.epics.archiverappliance.mgmt.bpl.reports.DroppedEventsBufferOverflowReport;
import org.epics.archiverappliance.mgmt.bpl.reports.DroppedEventsTimestampReport;
import org.epics.archiverappliance.mgmt.bpl.reports.DroppedEventsTypeChangeReport;
import org.epics.archiverappliance.mgmt.bpl.reports.EventRateReport;
import org.epics.archiverappliance.mgmt.bpl.reports.InstanceReport;
import org.epics.archiverappliance.mgmt.bpl.reports.InstanceReportDetails;
import org.epics.archiverappliance.mgmt.bpl.reports.LostConnectionsReport;
import org.epics.archiverappliance.mgmt.bpl.reports.NeverConnectedPVsAction;
import org.epics.archiverappliance.mgmt.bpl.reports.NeverConnectedPVsForThisAppliance;
import org.epics.archiverappliance.mgmt.bpl.reports.PVDetails;
import org.epics.archiverappliance.mgmt.bpl.reports.PVsByStorageConsumed;
import org.epics.archiverappliance.mgmt.bpl.reports.PausedPVsReport;
import org.epics.archiverappliance.mgmt.bpl.reports.RecentlyAddedPVs;
import org.epics.archiverappliance.mgmt.bpl.reports.RecentlyAddedPVsforThisInstance;
import org.epics.archiverappliance.mgmt.bpl.reports.RecentlyChangedPVs;
import org.epics.archiverappliance.mgmt.bpl.reports.RecentlyChangedPVsforThisInstance;
import org.epics.archiverappliance.mgmt.bpl.reports.SilentPVReport;
import org.epics.archiverappliance.mgmt.bpl.reports.StorageRateReport;
import org.epics.archiverappliance.mgmt.bpl.reports.StorageReport;
import org.epics.archiverappliance.mgmt.bpl.reports.StorageReportDetails;
import org.epics.archiverappliance.mgmt.bpl.reports.TimeSpanReport;
import org.epics.archiverappliance.mgmt.bpl.reports.WaveformPVsAction;
import org.epics.archiverappliance.mgmt.policy.GetApplianceProps;
import org.epics.archiverappliance.mgmt.policy.GetPolicyList;
import org.epics.archiverappliance.mgmt.policy.GetPolicyText;

/**
 * The main business logic servlet for mgmt. All BPLActions are registered here.
 * @author mshankar
 *
 */
@SuppressWarnings("serial")
public class BPLServlet extends HttpServlet {
	private static HashMap<String, Class<? extends BPLAction>> getActions = new HashMap<String, Class<? extends BPLAction>>();
	private static LinkedList<String> actionsSequenceForDocs = new LinkedList<String>();
	
	static {
		// BPL related to PVs/appliances etc
		addAction("/getAllPVs", GetAllPVs.class);
		addAction("/getAllExpandedPVNames", GetAllExpandedPVNames.class);
		addAction("/getPVStatus", GetPVStatusAction.class);
		addAction("/getPVTypeInfo", GetPVTypeInfo.class);
		addAction("/archivePV", ArchivePVAction.class);
		addAction("/pauseArchivingPV", PauseArchivingPV.class);
		addAction("/resumeArchivingPV", ResumeArchivingPV.class);
		addAction("/getStoresForPV", GetStoresForPV.class);
		addAction("/consolidateDataForPV", ConsolidatePBFilesForOnePV.class);
		addAction("/deletePV", DeletePV.class);
		addAction("/abortArchivingPV", AbortArchiveRequest.class);
		addAction("/abortArchivingPVForThisAppliance", AbortArchiveRequestForAppliance.class);
		addAction("/changeArchivalParameters", ChangeArchivalParamsAction.class);
		addAction("/getPVDetails", PVDetails.class);
		addAction("/getApplianceInfo", GetApplianceInfo.class);
		addAction("/getAppliancesInCluster", GetAppliancesInCluster.class);
		addAction("/renamePV", RenamePVAction.class);
		addAction("/reshardPV", ReshardPV.class);
		addAction("/addAlias", AddAliasAction.class);
		addAction("/removeAlias", RemoveAliasAction.class);
		addAction("/skipAliasCheck", SkipAliasCheckAction.class);
		addAction("/changeTypeForPV", ChangeTypeForPV.class);
		addAction("/getVersions", GetVersions.class);
		addAction("/modifyMetaFields", ModifyMetaFieldsAction.class);
		addAction("/getNamedFlag", NamedFlagsGet.class);
		addAction("/setNamedFlag", NamedFlagsSet.class);
		

		// BPL related to reports
		addAction("/getNeverConnectedPVs", NeverConnectedPVsAction.class);
		addAction("/getNeverConnectedPVsForThisAppliance", NeverConnectedPVsForThisAppliance.class);
		addAction("/getCurrentlyDisconnectedPVs", CurrentlyDisconnectedPVs.class);
		addAction("/getEventRateReport", EventRateReport.class);
		addAction("/getStorageRateReport", StorageRateReport.class);
		addAction("/getRecentlyAddedPVs", RecentlyAddedPVs.class);
		addAction("/getRecentlyAddedPVsForThisInstance", RecentlyAddedPVsforThisInstance.class);
		addAction("/getRecentlyModifiedPVs", RecentlyChangedPVs.class);
		addAction("/getRecentlyModifiedPVsForThisInstance", RecentlyChangedPVsforThisInstance.class);
		addAction("/getStorageMetrics", StorageReport.class);
		addAction("/getStorageMetricsForAppliance", StorageReportDetails.class);
		addAction("/getPVsByStorageConsumed", PVsByStorageConsumed.class);
		addAction("/getLostConnectionsReport", LostConnectionsReport.class);
		addAction("/getSilentPVsReport", SilentPVReport.class);
		addAction("/getPVsForThisAppliance", GetPVsForThisAppliance.class);
		addAction("/getPVsByDroppedEventsTimestamp",DroppedEventsTimestampReport.class);
		addAction("/getPVsByDroppedEventsBuffer", DroppedEventsBufferOverflowReport.class);
		addAction("/getPVsByDroppedEventsTypeChange", DroppedEventsTypeChangeReport.class);
		addAction("/getPausedPVsReport", PausedPVsReport.class);
		addAction("/getPausedPVsForThisAppliance", GetPausedPVsForThisAppliance.class);
		addAction("/getArchivedWaveforms", WaveformPVsAction.class);	
		addAction("/getTimeSpanReport", TimeSpanReport.class);
		
		
		// Others.
		addAction("/getPolicyText", GetPolicyText.class);
		addAction("/exportConfig", ExportConfig.class);
		addAction("/exportConfigForAppliance", ExportConfigForThisInstance.class);
		addAction("/getInstanceMetrics", InstanceReport.class);
		addAction("/getInstanceMetricsForAppliance", InstanceReportDetails.class);
		addAction("/getApplianceMetrics", ApplianceMetrics.class);
		addAction("/getApplianceMetricsForAppliance", ApplianceMetricsDetails.class);
		addAction("/getExternalArchiverServers", ChannelArchiverListView.class);
		addAction("/addExternalArchiverServer", AddExternalArchiverServer.class);
		addAction("/addExternalArchiverServerArchives", AddExternalArchiverServerArchives.class);
		addAction("/removeExternalArchiverServer", RemoveExternalArchiverServer.class);
		addAction("/test/compareWithChannelArchiver", CompareWithChannelArchiver.class);
		addAction("/getAggregatedApplianceInfo", AggregatedApplianceInfo.class);
		addAction("/importDataFromPlugin", ImportDataFromPlugin.class);
		addAction("/getPolicyList", GetPolicyList.class);		
		addAction("/getApplianceProperties", GetApplianceProps.class);		
		addAction("/webAppReady", WebappReady.class);
		addAction("/getProcessMetrics", ProcessMetricsReport.class);
		addAction("/getProcessMetricsDataForAppliance", ProcessMetricsChartData.class);
		addAction("/refreshPVDataFromChannelArchivers", RefreshPVDataFromChannelArchivers.class);
		addAction("/getMatchingPVsForThisAppliance", GetMatchingPVsForAppliance.class);
		addAction("/getCreationReportForAppliance", CreationTimeReportForAppliance.class);		
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		BasicDispatcher.dispatch(req, resp, configService, getActions);
	}

	

	private ConfigService configService;
	@Override
	public void init() throws ServletException {
		super.init();
		configService = (ConfigService) getServletConfig().getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
	}
	

	private static HashMap<String, Class<? extends BPLAction>> postActions = new HashMap<String, Class<? extends BPLAction>>();
	static {
		addPostAction("/importChannelArchiverConfiguration", ImportChannelArchiverConfigAction.class);
		addPostAction("/uploadChannelArchiverConfiguration", UploadChannelArchiverConfigAction.class);
		addPostAction("/importConfig", ImportConfig.class);
		addPostAction("/importConfigForAppliance", ImportConfigForAppliance.class);
		addPostAction("/archivePV", ArchivePVAction.class);
		addPostAction("/getPVStatus", GetPVStatusAction.class);
		addPostAction("/pauseArchivingPV", PauseArchivingPV.class);
		addPostAction("/resumeArchivingPV", ResumeArchivingPV.class);
		addPostAction("/putPVTypeInfo", PutPVTypeInfo.class);
		addPostAction("/unarchivedPVs", UnarchivedPVsAction.class);
		addPostAction("/archivedPVs", ArchivedPVsAction.class);
		addPostAction("/archivedPVsForThisAppliance", ArchivedPVsAction.class);
		addPostAction("/archivedPVsNotInList", ArchivedPVsNotInListAction.class);
	}
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		BasicDispatcher.dispatch(req, resp, configService, postActions);
	}
	
	/**
	 * Add so that we can maintain the sequence of addition as well. 
	 * @param path
	 * @param bplClassName
	 */
	private static void addAction(String path, Class<? extends BPLAction> bplClassName) { 
		getActions.put(path, bplClassName);
		actionsSequenceForDocs.add(path);
	}
	
	private static void addPostAction(String path, Class<? extends BPLAction> bplClassName) { 
		postActions.put(path, bplClassName);
		if(!actionsSequenceForDocs.contains(path)) { 
			actionsSequenceForDocs.add(path);
		}
	}

	
	/**
	 * The main method here is used only to generate documentation for the scripting guide. 
	 * No other functionality is provided
	 * @param args  &emsp;
	 * @throws IOException  &emsp;
	 */
	public static void main(String[] args) throws IOException {
		System.out.println("#Path mappings for mgmt BPLs");
		for(String path : actionsSequenceForDocs) { 
			Class<? extends BPLAction> classObj = getActions.get(path);
			if(classObj == null) { 
				classObj = postActions.get(path);
			}
			if(classObj == null) { 
				System.err.println("Invalid registration for " + path);
			}
			System.out.println(path + "=" + classObj.getName());
		}
	}
}
