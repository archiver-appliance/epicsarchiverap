/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.TimeSpanDependentProcessing;
import org.epics.archiverappliance.retrieval.postprocessors.TimeSpanDependentProcessor;

/**
 * @author mshankar
 * Class for resolving data sources
 */
public class DataSourceResolution {
	private static Logger logger = Logger.getLogger(DataSourceResolution.class.getName());
	private ConfigService configService;
	
	public DataSourceResolution(ConfigService configService) {
		this.configService = configService;
		
	}

	public LinkedList<UnitOfRetrieval> resolveDataSources(String pvName, Timestamp start, Timestamp end, PVTypeInfo typeInfo, BasicContext context, PostProcessor postProcessor, HttpServletRequest req, HttpServletResponse resp, ApplianceInfo applianceForPV) throws IOException {
		LinkedList<UnitOfRetrieval> unitsofretrieval = new LinkedList<UnitOfRetrieval>();
		if(!applianceForPV.equals(configService.getMyApplianceInfo())) {
			logger.debug("Data for pv " + pvName + " is on appliance " + applianceForPV.getIdentity() + ". Remoting it thru this appliance.");
			try {
				URI redirectURI = new URI(applianceForPV.getRetrievalURL() + "/../data/getData.raw");
				String redirectURIStr = redirectURI.normalize().toString();
				logger.debug("Raw URL on remote appliance for pv " + pvName + " is " + redirectURIStr);
				String remoteRawURL = URLEncoder.encode(redirectURIStr, "UTF-8");
				StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin("pbraw://localhost?rawURL=" + remoteRawURL, configService);
				unitsofretrieval.add(new UnitOfRetrieval(storagePlugin.getDescription(), storagePlugin, typeInfo.getPvName(), pvName, start, end, postProcessor, context));
			} catch (URISyntaxException e) {
				throw new IOException(e);
			}
		} else { 
			List<DataSourceforPV> dataSources = configService.getRetrievalRuntimeState().getDataSources(pvName, typeInfo, start, end, req);

			List<TimeSpan> yearlySpans = TimeUtils.breakIntoYearlyTimeSpans(start, end);
			List<TimeSpanDependentProcessor> spannedProcessors = null;
			if(postProcessor != null && postProcessor instanceof TimeSpanDependentProcessing) {
				logger.debug("Giving the chance for the post processor to alter the time spans");
				spannedProcessors = ((TimeSpanDependentProcessing)postProcessor).generateTimeSpanDependentProcessors(yearlySpans);
			} else {
				spannedProcessors = TimeSpanDependentProcessor.sameProcessorForAllTimeSpans(yearlySpans, postProcessor);
			}

			for(TimeSpanDependentProcessor spannedProcessor : spannedProcessors) {
				for(DataSourceforPV dataSource : dataSources) {
					// Ideally we'd check to see if this data source has data in this time span.
					// However, this means we have to keep all the various infos updated. 
					// The MergeDedup now takes care of merging from multiple sources.
					try {
						if(dataSource.isOverridingStartAndEndTimes() && spannedProcessor.getTimeSpan().contains(dataSource.getRequestTimeSpan())) { 
							if(logger.isDebugEnabled()) { 
								logger.debug("Adding data source " + dataSource.getStoragePlugin().getDescription() 
										+ " using post processor " + spannedProcessor.getPostProcessor().getIdentity() 
										+ " from start time "  + TimeUtils.convertToHumanReadableString(dataSource.getRequestTimeSpan().getStartTime())
										+ " to end time " + TimeUtils.convertToHumanReadableString(dataSource.getRequestTimeSpan().getEndTime())
										);
							}
							StoragePlugin storagePlugin = dataSource.getStoragePlugin();
							unitsofretrieval.add(new UnitOfRetrieval(storagePlugin.getDescription(), storagePlugin, typeInfo.getPvName(), pvName, dataSource.getRequestTimeSpan().getStartTime(), dataSource.getRequestTimeSpan().getEndTime(), spannedProcessor.getPostProcessor(), context));							
						} else { 
							if(logger.isDebugEnabled()) { 
								logger.debug("Adding data source " + dataSource.getStoragePlugin().getDescription() 
										+ " using post processor " + spannedProcessor.getPostProcessor().getIdentity() 
										+ " from start time "  + TimeUtils.convertToHumanReadableString(spannedProcessor.getTimeSpan().getStartTime())
										+ " to end time " + TimeUtils.convertToHumanReadableString(spannedProcessor.getTimeSpan().getEndTime())
										);
							}
							StoragePlugin storagePlugin = dataSource.getStoragePlugin();
							unitsofretrieval.add(new UnitOfRetrieval(storagePlugin.getDescription(), storagePlugin, typeInfo.getPvName(), pvName, spannedProcessor.getTimeSpan().getStartTime(), spannedProcessor.getTimeSpan().getEndTime(), spannedProcessor.getPostProcessor(), context));
						}
					} catch(Exception ex) {
						logger.error("Exception initializing storage plugin", ex);
					}
				}
			}
		}
		
		return unitsofretrieval;
	}
}
