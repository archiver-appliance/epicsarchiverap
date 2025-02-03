/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.TimeSpanDependentProcessing;
import org.epics.archiverappliance.retrieval.postprocessors.TimeSpanDependentProcessor;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author mshankar
 * Class for resolving data sources
 */
public class DataSourceResolution {
	private static final Logger logger = LogManager.getLogger(DataSourceResolution.class.getName());
	private final ConfigService configService;
	
	public DataSourceResolution(ConfigService configService) {
		this.configService = configService;
		
	}

	/**
	 * <p>
	 * Returns a list of units of retrieval. I.e., the storage plugin description, the storage plugin object used to retrieve data,
	 * the type information for the PV, the PV name, the start and end timestamp, the postprocessor that should be used and the
	 * context in which the data should be retrieved
	 * </p>
	 * <p>
	 * If the PV name that was specified in the method call is not to be found in the current appliance, the storage plugin object
	 * stored in the UnitOfRetrieval object will be a PBOverHTTPStoragePlugin object.
	 * </p>
	 * @param pvName The name of PV.
     * @param start  Instant
     * @param end Instant
	 * @param typeInfo PVTypeInf 
	 * @param context BasicContext
	 * @param postProcessor PostProcessor
	 * @param req HttpServletRequest
	 * @param applianceForPV ApplianceInfo
	 * @return UnitOfRetrieval 
	 * @throws IOException  &emsp; 
	 * A LinkedList of UnitOfRetrieval objects that contain the storage plguin description, the storage plugin object used to retrieve data,
	 * the type information of the PV, the PV name, the start and end timestamp, the postprocessor that should be used and the context in
	 * which data should be retrieved.
	 * @throws IOException
	 * Thrown if there is a syntax error in the URI.
	 */
	public LinkedList<UnitOfRetrieval> resolveDataSources(String pvName, Instant start, Instant end, PVTypeInfo typeInfo, BasicContext context, PostProcessor postProcessor, HttpServletRequest req, ApplianceInfo applianceForPV) throws IOException {
		LinkedList<UnitOfRetrieval> unitsofretrieval = new LinkedList<UnitOfRetrieval>();
		if(!applianceForPV.equals(configService.getMyApplianceInfo())) {
			logger.debug("Data for pv " + pvName + " is on appliance " + applianceForPV.getIdentity() + ". Remoting it thru this appliance.");
			try {
				URI redirectURI = new URI(applianceForPV.getRetrievalURL() + "/../data/getData.raw");
				String redirectURIStr = redirectURI.normalize().toString();
				logger.debug("Raw URL on remote appliance for pv " + pvName + " is " + redirectURIStr);
				String remoteRawURL = URLEncoder.encode(redirectURIStr, StandardCharsets.UTF_8);
				StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin("pbraw://localhost?rawURL=" + remoteRawURL, configService);
				unitsofretrieval.add(new UnitOfRetrieval(storagePlugin.getDescription(), storagePlugin, typeInfo.getPvName(), pvName, start, end, postProcessor, context));
			} catch (URISyntaxException e) {
				throw new IOException(e);
			}
		} else { 
			List<DataSourceforPV> dataSources = configService.getRetrievalRuntimeState().getDataSources(context, pvName, typeInfo, start, end, req);

			List<TimeSpan> yearlySpans = TimeUtils.breakIntoYearlyTimeSpans(start, end);
			List<TimeSpanDependentProcessor> spannedProcessors = null;
			if(postProcessor instanceof TimeSpanDependentProcessing) {
				logger.debug("Giving the chance for the post processor to alter the time spans");
				spannedProcessors = ((TimeSpanDependentProcessing)postProcessor).generateTimeSpanDependentProcessors(yearlySpans);
			} else {
				spannedProcessors = TimeSpanDependentProcessor.sameProcessorForAllTimeSpans(yearlySpans, postProcessor);
			}

			for(TimeSpanDependentProcessor spannedProcessor : spannedProcessors) {
				for(DataSourceforPV dataSource : dataSources) {
					// Check to see if there is a named flag that turns off this data source. 
					String namedFlagForSkippingDataSource = "SKIP_" + dataSource.getStoragePlugin().getName() + "_FOR_RETRIEVAL";
					if(configService.getNamedFlag(namedFlagForSkippingDataSource)) {
						logger.warn("Skipping " + dataSource.getStoragePlugin().getName() + " as the named flag " + namedFlagForSkippingDataSource + " is set");
						continue;
					}

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
							unitsofretrieval.add(new UnitOfRetrieval(storagePlugin.getDescription(), storagePlugin, typeInfo.getPvName(), pvName, 
									dataSource.getRequestTimeSpan().getStartTime(), dataSource.getRequestTimeSpan().getEndTime(), spannedProcessor.getPostProcessor(), context));							
						} else { 
							if(logger.isDebugEnabled()) { 
								logger.debug("Adding data source " + dataSource.getStoragePlugin().getDescription() 
										+ " using post processor " + spannedProcessor.getPostProcessor().getIdentity() 
										+ " from start time "  + TimeUtils.convertToHumanReadableString(spannedProcessor.getTimeSpan().getStartTime())
										+ " to end time " + TimeUtils.convertToHumanReadableString(spannedProcessor.getTimeSpan().getEndTime())
										);
							}
							StoragePlugin storagePlugin = dataSource.getStoragePlugin();
							unitsofretrieval.add(new UnitOfRetrieval(storagePlugin.getDescription(), storagePlugin, typeInfo.getPvName(), pvName, 
									spannedProcessor.getTimeSpan().getStartTime(), spannedProcessor.getTimeSpan().getEndTime(), spannedProcessor.getPostProcessor(), context));
						}
					} catch(Exception ex) {
						logger.error("Exception initializing storage plugin", ex);
					}
				}
			}

			if(RetrievalState.includeExternalServers(req)) {
				String failoverServer = configService.getFailoverApplianceURL(pvName);
				if(failoverServer != null) {
					logger.debug("Including the failover server " + failoverServer + " during data retrieval for PV " + pvName);
					String pluginDefString = "pbraw://localhost?name=failover&rawURL=" + URLEncoder.encode(failoverServer.split("\\?")[0] + "/data/getData.raw", StandardCharsets.UTF_8);
					StoragePlugin failoverPlugin = StoragePluginURLParser.parseStoragePlugin(pluginDefString, configService);
					List<Callable<EventStream>> failoverStrms = failoverPlugin.getDataForPV(context, pvName, start, end, postProcessor);
					if(failoverStrms != null && !failoverStrms.isEmpty()) {
						ArrayListEventStream cacheStream = null;
						int sampleCount = 0;
						for(Callable<EventStream> failoverStrm : failoverStrms) {
							try (EventStream ev = failoverStrm.call()){

								if(cacheStream == null) {
									cacheStream = new ArrayListEventStream(10000, (RemotableEventStreamDesc) ev.getDescription());
								}
								for(Event e : ev) {
									cacheStream.add(e);
									sampleCount++;
								}
								
							} catch (Exception ex) {
								logger.error("Exception during failover retrieval " + pvName, ex);
							}
						}
						if(sampleCount > 0) {
							logger.debug("Merging " + sampleCount + " samples from the failover server ");
							for(UnitOfRetrieval unitofretrieval : unitsofretrieval) {
								logger.debug("Wrapping UnitOfRetrieval with failover stream for PV " + pvName);
								unitofretrieval.wrapWithFailoverStreams(CallableEventStream.makeOneStreamCallableList(cacheStream));
							}
						} else {
							logger.warn("0 samples from failover stream for PV " + pvName);
						}
					} else {
						logger.warn("No streams from the failover server for PV " + pvName);				
					}
				}
			}
		}

		return unitsofretrieval;
	}
}
