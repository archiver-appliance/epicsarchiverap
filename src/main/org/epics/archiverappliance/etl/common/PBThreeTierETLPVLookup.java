/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl.common;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.etl.StorageMetrics;

/**
 * Holds runtime state for ETL.
 * For now, gets all of the info from PVTypeInfo.
 *
 * @author rdh
 * @version 4-Jun-2012, Luofeng Li:added codes to create one ETL thread for each ETL
 * @version 7-Aug-2012, Murali Shankar:Refactored the threading to take advantage of maximumNumberOfLifetimesInInstallation
 */

public final class PBThreeTierETLPVLookup {
	private static final String MAXIMUM_NUMBER_OF_LIFETIMES_IN_INSTALLATION = "org.epics.archiverappliance.config.PVTypeInfo.maximumNumberOfLifetimesInInstallation";
	private static Logger logger = Logger.getLogger(PBThreeTierETLPVLookup.class.getName());
	private static Logger configlogger = Logger.getLogger("config." + PBThreeTierETLPVLookup.class.getName());
	private static int DEFAULT_ETL_PERIOD = 60*5; // Seconds; needs to be the smallest time interval in the PartitionGranularity.
	private static int DEFAULT_ETL_INITIAL_DELAY = 60*1; // Seconds.

	private ConfigService configService = null;
	
	private int maxLifetimes = 3;

	/**
	 * Used to poll the config service in the background and add ETL jobs for PVs 
	 */
	private ScheduledThreadPoolExecutor configServiceSyncThread = null;

	/**
	 * PVs for whom we have already added etl jobs
	 */
	private ConcurrentSkipListSet<String> pvsForWhomWeHaveAddedETLJobs = new ConcurrentSkipListSet<String>();

	/**
	 * Metrics and state for each lifetimeid transition for a pv
	 * The first level index is the source lifetimeid
	 * The seconds level index is the pv name.
	 */
	private HashMap<Integer, ConcurrentHashMap<String, ETLPVLookupItems>> lifetimeId2PVName2LookupItem = new HashMap<Integer, ConcurrentHashMap<String, ETLPVLookupItems>>();
	
	/**
	 * We have a thread pool for each lifetime id transition.
	 * Adding a pv to ETL involves scheduling an ETLPVLookupItem into each of the appropriate lifetimeid transitions with a period appropriate to the source partition granularity
	 */
	private ScheduledThreadPoolExecutor[] etlLifeTimeThreadPoolExecutors = null;
	
	private ETLMetricsForLifetime[] applianceMetrics = null;
	
	public PBThreeTierETLPVLookup(ConfigService configService) {
		this.configService = configService;
		configServiceSyncThread = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread ret = new Thread(r, "Config service sync thread");
				return ret;
			}
		});
		
		maxLifetimes = Integer.parseInt(configService.getInstallationProperties().getProperty(MAXIMUM_NUMBER_OF_LIFETIMES_IN_INSTALLATION, "3"));
		if(maxLifetimes-1 > 0) { 
			logger.info("Creating a thread pool for each lifetimeid transition => " + (maxLifetimes-1));
			etlLifeTimeThreadPoolExecutors = new ScheduledThreadPoolExecutor[maxLifetimes-1];
			applianceMetrics = new ETLMetricsForLifetime[maxLifetimes-1];
			for(int lifetimeId = 0; lifetimeId < maxLifetimes-1; lifetimeId++) {
				etlLifeTimeThreadPoolExecutors[lifetimeId] = new ScheduledThreadPoolExecutor(1, new ETLLifeTimeThreadFactory(lifetimeId));
				lifetimeId2PVName2LookupItem.put(new Integer(lifetimeId), new ConcurrentHashMap<String, ETLPVLookupItems>());
				applianceMetrics[lifetimeId] = new ETLMetricsForLifetime(lifetimeId);
			}
		}
		
		configService.addShutdownHook(new ETLShutdownThread(this));
	}

	/**
	 * Initialize the ETL background scheduled executors and create the runtime state for various ETL components.
	 */
	public void postStartup() {
		configlogger.info("Beginning ETL post startup; scheduling the configServiceSyncThread to keep the local ETL lifetimeId2PVName2LookupItem in sync");
		configServiceSyncThread.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				try { 
					Iterable<String> pVsForThisAppliance = configService.getPVsForThisAppliance();
					if(pVsForThisAppliance != null) { 
						for(String pvName : pVsForThisAppliance) { 
							if(!pvsForWhomWeHaveAddedETLJobs.contains(pvName)) { 
								PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
								if(!typeInfo.isPaused()) { 
									addETLJobs(pvName, typeInfo);
								} else { 
									logger.info("Skipping adding ETL jobs for paused PV " + pvName);
								}
							}
						}
					} else { 
						configlogger.info("There are no PVs on this appliance yet");
					}
				} catch(Throwable t) { 
					configlogger.error("Excepting syncing ETL jobs with config service", t);
				}
			}
		}, DEFAULT_ETL_INITIAL_DELAY, DEFAULT_ETL_PERIOD, TimeUnit.SECONDS);
		configlogger.debug("Done initializing ETL post startup.");
	}
	
	/**
	 * Add jobs for each of the ETL lifetime transitions.
	 * @param pvName
	 * @param typeInfo
	 */
	public void addETLJobs(String pvName, PVTypeInfo typeInfo) {
		if(!pvsForWhomWeHaveAddedETLJobs.contains(pvName)) {
			// Precompute the chunkKey when we have access to the typeInfo. The keyConverter should store it in its cache.
			String chunkKey = configService.getPVNameToKeyConverter().convertPVNameToKey(pvName);
			logger.debug("Adding etl jobs for pv " + pvName + " for chunkkey " + chunkKey);
			String[] dataSources = typeInfo.getDataStores();
			if(dataSources == null ||  dataSources.length < 2) { 
				logger.warn("Skipping adding PV to ETL as it has less than 2 datasources" + pvName);
				return;
			}

			if(dataSources.length >= applianceMetrics.length) { 
				configlogger.fatal("The pv " + pvName + " has " + dataSources.length + " datasources. This installation is configured for a max of " + applianceMetrics.length + " datasources. To change this, please edit the archappl.properties for your installation and change the " + MAXIMUM_NUMBER_OF_LIFETIMES_IN_INSTALLATION + " to " + dataSources.length);
				return;
			}

			
			for(int etllifetimeid = 0; etllifetimeid < dataSources.length-1; etllifetimeid++) {
				try {
					
					String sourceStr=dataSources[etllifetimeid];
					ETLSource etlSource = StoragePluginURLParser.parseETLSource(sourceStr, configService);
					String destStr=dataSources[etllifetimeid+1];
					ETLDest etlDest = StoragePluginURLParser.parseETLDest(destStr, configService);
					ETLPVLookupItems etlpvLookupItems = new ETLPVLookupItems(pvName, typeInfo.getDBRType(), etlSource, etlDest, etllifetimeid, applianceMetrics[etllifetimeid], determineOutOfSpaceHandling(configService));
					if(etlDest instanceof StorageMetrics) { 
						// At least on some of the test machines, checking free space seems to take the longest time. In this, getting the fileStore seems to take the longest time. 
						// The plainPB plugin caches the fileStore; so we make a call once when adding to initialize this upfront.
						((StorageMetrics)etlDest).getUsableSpace(etlpvLookupItems.getMetricsForLifetime());
					}
					lifetimeId2PVName2LookupItem.get(etllifetimeid).put(pvName, etlpvLookupItems);
					// We schedule using the source granularity or a shift (8 hours) whichever is smaller.
					int delaybetweenETLJobs = Math.min(etlSource.getPartitionGranularity().getApproxSecondsPerChunk(), 8*60*60);
					long epochSeconds = TimeUtils.getCurrentEpochSeconds();
					// We then compute the start of the next partition.
					long nextPartitionFirstSec = TimeUtils.getNextPartitionFirstSecond(epochSeconds, etlSource.getPartitionGranularity());
					// Add a small buffer to this
					long nextExpectedETLRunInSecs = nextPartitionFirstSec + 5*60*(etllifetimeid+1);
					// We compute the initial delay so that the ETL jobs run at a predictable time. 
					long initialDelay = nextExpectedETLRunInSecs - epochSeconds;
					// We schedule a ETLPVLookupItems with the appropriate thread using an ETLJob
					ETLJob etlJob = new ETLJob(etlpvLookupItems);
					ScheduledFuture<?> cancellingFuture = etlLifeTimeThreadPoolExecutors[etllifetimeid].scheduleWithFixedDelay(etlJob, initialDelay, delaybetweenETLJobs, TimeUnit.SECONDS);
					etlpvLookupItems.setCancellingFuture(cancellingFuture);
					logger.debug("Scheduled ETL job for " + pvName + " and lifetime " + etllifetimeid + " with initial delay of " + initialDelay + " and between job delay of " + delaybetweenETLJobs);
				} catch(Throwable t) {
					logger.error("Exception get  for pv " + pvName, t);
				}
			}
			
			pvsForWhomWeHaveAddedETLJobs.add(pvName);
		} else { 
			logger.debug("Not adding ETL jobs for PV already in pvsForWhomWeHaveAddedETLJobs " + pvName);
		}
	}
	
	/**
	 * Cancel the ETL jobs for each of the ETL lifetime transitions and also remove from internal structures.
	 * @param pvName
	 */
	public void deleteETLJobs(String pvName){
		if(pvsForWhomWeHaveAddedETLJobs.contains(pvName)) { 
			logger.debug("deleting etl jobs for  pv " + pvName + " from the locally cached copy of pvs for this appliance");
			for(int etllifetimeid = 0; etllifetimeid < maxLifetimes-1; etllifetimeid++) {
				ETLPVLookupItems lookupItem = lifetimeId2PVName2LookupItem.get(etllifetimeid).get(pvName);
				if(lookupItem != null) { 
					lookupItem.getCancellingFuture().cancel(false);
					lifetimeId2PVName2LookupItem.get(etllifetimeid).remove(pvName);
					
					if(lookupItem.getETLSource().consolidateOnShutdown()) { 
						logger.debug("Need to consolidate data from etl source " + ((StoragePlugin) lookupItem.getETLSource()).getName() + " for pv " + pvName + " for storage " + ((StorageMetrics) lookupItem.getETLDest()).getName());
						Timestamp oneYearLaterTimeStamp=TimeUtils.convertFromEpochSeconds(TimeUtils.getCurrentEpochSeconds()+365*24*60*60, 0);
						new ETLJob(lookupItem, oneYearLaterTimeStamp).run();
					}
				} else { 
					logger.debug("Did not find lookup item for " + pvName + " for lifetime id " + etllifetimeid);
				}
			}

			pvsForWhomWeHaveAddedETLJobs.remove(pvName);
		} else { 
			logger.debug("Not deleting ETL jobs for PV missing from pvsForWhomWeHaveAddedETLJobs " + pvName);
		}
	}
	
	/**
	 * Get the internal state for all the ETL lifetime transitions for a pv
	 * @param pvName
	 * @return
	 */
	public LinkedList<ETLPVLookupItems> getLookupItemsForPV(String pvName) {
		LinkedList<ETLPVLookupItems> ret = new LinkedList<ETLPVLookupItems>();
		if(pvsForWhomWeHaveAddedETLJobs.contains(pvName)) { 
			for(int etllifetimeid = 0; etllifetimeid < maxLifetimes-1; etllifetimeid++) {
				ETLPVLookupItems lookupItem = lifetimeId2PVName2LookupItem.get(etllifetimeid).get(pvName);
				if(lookupItem != null) { 
					ret.add(lookupItem);
				} else { 
					logger.debug("Did not find lookup item for " + pvName + " for lifetime id " + etllifetimeid);
				}
			}
		} else { 
			logger.debug("Returning empty list for PV missing from pvsForWhomWeHaveAddedETLJobs " + pvName);
		}
		return ret;
	}
	


	/**
	 * Get the latest (last known) entry from the stores for this PV.
	 * @param pvName
	 * @return
	 * @throws IOException
	 */
	public Event getLatestEventFromDataStores(String pvName) throws IOException {
		LinkedList<ETLPVLookupItems> etlEntries = getLookupItemsForPV(pvName);
		try(BasicContext context = new BasicContext()) {
			for(ETLPVLookupItems etlEntry : etlEntries) {
				Event e = etlEntry.getETLDest().getLastKnownEvent(context, pvName);
				if(e != null) return e;
			}
		}
		return null;
	}

	private final class ETLShutdownThread implements Runnable {
		private PBThreeTierETLPVLookup theLookup = null;
		public ETLShutdownThread(PBThreeTierETLPVLookup theLookup) { 
			this.theLookup = theLookup;
		}
		
		@Override
		public void run() {
			logger.debug("Shutting down ETL threads.");
			theLookup.configServiceSyncThread.shutdown();
			for(int lifetimeId = 0; lifetimeId < theLookup.maxLifetimes-1; lifetimeId++) {
				logger.debug("Shutting down ETL lifetimeid transition thread " + lifetimeId);
				theLookup.etlLifeTimeThreadPoolExecutors[lifetimeId].shutdown();
				
				ConcurrentHashMap<String, ETLPVLookupItems> lifetimeItems = theLookup.lifetimeId2PVName2LookupItem.get(lifetimeId);
				for(String pvName : lifetimeItems.keySet()) { 
					try { 
						ETLPVLookupItems etlitem = lifetimeItems.get(pvName);
						if(etlitem.getETLSource().consolidateOnShutdown()) { 
							ETLDest etlDest=etlitem.getETLDest();
							StorageMetrics storageMetricsAPIDest = (StorageMetrics) etlDest;
							String identifyDest=storageMetricsAPIDest.getName();
							logger.debug("Need to consolidate data from etl source " + ((StoragePlugin) etlitem.getETLSource()).getName() + " for pv " + pvName + " for storage " + identifyDest);

							Timestamp oneYearLaterTimeStamp=TimeUtils.convertFromEpochSeconds(TimeUtils.getCurrentEpochSeconds()+365*24*60*60, 0);
							new ETLJob(etlitem, oneYearLaterTimeStamp).run();
						}
					}  catch (Throwable t) {
						logger.error( "Error when consolidating data on shutdown for pv " + pvName, t);
					}
				}
			}
		}
	}


	private final class ETLLifeTimeThreadFactory implements ThreadFactory {
		private int lifetimeid;
		ETLLifeTimeThreadFactory(int lifetimeid) {
			this.lifetimeid = lifetimeid;
		}
		@Override
		public Thread newThread(Runnable r) {
			Thread ret = new Thread(r, "ETL - " + lifetimeid);
			return ret;
		}
	}

	public ETLMetricsForLifetime[] getApplianceMetrics() {
		return applianceMetrics;
	}
	
	
	/**
	 * Some unit tests want to run the ETL jobs manually; so we shut down the threads.
	 * We should probably write a pausable thread pool executor
	 * Use with care.
	 */
	public void manualControlForUnitTests() { 
		logger.error("Shutting down ETL for unit tests...");
		for(ScheduledThreadPoolExecutor scheduledThreadPoolExecutor : this.etlLifeTimeThreadPoolExecutors) { 
			scheduledThreadPoolExecutor.shutdown();
		}
	}
	
	public static OutOfSpaceHandling determineOutOfSpaceHandling(ConfigService configService) { 
		String outOfSpaceHandler = configService.getInstallationProperties().getProperty("org.epics.archiverappliance.etl.common.OutOfSpaceHandling", OutOfSpaceHandling.DELETE_SRC_STREAMS_IF_FIRST_DEST_WHEN_OUT_OF_SPACE.toString());
		return OutOfSpaceHandling.valueOf(outOfSpaceHandler);
	}
}

