package org.epics.archiverappliance.etl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.common.ETLJob;
import org.epics.archiverappliance.etl.common.ETLMetricsForLifetime;
import org.epics.archiverappliance.etl.common.ETLPVLookupItems;
import org.epics.archiverappliance.etl.common.PBThreeTierETLPVLookup;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Run ETLs for one PV; mostly for unit tests..
 * @author luofeng
 *
 */
public class ETLExecutor {
	private static Logger logger = LogManager.getLogger(ETLExecutor.class.getName());
	
	/**
	 * This should only be called from within unit tests...
	 * @param configService  ConfigService 
     * @param timeETLruns Instant
	 * @throws IOException  &emsp;
	 */
    public static void runETLs(ConfigService configService, Instant timeETLruns) throws IOException {
		for(String pvName : configService.getPVsForThisAppliance()) {
			logger.debug("Running ETL for " + pvName);
			LinkedList<ETLPVLookupItems> lookupItems = configService.getETLLookup().getLookupItemsForPV(pvName);
			for(ETLPVLookupItems lookupItem : lookupItems) { 
				logger.debug("Running ETL for " + pvName + " for lifetime " + lookupItem.getLifetimeorder() + " from " + lookupItem.getETLSource().getDescription() + " to " + lookupItem.getETLDest().getDescription());
				ETLJob job = new ETLJob(lookupItem, timeETLruns);
				job.run();
				if(job.getExceptionFromLastRun() != null) throw new IOException(job.getExceptionFromLastRun());
			}
		}
	}
	
	
	/**
	 * Run ETL for one PV until one storage; used in consolidate...
	 * Make sure that the regular ETL has been paused..
	 * @param configService ConfigService  
     * @param timeETLruns Instant
	 * @param pvName The name of PV.
	 * @param storageName   &emsp;
	 * @throws IOException  &emsp;
	 */
    public static void runPvETLsBeforeOneStorage(final ConfigService configService, final Instant timeETLruns, final String pvName, final String storageName) throws IOException {
		PBThreeTierETLPVLookup etlLookup = configService.getETLLookup();
		LinkedList<ETLPVLookupItems> lookupItems = etlLookup.getLookupItemsForPV(pvName);
		if(lookupItems != null && !lookupItems.isEmpty()) { 
			throw new IOException("The pv " + pvName + " has entries in PBThreeTierETLPVLookup. Please remove these first");
		}
		PVTypeInfo pvTypeInfo = configService.getTypeInfoForPV(pvName);
		String[] dataStores = pvTypeInfo.getDataStores();
		if(dataStores == null || dataStores.length < 2) { 
			throw new IOException("The pv " + pvName + " has not enough stores.");
		}
		
		lookupItems = new LinkedList<ETLPVLookupItems>();
		for(int i=1;i<dataStores.length;i++){
			String destStr=dataStores[i];
			ETLDest etlDest = StoragePluginURLParser.parseETLDest(destStr, configService);
			StorageMetrics storageMetricsAPIDest = (StorageMetrics)etlDest;
			String identifyDest=storageMetricsAPIDest.getName();
			logger.info("storage name:"+identifyDest);
			String sourceStr=dataStores[i-1];
			ETLSource etlSource = StoragePluginURLParser.parseETLSource(sourceStr, configService);
			lookupItems.add(new ETLPVLookupItems(pvName, pvTypeInfo.getDBRType(), etlSource, etlDest, i-1, new ETLMetricsForLifetime(i-1), PBThreeTierETLPVLookup.determineOutOfSpaceHandling(configService)));
			if(storageName.equals(identifyDest)){
				break;
			}
		}
		
		ScheduledThreadPoolExecutor threadPool = new ScheduledThreadPoolExecutor(1);
		for(ETLPVLookupItems lookupItem : lookupItems) { 
			threadPool.execute(new ETLJob(lookupItem, timeETLruns));
		}
		threadPool.shutdown();
		try { threadPool.awaitTermination(10, TimeUnit.MINUTES); } catch(InterruptedException ex) {}
	}	
}
