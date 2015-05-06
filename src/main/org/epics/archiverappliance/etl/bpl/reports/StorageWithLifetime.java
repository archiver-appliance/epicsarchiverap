package org.epics.archiverappliance.etl.bpl.reports;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.etl.StorageMetrics;
import org.epics.archiverappliance.etl.common.ETLMetricsForLifetime;
import org.epics.archiverappliance.etl.common.ETLPVLookupItems;
import org.json.simple.JSONValue;

public class StorageWithLifetime {
	private static Logger logger = Logger.getLogger(StorageWithLifetime.class.getName());
	StorageMetrics storageMetricsAPI;
	int lifetimeid;
	double totalETLTimeIntoThisDestInMillis;
	int maxTotalETLRunsIntoThisDest;
	int minPartitionSourceGranularityInSecs;
	public StorageWithLifetime(StorageMetrics storageMetricsAPI, int lifetimeid) {
		this.storageMetricsAPI = storageMetricsAPI;
		this.lifetimeid = lifetimeid;
	}
	
	public static String getStorageMetrics(ConfigService configService) {
		LinkedList<Map<String, String>> allStorageMetrics = new LinkedList<Map<String, String>>();

		LinkedList<StorageWithLifetime> finalStorages = getStorageWithLifetimes(configService);

		for(StorageWithLifetime storage : finalStorages) {
			try {
				ETLMetricsForLifetime metricsForLifetime = configService.getETLLookup().getApplianceMetrics()[storage.lifetimeid];
				HashMap<String, String> storageMetrics = new HashMap<String, String>();
				allStorageMetrics.add(storageMetrics);

				storageMetrics.put("identity", storage.storageMetricsAPI.getName());
				storageMetrics.put("totalSpace", Long.toString(storage.storageMetricsAPI.getTotalSpace(metricsForLifetime)));
				storageMetrics.put("availableSpace", Long.toString(storage.storageMetricsAPI.getUsableSpace(metricsForLifetime)));
				double avgTimeIntoThisDestInMillis = storage.totalETLTimeIntoThisDestInMillis/storage.maxTotalETLRunsIntoThisDest;
				storageMetrics.put("avgTimeConsumedMs", Double.toString(avgTimeIntoThisDestInMillis));
				storageMetrics.put("avgTimeConsumedPercent", Double.toString((avgTimeIntoThisDestInMillis/1000)/storage.minPartitionSourceGranularityInSecs));
			} catch(IOException ex) {
				logger.warn("Exception retrieving details from " + storage.storageMetricsAPI.getName(), ex);
			}
		}
		return JSONValue.toJSONString(allStorageMetrics);
	}


	public static String getStorageDetails(ConfigService configService) {
		DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
		LinkedList<HashMap<String, String>> details = new LinkedList<HashMap<String, String>>();

		LinkedList<StorageWithLifetime> finalStorages = getStorageWithLifetimes(configService);

		for(StorageWithLifetime storage : finalStorages) {
			ETLMetricsForLifetime metricsForLifetime = configService.getETLLookup().getApplianceMetrics()[storage.lifetimeid];
			HashMap<String, String> detail = new HashMap<String, String>();
			details.add(detail);
			try {
				detail.put("name", storage.storageMetricsAPI.getName());
				double totalSpaceGB = storage.storageMetricsAPI.getTotalSpace(metricsForLifetime)*1.0/(1024*1024*1024);
				detail.put("total_space", twoSignificantDigits.format(totalSpaceGB));
				double availbleSpaceGB = storage.storageMetricsAPI.getUsableSpace(metricsForLifetime)*1.0/(1024*1024*1024);
				detail.put("available_space", twoSignificantDigits.format(availbleSpaceGB));
				detail.put("available_space_percent", twoSignificantDigits.format(availbleSpaceGB*100/totalSpaceGB));
				double avgTimeIntoThisDestInMillis = storage.totalETLTimeIntoThisDestInMillis/storage.maxTotalETLRunsIntoThisDest;
				detail.put("time_copy_data_into_store", twoSignificantDigits.format(avgTimeIntoThisDestInMillis/1000));
				// Copy time as percent of source granularities
				detail.put("time_copy_data_into_store_percent", twoSignificantDigits.format((avgTimeIntoThisDestInMillis/1000)/storage.minPartitionSourceGranularityInSecs));

			} catch(IOException ex) {
				logger.warn("Exception retrieving details from " + storage.storageMetricsAPI.getName(), ex);
			}
		}
		return JSONValue.toJSONString(details);
	}

	/**
	 * Utility method to get all the ETL lookup items as storagemetrics instances if they support it.
	 * @return
	 */
	private static LinkedList<StorageWithLifetime> getStorageWithLifetimes(ConfigService configService) {
		LinkedHashMap<String, StorageWithLifetime> storages = new LinkedHashMap<String, StorageWithLifetime>();
		for(String pvName : configService.getPVsForThisAppliance()) { 
			for(ETLPVLookupItems lookupItem : configService.getETLLookup().getLookupItemsForPV(pvName)) {
				ETLSource etlSrc = lookupItem.getETLSource();
				if(etlSrc instanceof StorageMetrics) {
					StorageMetrics storageMetricsAPI = (StorageMetrics) etlSrc;
					if(!storages.containsKey(storageMetricsAPI.getName())) {
						storages.put(storageMetricsAPI.getName(), new StorageWithLifetime(storageMetricsAPI, lookupItem.getLifetimeorder()));
					}
				}
				ETLDest etlDest = lookupItem.getETLDest();
				if(etlDest instanceof StorageMetrics) {
					StorageMetrics storageMetricsAPI = (StorageMetrics) etlDest;
					if(!storages.containsKey(storageMetricsAPI.getName())) {
						storages.put(storageMetricsAPI.getName(), new StorageWithLifetime(storageMetricsAPI, lookupItem.getLifetimeorder()));
					}

					storages.get(storageMetricsAPI.getName()).addETLDestTimes(lookupItem);
				}
			}
		}

		LinkedList<StorageWithLifetime> finalStorages = new LinkedList<StorageWithLifetime>(storages.values());
		Collections.sort(finalStorages, new Comparator<StorageWithLifetime>() {
			@Override
			public int compare(StorageWithLifetime o1, StorageWithLifetime o2) {
				return o1.lifetimeid - o2.lifetimeid;
			}
		});
		return finalStorages;
	}
	
	private void addETLDestTimes(ETLPVLookupItems lookupItem) {
		if(lookupItem.getNumberofTimesWeETLed() > 0) {
			this.totalETLTimeIntoThisDestInMillis += lookupItem.getTotalTimeWeSpentInETLInMilliSeconds();
			this.maxTotalETLRunsIntoThisDest = Math.max(this.maxTotalETLRunsIntoThisDest, lookupItem.getNumberofTimesWeETLed());
			// We compute the percent as the percent of the source granularity.
			// For example, if the source granularity is an hour, then we have an hour to get the data into the dest.
			// What fraction of this did we consume?
			int typicalSecsInSrcPG = lookupItem.getETLSource().getPartitionGranularity().getApproxSecondsPerChunk();
			this.minPartitionSourceGranularityInSecs = Math.min(this.minPartitionSourceGranularityInSecs, typicalSecsInSrcPG);
		} else {
			if(logger.isDebugEnabled()) logger.debug("We do not seem to have ETLed for pv " + lookupItem.getPvName());
		}
	}


	public static class StorageConsumedByPV {
		public String pvName;
		public long storageConsumed;
		public StorageConsumedByPV(String pvName, long storageConsumed) {
			this.pvName = pvName;
			this.storageConsumed = storageConsumed;
		}

	}

	/**
	 * Get a list of PVs and the storage they consume on all the devices sorted by desc storage consumed...
	 * @param limit
	 * @return
	 */
	public static LinkedList<StorageConsumedByPV> getPVSByStorageConsumed(ConfigService configService) throws IOException {
		//TODO there may be some problems . When visiting the web page of reports and look up the "PVs by storage consumed(100)" , it takes a long time
		HashMap<String, HashMap<String, StorageMetrics>> storesForAllPVs = getStoresForAllPVs(configService);
		LinkedList<StorageConsumedByPV> storageConsumedList = new LinkedList<StorageConsumedByPV>();
		for(String pvName: storesForAllPVs.keySet()) {
			long spaceConsumedByPV = 0;
			HashMap<String, StorageMetrics> pvStores = storesForAllPVs.get(pvName);
			for(StorageMetrics storageMetrics : pvStores.values()) {
				long spaceConsumedByPVInThisStore = storageMetrics.spaceConsumedByPV(pvName);
				spaceConsumedByPV = spaceConsumedByPV + spaceConsumedByPVInThisStore;
			}
			storageConsumedList.add(new StorageConsumedByPV(pvName, spaceConsumedByPV));
		}

		Collections.sort(storageConsumedList, new Comparator<StorageConsumedByPV>() {
			@Override
			public int compare(StorageConsumedByPV o1, StorageConsumedByPV o2) {
				if(o1.storageConsumed == o2.storageConsumed) return 0;
				return (o1.storageConsumed < o2.storageConsumed) ? 1 : -1;
			}
		});

		return storageConsumedList;
	}

	/**
	 * Get the stores for all PV's indexed by PV name..
	 * @return
	 */
	private static HashMap<String, HashMap<String, StorageMetrics>> getStoresForAllPVs(ConfigService configService) {
		HashMap<String, HashMap<String, StorageMetrics>> storesForAllPVs = new HashMap<String, HashMap<String, StorageMetrics>>();

		for(String pvName : configService.getPVsForThisAppliance()) { 
			for(ETLPVLookupItems lookupItem : configService.getETLLookup().getLookupItemsForPV(pvName)) {
				HashMap<String, StorageMetrics> pvStores = storesForAllPVs.get(pvName);
				if(pvStores == null) {
					pvStores = new HashMap<String, StorageMetrics>();
					storesForAllPVs.put(pvName, pvStores);
				}

				ETLSource etlSrc = lookupItem.getETLSource();
				if(etlSrc instanceof StorageMetrics) {
					StorageMetrics storageMetricsAPI = (StorageMetrics) etlSrc;
					if(!pvStores.containsKey(storageMetricsAPI.getName())) {
						pvStores.put(storageMetricsAPI.getName(), storageMetricsAPI);
					}
				}

				ETLDest etlDest = lookupItem.getETLDest();
				if(etlDest instanceof StorageMetrics) {
					StorageMetrics storageMetricsAPI = (StorageMetrics) etlDest;
					if(!pvStores.containsKey(storageMetricsAPI.getName())) {
						pvStores.put(storageMetricsAPI.getName(), storageMetricsAPI);
					}
				}
			}
		}

		return storesForAllPVs;
	}
}
