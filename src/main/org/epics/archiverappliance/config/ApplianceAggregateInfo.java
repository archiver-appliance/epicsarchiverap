package org.epics.archiverappliance.config;

import java.util.HashMap;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.StorageMetrics;


/**
 * POJO facilitating various optimizations for BPL that uses appliance wide information.
 * The correct values for most of this is obtained by iterating thru the PVTypeInfo's and aggregating them.
 * To improve performance for business logic that takes the entire appliance into consideration, a few aggregated parameters are stored and maintained here.
 * There are no transactional guarantees with the information aggregated here; if the perfect/best information is desired, walk thru the PVTypeInfos.
 * This is available within the appliance for that appliance.
 * @author mshankar
 *
 */
public class ApplianceAggregateInfo {
	private double totalStorageRate;
	private double totalEventRate;
	private double totalPVCount;
	private HashMap<String, Long> totalStorageImpact= new HashMap<String, Long>();
	private static Logger logger = Logger.getLogger(DefaultConfigService.class.getName());
	
	public ApplianceAggregateInfo clone() {
		ApplianceAggregateInfo retval = new ApplianceAggregateInfo();
		retval.totalStorageRate = this.totalStorageRate;
		retval.totalEventRate = this.totalEventRate;
		retval.totalPVCount = this.totalPVCount;
		retval.totalStorageImpact = new HashMap<String, Long>();
		for(String key : this.totalStorageImpact.keySet()) {
			retval.totalStorageImpact.put(key, this.totalStorageImpact.get(key));
		}
		return retval;
	}
	
	public void addInfoForPV(String pvName, PVTypeInfo typeInfo, ConfigService configService) {
		synchronized(this) {
			totalStorageRate += typeInfo.getComputedStorageRate();
			totalEventRate += typeInfo.getComputedEventRate();
			totalPVCount++;
			if(typeInfo.getDataStores() != null && typeInfo.getDataStores().length > 0) {
				for(String dataStore : typeInfo.getDataStores()) {
					try {
						ETLDest etlDest = StoragePluginURLParser.parseETLDest(dataStore, configService);
						if(etlDest instanceof StorageMetrics) {
							StorageMetrics stMetrics = (StorageMetrics) etlDest;
							String identity = stMetrics.getName();
							double storageImpact = etlDest.getPartitionGranularity().getApproxSecondsPerChunk()*typeInfo.getComputedStorageRate();
							if(!totalStorageImpact.containsKey(identity)) {
								totalStorageImpact.put(identity, new Long(0));
							}

							long currentStorageImpact = totalStorageImpact.get(identity);
							currentStorageImpact += storageImpact;
							totalStorageImpact.put(identity, currentStorageImpact);
						}
					} catch(Exception ex) {
						logger.error("Exception parsing storage metrics url " + dataStore, ex);
					}
				}
			}
		}
	}
	
	/**
	 * Gets the aggregated total computedStorageRate for this appliance
	 * @return
	 */
	public double getTotalStorageRate() {
		return totalStorageRate;
	}

	/**
	 * Gets the aggregated total computedEventRate for this appliance
	 * @return
	 */
	public double getTotalEventRate() {
		return totalEventRate;
	}

	/**
	 * Gets the aggregated pv count for this appliance.
	 * @return
	 */
	public double getTotalPVCount() {
		return totalPVCount;
	}


	public void setTotalStorageRate(double totalStorageRate) {
		this.totalStorageRate = totalStorageRate;
	}

	public void setTotalEventRate(double totalEventRate) {
		this.totalEventRate = totalEventRate;
	}

	public void setTotalPVCount(double totalPVCount) {
		this.totalPVCount = totalPVCount;
	}

	/**
	 * The storage impact is the impact of a PV on the particular store. 
	 * It is the product of the estimated storage rate and the partition granularity of the source.
	 * This returns the aggregated impact on the various stores on this appliance indexed by the identity of the store.
	 * @return
	 */
	public HashMap<String, Long> getTotalStorageImpact() {
		return totalStorageImpact;
	}

	public void setTotalStorageImpact(HashMap<String, String> totalStorageImpact) {
		// The JSON Encoder and Decoder can only understand a HashMap<String, String> so we override the set method to take on HashMap<String, String>
		for(String id : totalStorageImpact.keySet()) {
			String impact = totalStorageImpact.get(id);
			try {
				long impactLong = Long.parseLong(impact);
				this.totalStorageImpact.put(id, impactLong);
			} catch(NumberFormatException ex) { 
				logger.error("Exception parsing number " + impact, ex);
			}
		}
	}
	
	
	/**
	 * This returns a new ApplianceAggregateInfo that is the difference between this info and the other info.
	 * Used to maintain the delta of ApplianceAggregateInfo's between the periodic fetches of the CapacityPlanningMetricsPerApplianceForPV
	 * @param other
	 * @return
	 */
	public ApplianceAggregateInfo getDifference(ApplianceAggregateInfo other) {
		ApplianceAggregateInfo retval = new ApplianceAggregateInfo();
		retval.totalPVCount = this.totalPVCount - other.totalPVCount;
		retval.totalEventRate = this.totalEventRate - other.totalEventRate;
		retval.totalStorageRate = this.totalStorageRate - other.totalStorageRate;
		if(logger.isDebugEnabled()) logger.debug("Returning " + retval.totalStorageRate + " this.totalStorageRate " + this.totalStorageRate + " other.totalStorageRate " + other.totalStorageRate);
		for(String key : totalStorageImpact.keySet()) {
			long si = totalStorageImpact.get(key);
			if(other.totalStorageImpact.containsKey(key)) {
				si = si - other.totalStorageImpact.get(key);
			}
			retval.totalStorageImpact.put(key, new Long(si));
		}
		return retval;
	}
}
