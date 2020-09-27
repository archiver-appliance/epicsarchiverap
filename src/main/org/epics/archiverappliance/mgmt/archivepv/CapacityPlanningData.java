package org.epics.archiverappliance.mgmt.archivepv;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceAggregateInfo;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Data from various appliances that is used for capacity planning.
 * We assume that policies have been applied for this PV and that typeinfo is available.
 * We gather data from various appliances (engine + ETL + storage) as it applies to this PV.
 * Capacity planning consists of measured data and estimated data.
 * The estimated data is available from the configService as ApplianceAggregateInfo
 * The measured data is available thru this class.
 * As the measured data does not change very often, we cache this locally for a time period as defined by MEASURED_DATA_CACHE_TIME
 * @author mshankar
 *
 */
public class CapacityPlanningData {
	private static final int MEASURED_DATA_CACHE_TIME = 60*60*1000;
	private static Logger logger = Logger.getLogger(CapacityPlanningData.class.getName());
	/**
	 * This is the percentage of time taken (averaged over the lifetime of the engine) that the engine write thread takes to flush data into short term store.
	 */
	private float engineWriteThreadUsage; 
	private float secondsConsumedByWriter;
	private float currentTotalStorageRate;
	private float  percentageTimeForWriterAfterPVadded=0;
	/**
	 * ETL metrics for all the stores in this appliance that support the storage API.
	 */
	private ConcurrentHashMap<String, ETLMetrics> etlMetrics = new ConcurrentHashMap<String, ETLMetrics>();
	
	private ApplianceAggregateInfo applianceAggregateInfoAsOfLastFetch;
	private String identity;
	private boolean isAvailable=true;
	private static CPStaticData cachedCPStaticData = null;
	
	
	
	class ETLMetrics {
		String identity;
		/**
		 * This is percentage time taken (averaged over the lifetime of ETL) that the ETL thread takes to move data from one store to another.
		 */
		double etlTimeTaken;
		/**
		   this is the estimated storage size for pv just added.
		   estimateStoragePVadded=sum(pvDataRate*partitionTime);
		 */
		long estimateStoragePVadded=0;
		
		double estimateETLtimePercentageAfterPVadded;
		
		
		/**
		 * This is the raw storage (in bytes, averaged over the lifetime of ETL) available before ETL runs in all the various stores.
		 */
		long etlStorageAvailable;
		
		/**
		 * This is the total size of the store.
		 */
		long totalSpace;
		
		public ETLMetrics(JSONObject obj) {
			this.identity = (String) obj.get("identity");
			this.totalSpace = Long.parseLong((String) obj.get("totalSpace"));
			this.etlStorageAvailable = Long.parseLong((String) obj.get("availableSpace"));
			this.etlTimeTaken = Double.parseDouble((String) obj.get("avgTimeConsumedPercent"));
			//avgTimeConsumedMs
			
		}
	}
	
	
	
	public CapacityPlanningData(ConfigService configService, ApplianceInfo applianceInfo) throws IOException {
		try{
			identity = applianceInfo.getIdentity();
			String engineURL = applianceInfo.getEngineURL() + "/getApplianceMetrics";
			JSONObject engineMetrics = GetUrlContent.getURLContentAsJSONObject(engineURL);
			DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
			String secondsConsumedByWriterStr = (String) engineMetrics.get("secondsConsumedByWriter");
			secondsConsumedByWriter=twoSignificantDigits.parse(secondsConsumedByWriterStr).floatValue();
			String currentTotalStorageRateStr = (String) engineMetrics.get("dataRate");
			if(currentTotalStorageRateStr != null) {
				currentTotalStorageRate=twoSignificantDigits.parse(currentTotalStorageRateStr).floatValue();
			}
			String etlURL = applianceInfo.getEtlURL() + "/getStorageMetricsForAppliance";
			JSONArray etlMetricsArray = GetUrlContent.getURLContentAsJSONArray(etlURL);
			logger.debug(applianceInfo.getIdentity()+"'s ETLMetric:"+etlMetricsArray.toJSONString());
			for(Object etlMetricObj : etlMetricsArray) {
				ETLMetrics etlMetric = new ETLMetrics((JSONObject) etlMetricObj);
				etlMetrics.put(etlMetric.identity, etlMetric);
			}
			
			applianceAggregateInfoAsOfLastFetch = configService.getAggregatedApplianceInfo(applianceInfo).clone();
		} catch(Exception e) {
			logger.error("Exception in CapacityPlanningMetricsPerApplianceForPV", e);
			throw new IOException(e);
		}
	}
	
	public static class CPStaticData {
		public ConcurrentHashMap<ApplianceInfo, CapacityPlanningData> cpApplianceMetrics;
		Timestamp timeofData;
		public CPStaticData(ConcurrentHashMap<ApplianceInfo, CapacityPlanningData> cpApplianceMetrics, Timestamp timeofData) {
			this.cpApplianceMetrics = cpApplianceMetrics;
			this.timeofData = timeofData;
		}
	}
	
	public static CPStaticData getMetricsForAppliances(ConfigService configService) throws IOException {
		Timestamp now = TimeUtils.now();
		if(cachedCPStaticData != null) {
			if((now.getTime() - cachedCPStaticData.timeofData.getTime()) > MEASURED_DATA_CACHE_TIME) {
				logger.debug("Refetching static data for capacity planning as it is stale " + (now.getTime() - cachedCPStaticData.timeofData.getTime()));
			} else {
				logger.debug("Using cached copy of measured data");
				return cachedCPStaticData;
			}
		}
		
		logger.debug("Fetching new capacity planning static data");
		ConcurrentHashMap<ApplianceInfo, CapacityPlanningData> capacityMetrics = new ConcurrentHashMap<ApplianceInfo, CapacityPlanningData>(); 
		for(ApplianceInfo applianceInfo : configService.getAppliancesInCluster()) {
			capacityMetrics.put(applianceInfo, new CapacityPlanningData(configService, applianceInfo));
		}
		
		CPStaticData newStaticData = new CPStaticData(capacityMetrics, now);
		cachedCPStaticData = newStaticData;
		return cachedCPStaticData;
	}


	public float getEngineWriteThreadUsage(float writePeriod) {
		engineWriteThreadUsage = (float) (secondsConsumedByWriter*100/writePeriod);
		logger.debug("engineWriteThreadUsage for appliance " + identity + " is " + engineWriteThreadUsage);
		return engineWriteThreadUsage;
	}


	public ConcurrentHashMap<String, ETLMetrics> getEtlMetrics() {
		return etlMetrics;
	}


	public float getCurrentTotalStorageRate() {
		return currentTotalStorageRate;
	}
	
	/**
	 * Return the difference between the appliance aggregate info as of "now" and from the time we last fetched the static data.
	 * @param configService ConfigService
	 * @return ApplianceAggregateInfo  &emsp;
	 * @throws IOException  &emsp;
	 */
	public ApplianceAggregateInfo getApplianceAggregateDifferenceFromLastFetch(ConfigService configService) throws IOException {
		ApplianceAggregateInfo freshData = configService.getAggregatedApplianceInfo(configService.getAppliance(identity));
		return freshData.getDifference(applianceAggregateInfoAsOfLastFetch);
	}

	public float getPercentageTimeForWriter() {
		return percentageTimeForWriterAfterPVadded;
	}


	public void setPercentageTimeForWriter(float percentageTimeForWriterAfterPVadded) {
		this.percentageTimeForWriterAfterPVadded = percentageTimeForWriterAfterPVadded;
	}


	public float getSecondsConsumedByWriter() {
		return secondsConsumedByWriter;
	}


	public boolean isAvailable() {
		return isAvailable;
	}


	public void setAvailable(boolean isAvailable) {
		this.isAvailable = isAvailable;
	}
	
	
	public String getStaticDataLastUpdated() { 
		if(cachedCPStaticData != null) { 
			return TimeUtils.convertToHumanReadableString(cachedCPStaticData.timeofData);
		} else { 
			return "Unknown";
		}
	}
}
