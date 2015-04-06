/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Somewhat static information about a PV like it's type info, graphic limits, event rates etc.
 * 
 * @author mshankar
 */
public class PVTypeInfo implements Serializable {
	private static final long serialVersionUID = 6298175991390616559L;
	private static Logger logger = Logger.getLogger(PVTypeInfo.class.getName());
	public static final int DEFAULT_BUFFER_INTERVAL = 10;

	/**
	 * The name of the PV
	 */
	private String pvName;
	/**
	 * The DBRType of the PV
	 */
	private ArchDBRTypes DBRType;
	/**
	 * Is this a scalar?
	 */
	private boolean isScalar;
	/**
	 * If waveform, how many elements
	 */
	private int elementCount;
	/**
	 * Which appliance owns this PV
	 */
	private String applianceIdentity;
	/**
	 * What's the chunk key that this PV was mapped to?
	 */
	private String chunkKey;
	
	/**
	 * IOC where this PV came from last time.
	 */
	private String hostName;
	

	// Info from the dbr_ctrl structures
	/**
	 * <PV Name>.LOLO; though we get it using the dbr_ctrl structures
	 */
	private double lowerAlarmLimit;
	/**
	 * <PV Name>.DRVL; though we get it using the dbr_ctrl structures
	 */
	private double lowerCtrlLimit;
	/**
	 * <PV Name>.LOPR; though we get it using the dbr_ctrl structures
	 */
	private double lowerDisplayLimit;
	/**
	 * <PV Name>.LOW; though we get it using the dbr_ctrl structures
	 */
	private double lowerWarningLimit;
	/**
	 * <PV Name>.HIHI; though we get it using the dbr_ctrl structures
	 */
	private double upperAlarmLimit;
	/**
	 * <PV Name>.DRVH though we get it using the dbr_ctrl structures
	 */
	private double upperCtrlLimit;
	/**
	 * <PV Name>.HOPR; though we get it using the dbr_ctrl structures
	 */
	private double upperDisplayLimit;
	/**
	 * <PV Name>.HIGH; though we get it using the dbr_ctrl structures
	 */
	private double upperWarningLimit;
	/**
	 * <PV Name>.PREC; though we get it using the dbr_ctrl structures
	 */
	private double precision;
	/**
	 * <PV Name>.EGU; though we get it using the dbr_ctrl structures
	 */
	private String units;
	
	
	// Info pertaining to the archiver...
	private boolean hasReducedDataSet;
	private float computedEventRate;
	private float computedStorageRate;
	private int computedBytesPerEvent;
	private float userSpecifiedEventRate;
	private Timestamp creationTime;
	private Timestamp modificationTime;
	private boolean paused = false;
	private SamplingMethod samplingMethod;
	private float samplingPeriod;
	private String policyName;
	private String[] dataStores = new String[0];
	private HashMap<String, String> extraFields = new HashMap<String, String>();
	private String controllingPV;
	private String[] archiveFields = new String[0];
	
	public PVTypeInfo() {
		
	}

	public PVTypeInfo(String pvName, ArchDBRTypes dBRType, boolean isScalar, int elementCount) {
		super();
		this.pvName = pvName;
		DBRType = dBRType;
		this.isScalar = isScalar;
		this.elementCount = elementCount;
	}
	
	public PVTypeInfo(String pvName, PVTypeInfo srcTypeInfo) {
		this.pvName = pvName;
		this.DBRType = srcTypeInfo.DBRType;
		this.isScalar = srcTypeInfo.isScalar;
		this.elementCount = srcTypeInfo.elementCount;

		this.applianceIdentity = srcTypeInfo.applianceIdentity;
		this.lowerAlarmLimit = srcTypeInfo.lowerAlarmLimit;
		this.lowerCtrlLimit = srcTypeInfo.lowerCtrlLimit;
		this.lowerDisplayLimit = srcTypeInfo.lowerDisplayLimit;
		this.lowerWarningLimit = srcTypeInfo.lowerWarningLimit;
		this.upperAlarmLimit = srcTypeInfo.upperAlarmLimit;
		this.upperCtrlLimit = srcTypeInfo.upperCtrlLimit;
		this.upperDisplayLimit = srcTypeInfo.upperDisplayLimit;
		this.upperWarningLimit = srcTypeInfo.upperWarningLimit;
		this.precision = srcTypeInfo.precision;
		this.units = srcTypeInfo.units;
		this.hasReducedDataSet = srcTypeInfo.hasReducedDataSet;
		this.computedEventRate = srcTypeInfo.computedEventRate;
		this.computedStorageRate = srcTypeInfo.computedStorageRate;
		this.computedBytesPerEvent = srcTypeInfo.computedBytesPerEvent;
		this.userSpecifiedEventRate = srcTypeInfo.userSpecifiedEventRate;
		this.paused = srcTypeInfo.paused;
		this.samplingMethod = srcTypeInfo.samplingMethod;
		this.samplingPeriod = srcTypeInfo.samplingPeriod;
		this.policyName = srcTypeInfo.policyName;
		this.controllingPV = srcTypeInfo.controllingPV;

		this.dataStores = Arrays.copyOf(srcTypeInfo.dataStores, srcTypeInfo.dataStores.length);
		this.extraFields = new HashMap<String, String>(srcTypeInfo.extraFields);
		this.archiveFields = Arrays.copyOf(srcTypeInfo.archiveFields, srcTypeInfo.archiveFields.length);

		this.creationTime = TimeUtils.now();
		this.modificationTime = creationTime;
	}

	public String getPvName() {
		return pvName;
	}
	public ArchDBRTypes getDBRType() {
		return DBRType;
	}
	public boolean isScalar() {
		return isScalar;
	}
	public int getElementCount() {
		return elementCount;
	}
	public Double getUpperDisplayLimit() {
		return upperDisplayLimit;
	}
	public Double getLowerDisplayLimit() {
		return lowerDisplayLimit;
	}
	public boolean isHasReducedDataSet() {
		return hasReducedDataSet;
	}
	public float getComputedEventRate() {
		return computedEventRate;
	}
	public float getUserSpecifiedEventRate() {
		return userSpecifiedEventRate;
	}

	public Timestamp getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(Timestamp creationTime) {
		this.creationTime = creationTime;
		this.modificationTime = creationTime;
	}

	public Timestamp getModificationTime() {
		return modificationTime;
	}

	public void setModificationTime(Timestamp modificationTime) {
		this.modificationTime = modificationTime;
	}

	public boolean isPaused() {
		return paused;
	}

	public void setPaused(boolean paused) {
		this.paused = paused;
	}

	public void setPvName(String pvName) {
		this.pvName = pvName;
	}

	public void setDBRType(ArchDBRTypes dBRType) {
		DBRType = dBRType;
	}

	public void setScalar(boolean isScalar) {
		this.isScalar = isScalar;
	}

	public void setElementCount(int elementCount) {
		this.elementCount = elementCount;
	}

	public void setUpperDisplayLimit(Double upperDisplayLimit) {
		this.upperDisplayLimit = upperDisplayLimit;
	}

	public void setLowerDisplayLimit(Double lowerDisplayLimit) {
		this.lowerDisplayLimit = lowerDisplayLimit;
	}

	public void setHasReducedDataSet(boolean hasReducedDataSet) {
		this.hasReducedDataSet = hasReducedDataSet;
	}

	public void setComputedEventRate(float computedEventRate) {
		this.computedEventRate = computedEventRate;
	}

	public void setUserSpecifiedEventRate(float userSpecifiedEventRate) {
		this.userSpecifiedEventRate = userSpecifiedEventRate;
	}

	public SamplingMethod getSamplingMethod() {
		return samplingMethod;
	}

	public void setSamplingMethod(SamplingMethod samplingMethod) {
		this.samplingMethod = samplingMethod;
	}

	public float getSamplingPeriod() {
		return samplingPeriod;
	}

	public void setSamplingPeriod(float samplingPeriod) {
		this.samplingPeriod = samplingPeriod;
	}

	public String[] getDataStores() {
		return dataStores;
	}

	public void setDataStores(String[] dataStores) {
		this.dataStores = dataStores;
	}

	public String getApplianceIdentity() {
		return applianceIdentity;
	}

	public void setApplianceIdentity(String applianceIdentity) {
		this.applianceIdentity = applianceIdentity;
	}
	
	/**
	 * Parse a string (JSON) representation of the PVTypeInfo object into this object
	 * @param typeInfoStr
	 * @throws Exception
	 */
	public void parsePolicyRepresentation(String typeInfoStr) {
		JSONObject parsedObj = (JSONObject) JSONValue.parse(typeInfoStr);
		this.samplingMethod = SamplingMethod.valueOf((String)parsedObj.get("samplingMethod"));
		this.samplingPeriod = Float.parseFloat((String)parsedObj.get("samplingPeriod"));
		this.policyName = (String) parsedObj.get("policyName");
		
		JSONArray parsedStores = (JSONArray) parsedObj.get("dataStores");
		LinkedList<String> parsedStoresList = new LinkedList<String>();
		for(Object parsedStore : parsedStores) { 
			parsedStoresList.add((String)parsedStore);
		}
		dataStores = parsedStoresList.toArray(new String[0]);
		
		if(logger.isDebugEnabled()) { 
			try {
				JSONEncoder<PVTypeInfo> jsonEncoder = JSONEncoder.getEncoder(PVTypeInfo.class);
				JSONObject rep = jsonEncoder.encode(this);
				logger.debug("Policy object initialized from string " + rep.toJSONString());
			} catch(Exception ex) {
				logger.error("Exception marshalling type info for pv " + pvName);
			}
		}
	}
	
	public Double getLowerAlarmLimit() {
		return lowerAlarmLimit;
	}

	public void setLowerAlarmLimit(Double lowerAlarmLimit) {
		this.lowerAlarmLimit = lowerAlarmLimit;
	}

	public Double getLowerCtrlLimit() {
		return lowerCtrlLimit;
	}

	public void setLowerCtrlLimit(Double lowerCtrlLimit) {
		this.lowerCtrlLimit = lowerCtrlLimit;
	}

	public Double getLowerWarningLimit() {
		return lowerWarningLimit;
	}

	public void setLowerWarningLimit(Double lowerWarningLimit) {
		this.lowerWarningLimit = lowerWarningLimit;
	}

	public Double getUpperAlarmLimit() {
		return upperAlarmLimit;
	}

	public void setUpperAlarmLimit(Double upperAlarmLimit) {
		this.upperAlarmLimit = upperAlarmLimit;
	}

	public Double getUpperCtrlLimit() {
		return upperCtrlLimit;
	}

	public void setUpperCtrlLimit(Double upperCtrlLimit) {
		this.upperCtrlLimit = upperCtrlLimit;
	}

	public Double getUpperWarningLimit() {
		return upperWarningLimit;
	}

	public void setUpperWarningLimit(Double upperWarningLimit) {
		this.upperWarningLimit = upperWarningLimit;
	}

	public Double getPrecision() {
		return precision;
	}

	public void setPrecision(Double precision) {
		this.precision = precision;
	}

	public String getUnits() {
		return units;
	}

	public void setUnits(String units) {
		this.units = units;
	}
	
	
	public void absorbMetaInfo(MetaInfo metaInfo) {
		this.lowerAlarmLimit = metaInfo.getLowerAlarmLimit();
		this.lowerCtrlLimit = metaInfo.getLoweCtrlLimit();
		this.lowerDisplayLimit = metaInfo.getLowerDisplayLimit();
		this.lowerWarningLimit = metaInfo.getLowerWarningLimit();
		this.upperAlarmLimit = metaInfo.getUpperAlarmLimit();
		this.upperCtrlLimit = metaInfo.getUpperCtrlLimit();
		this.upperDisplayLimit = metaInfo.getUpperDisplayLimit();
		this.upperWarningLimit = metaInfo.getUpperWarningLimit();
		this.precision = metaInfo.getPrecision();
		this.units = metaInfo.getUnit();
		this.elementCount = metaInfo.getCount();
		this.computedEventRate = (float) metaInfo.getEventRate();
		this.computedStorageRate = (float) metaInfo.getStorageRate();
		if(metaInfo.getEventCount() != 0) { 
			this.computedBytesPerEvent = (int) (metaInfo.getStorageSize()/metaInfo.getEventCount());
		}
		HashMap<String, String> otherMetaInfo = metaInfo.getOtherMetaInfo();
		for(String extraName : otherMetaInfo.keySet()) {
			extraFields.put(extraName, otherMetaInfo.get(extraName).toString());
		}
	}

	public HashMap<String, String> getExtraFields() {
		return extraFields;
	}

	public void setExtraFields(HashMap<String, String> extraFields) {
		this.extraFields = extraFields;
	}
	
	public boolean hasExtraField(String key) {
		if(this.extraFields == null) return false;
		return this.extraFields.containsKey(key);
	}
	
	public String lookupExtraField(String key) {
		if(this.extraFields == null) return null;
		return this.extraFields.get(key);
	}

	public String getPolicyName() {
		return policyName;
	}

	public void setPolicyName(String policyName) {
		this.policyName = policyName;
	}

	public float getComputedStorageRate() {
		return computedStorageRate;
	}

	public void setComputedStorageRate(float computedStorageRate) {
		this.computedStorageRate = computedStorageRate;
	}

	public int getComputedBytesPerEvent() {
		return computedBytesPerEvent;
	}

	public void setComputedBytesPerEvent(int computedBytesPerEvent) {
		this.computedBytesPerEvent = computedBytesPerEvent;
	}

	public String getControllingPV() {
		return controllingPV;
	}

	public void setControllingPV(String controllingPV) {
		this.controllingPV = controllingPV;
	}

	public String[] getArchiveFields() {
		return archiveFields;
	}
	
	public String obtainArchiveFieldsAsString() {
		StringBuilder buf = new StringBuilder();
		boolean first = true;
		for(String archiveField : archiveFields) {
			if(first) { first = false; } else { buf.append(','); }
			buf.append(archiveField);
		}
		return buf.toString();
	}


	public void setArchiveFields(String[] archiveFields) {
		if(archiveFields == null || archiveFields.length == 0) { 
			this.archiveFields = new String[0];
			return;
		}
		
		HashSet<String> newFields = new HashSet<String>();
		for(String fieldName : archiveFields) {
			if(fieldName == null || fieldName.equals("")) continue; 
			if(fieldName.equals("VAL")) continue;
			newFields.add(fieldName);
		}
		
		this.archiveFields = newFields.toArray(new String[0]);
	}
	
	
	public void addArchiveField(String fieldName) {
		if(fieldName == null || fieldName.equals("")) return; 
		if(fieldName.equals("VAL")) return;

		HashSet<String> newFields = new HashSet<String>();
		if(this.archiveFields != null) { 
			for(String fieldBeingArchived : this.archiveFields) {
				newFields.add(fieldBeingArchived);
			}
		}
		newFields.add(fieldName);
		this.archiveFields = newFields.toArray(new String[0]);
		
	}
	
	public boolean checkIfFieldAlreadySepcified(String fieldName) {
		if(fieldName == null || fieldName.equals("")) return false; 
		if(fieldName.equals("VAL")) return true;

		if(this.archiveFields != null) { 
			for(String fieldBeingArchived : this.archiveFields) {
				if(fieldBeingArchived.equals(fieldName)) {
					return true;
				}
			}
		}
		return false;
	}
	
	
	/**
	 * Loop thru the stores outlined in this typeinfo and determine the most recent event for this pv
	 * @return
	 */
	public Timestamp determineLastKnownEventFromStores(ConfigService configService) throws IOException {
		try(BasicContext context = new BasicContext()) {
			for(String storeUrl : this.dataStores) {
				try {
					StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(storeUrl, configService);
					Event e = storagePlugin.getLastKnownEvent(context, pvName);
					if(e != null) return e.getEventTimeStamp();
				} catch(IOException ex) {
					logger.error("Exception determining last known timestamp", ex);
				}
			}
		}
        return null;
	}
	
	
	
	/**
	 * The secondsToBuffer is a system wide property. 
	 * Use this method to get the proper defaults.
	 * @param configService
	 * @return
	 */
	public static int getSecondsToBuffer(ConfigService configService) {
		String secondsToBufferStr = configService.getInstallationProperties().getProperty("org.epics.archiverappliance.config.PVTypeInfo.secondsToBuffer", "10");
		int secondsToBuffer = Integer.parseInt(secondsToBufferStr);
		if(logger.isDebugEnabled()) logger.debug("Seconds to buffer is " + secondsToBuffer);
		return secondsToBuffer;
	}

	/**
	 * The archiver appliance stores data in chunks that have a well defined key. 
	 * We record this key in the typeinfo (to accommodate slowly changing key mapping strategies)
	 * @return
	 */
	public String getChunkKey() {
		return chunkKey;
	}

	public void setChunkKey(String chunkKey) {
		this.chunkKey = chunkKey;
	}
	
	public boolean keyAlreadyGenerated() { 
		return this.chunkKey != null;
	}

	/**
	 * @return the hostName
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * @param hostName the hostName to set
	 */
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
}
