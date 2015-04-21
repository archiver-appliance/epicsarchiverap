/*******************************************************************************

 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University

 * as Operator of the SLAC National Accelerator Laboratory.

 * Copyright (c) 2011 Brookhaven National Laboratory.

 * EPICS archiver appliance is distributed subject to a Software License Agreement found

 * in file LICENSE that is included with this distribution.

 *******************************************************************************/

package org.epics.archiverappliance.config;

import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBR_CTRL_Double;
import gov.aps.jca.dbr.DBR_CTRL_Int;
import gov.aps.jca.dbr.DBR_LABELS_Enum;
import gov.aps.jca.dbr.GR;
import gov.aps.jca.dbr.PRECISION;

import java.util.HashMap;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.pvdata.pv.PVStructure;
/** this class is used for getting the meta data
 *  <p>
 *  @author luofeng li
 */

public class MetaInfo {

	private static Logger logger = Logger.getLogger(MetaInfo.class.getName());
	private static Logger configLogger = Logger.getLogger("config." + MetaInfo.class.getName());

	/**
	 * the name of ioc where this pv is
	 */
	private String hostName;

	/**
	 *  lowerAlarmLimit is responding to the LOLO field of ioc
	 */
	private double lowerAlarmLimit;

	/**
	 *  the lower control limit
	 */
	private double loweCtrlLimit;

	/**
	 *  lowerDisplayLimit is responding to the LOPR field of ioc
	 */
	private double lowerDisplayLimit;

	/**
	 *  lowerWarningLimit is responding to the LOW field of ioc
	 */

	private double lowerWarningLimit;

	/**
	 *  upperAlarmLimit is responding to the HIHI field of ioc
	 */
	private double upperAlarmLimit;

	/**
	 *  the upper control limit
	 */
	private double upperCtrlLimit;

	/**
	 *  upperDisplayLimit is responding to the HOPR field of ioc
	 */
	private double upperDisplayLimit;

	/**
	 *  upperWarningLimit is responding to the HIGH field of ioc
	 */
	private double upperWarningLimit;

	/**
	 *  the precision of the pv
	 */
	private int precision;

	/**
	 * the unit of the pv 
	 */
	private String unit ;

	/**
	 * The events count of the pv in one minute
	 */
	private long eventCount;

	/**
	 *  the alias name of pv, this is corresponding to the NAME field IOC
	 */
	private String aliasName="";

	/**
	 * the type of the pv
	 */
	private ArchDBRTypes archDBRTypes;

	/**
	 *  the event rate and the unit is events per second
	 */
	private double eventRate ;

	/**
	 *  the total storage size of pv data in one minute and the unit is bytes.
	 */
	private long storageSize;

	/**
	 * the storage rate of pv data and the unit is bytes per second
	 */
	private double storageRate;

	/**
	 *  the element count of the pv's value.
	 */
	private int count;

	/**
	 *  the pv is vector or not. when cout=nt >1 ,isVector=true. otherelse isVector=false;
	 */
	private boolean isVector=false;

	/**
	 *  if the pv is a label (dbr.isLABELS()),the label of the pv is stored in label[].
	 */
	private String label[];

	/**
	 * time from the first data received(monitor change function called) to the last data received
	 */
	private double second=0;

	/**
	 *  the start time of monitorchange first called  and startTime=System.currentTimeMillis();
	 */
	private long startTime=0L;

	/**
	 * store other information include MDEL,ADEL   &
	 */
	private HashMap<String,String> otherMetaInfo=new  HashMap<String,String>();



	/**
	 * add other meta info such as MDEL,ADEL
	 * @param name the info name
	 * @param value  the info value
	 */

	public void addOtherMetaInfo(String name,Double value) {
		if(value == null) { 
			logger.warn("Skipping adding meta info for " + name);
			return;
		}

		otherMetaInfo.put(name, value.toString());
	}

	/**
	 * 
	 * @return the host name where this pv is 
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * 
	 * @param hostName  the host name where this pv is 
	 */
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	/**
	 * add other meta info such as MDEL,ADEL
	 * @param name the info name
	 * @param value  the info value
	 */

	public void addOtherMetaInfo(String name,String value) {
		if(value == null) { 
			logger.warn("Skipping adding meta info for " + name);
			return;
		}

		otherMetaInfo.put(name, value.toString());
	}

	/**
	 * set the starting time of archiving this pv
	 * @param startTime the number of milliseconds since 1970/01/01
	 */
	public void setStartTime(long startTime) {
		this.startTime=startTime;
	}

	/**
	 * 
	 * @return the HashMap including all other info for this pv.
	 */
	public HashMap<String,String> getOtherMetaInfo() {
		return otherMetaInfo;
	}

	/**
	 * save the basical info from dbr
	 * @param dbr 
	 */

	public void applyBasicInfo(String pvName, final DBR dbr, ConfigService configService) {
		if (dbr.isLABELS()) {
			final DBR_LABELS_Enum labels = (DBR_LABELS_Enum)dbr;
			label=labels.getLabels();
		} else if (dbr instanceof DBR_CTRL_Double) {
			final DBR_CTRL_Double ctrl = (DBR_CTRL_Double)dbr;
			this.lowerDisplayLimit= ctrl.getLowerDispLimit().doubleValue();
			this.upperDisplayLimit=ctrl.getUpperDispLimit().doubleValue();
			this.lowerWarningLimit=ctrl.getLowerWarningLimit().doubleValue();
			this.upperWarningLimit=ctrl.getUpperWarningLimit().doubleValue();
			this.lowerAlarmLimit=ctrl.getLowerAlarmLimit().doubleValue();
			this.loweCtrlLimit=ctrl.getLowerCtrlLimit().doubleValue();
			this.upperCtrlLimit=ctrl.getUpperCtrlLimit().doubleValue();
			this.upperAlarmLimit=ctrl.getUpperAlarmLimit().doubleValue();
			this.precision=ctrl.getPrecision();
			this.unit=ctrl.getUnits();
			updateTypeInfo(pvName, configService);
		} else if (dbr instanceof DBR_CTRL_Int) {
			final DBR_CTRL_Int ctrl = (DBR_CTRL_Int)dbr;
			this.lowerDisplayLimit= ctrl.getLowerDispLimit().doubleValue();
			this.upperDisplayLimit=ctrl.getUpperDispLimit().doubleValue();
			this.lowerWarningLimit=ctrl.getLowerWarningLimit().doubleValue();
			this.upperWarningLimit=ctrl.getUpperWarningLimit().doubleValue();
			this.loweCtrlLimit=ctrl.getLowerCtrlLimit().doubleValue();
			this.upperCtrlLimit=ctrl.getUpperCtrlLimit().doubleValue();
			this.lowerAlarmLimit=ctrl.getLowerAlarmLimit().doubleValue();
			this.upperAlarmLimit=ctrl.getUpperAlarmLimit().doubleValue();
			this.precision=0;
			this.unit=ctrl.getUnits();
			updateTypeInfo(pvName, configService);
		} else if (dbr instanceof GR) {
			final GR ctrl = (GR)dbr;
			this.lowerDisplayLimit= ctrl.getLowerDispLimit().doubleValue();
			this.upperDisplayLimit=ctrl.getUpperDispLimit().doubleValue();
			this.lowerWarningLimit=ctrl.getLowerWarningLimit().doubleValue();
			this.upperWarningLimit=ctrl.getUpperWarningLimit().doubleValue();
			this.loweCtrlLimit=0;
			this.upperCtrlLimit=0;
			this.lowerAlarmLimit=ctrl.getLowerAlarmLimit().doubleValue();
			this.upperAlarmLimit=ctrl.getUpperAlarmLimit().doubleValue();
			this.precision=(dbr instanceof PRECISION)? ((PRECISION)dbr).getPrecision() : 0;
			this.unit=ctrl.getUnits();
			updateTypeInfo(pvName, configService);
		} else {
			logger.error("In applyBasicInfo, cannot determine dbr type for " + (dbr != null ? dbr.getClass().getName() : "Null DBR"));
		}
	}



	/**
	 * set aliaseName
	 * @param aliaseName
	 */
	public void setAliasName(String aliaseName) {
		this.aliasName=aliaseName;
	}

	/**
	 * get lowerAlarmLimit
	 * @return lowerAlarmLimit
	 */
	public double getLowerAlarmLimit() {
		return lowerAlarmLimit;
	}

	/**
	 * get loweCtrlLimit
	 * @return loweCtrlLimit
	 */
	public double getLoweCtrlLimit() {
		return loweCtrlLimit;
	}

	/**
	 * get lowerDisplayLimit
	 * @return lowerDisplayLimit
	 */
	public double getLowerDisplayLimit() {
		return lowerDisplayLimit;
	}



	/**
	 * get lowerWarningLimit
	 * @return lowerWarningLimit
	 */
	public double getLowerWarningLimit() {
		return lowerWarningLimit;
	}

	/**
	 * get upperAlarmLimit
	 * @return upperAlarmLimit
	 */
	public double getUpperAlarmLimit() {
		return upperAlarmLimit;
	}

	/**
	 * get upperCtrlLimit
	 * @return upperCtrlLimit
	 */
	public double getUpperCtrlLimit() {
		return upperCtrlLimit;
	}

	/**
	 * get upperDisplayLimit
	 * @return upperDisplayLimit
	 */

	public double getUpperDisplayLimit() {
		return upperDisplayLimit;
	}

	/**
	 * get upperWarningLimit
	 * @return upperWarningLimit
	 */
	public double getUpperWarningLimit() {
		return upperWarningLimit;
	}

	/**
	 * get precision
	 * @return precision
	 */
	public int getPrecision() {
		return precision;
	}

	/**
	 * get unit
	 * @return unit
	 */
	public String getUnit() {
		return unit;
	}

	/**
	 * get aliasName
	 * @return aliasName
	 */
	public String getAliasName() {
		return aliasName;
	}

	/**
	 * get ArchDBRTypes
	 * @return ArchDBRTypes
	 */
	public ArchDBRTypes getArchDBRTypes() {
		return archDBRTypes;
	}

	/**
	 * get the average event rate in 1 minute
	 * @return  average event rate
	 */
	public double getEventRate() {
		eventRate=((double)eventCount)/60;
		return eventRate;
	}

	/**
	 *  get the average storage rate in 1 minute
	 * @return average storage rate
	 */
	public double getStorageRate() {
		storageRate=((double)storageSize)/60;
		return storageRate;
	}

	/**
	 * get the element count of the pv's value. 
	 * @return count
	 */
	public int getCount() {
		return count;
	}

	/**
	 *the pv is vector or not
	 * @return true if vector ,else false;
	 */
	public boolean isVector() {
		return isVector;
	}

	public String[] getLabel() {
		return label;
	}

	/**
	 * get total count of event in 1 minute.
	 * @return the count of event
	 */
	public long getEventCount() {
		return eventCount;
	}

	/**
	 * get the toal storage size of the event in 1 minute
	 * @return   toal storage size
	 */
	public long getStorageSize() {
		return storageSize;
	}

	public double getSecond() {
		return second;
	}

	/**
	 * compute the storage rate and the event rate
	 * @param archDBRType 
	 * @param now  the current time and it is  the number of milliseconds since 1970/01/01
	 * @param dbrtimeevent 
	 */

	public void computeRate(final ArchDBRTypes archDBRType, long now, DBRTimeEvent dbrtimeevent, int elementCount) {
		this.count=elementCount;
		if(count>1)  { 
			isVector=true;
		} else { 
			isVector=false;
		}
		
		long tempmax=Long.MAX_VALUE-100000;

		if ((eventCount>tempmax)|(storageSize>tempmax)) {
			eventCount=0;
			storageSize=0;
			startTime= System.currentTimeMillis();
			this.second=0.01;
		} else {
			this.second=(now-this.startTime)/1000;
		}
		eventCount++;
		this.archDBRTypes=archDBRType;
		storageSize=storageSize+dbrtimeevent.getRawForm().len;
	}

	@Override
	public String toString() {
		String str="host Name:"+hostName+"\r\n";
		str=str+"lowerDisplayLimit:"+lowerDisplayLimit+"\r\n";
		str=str+"upperDisplayLimit:"+upperDisplayLimit+"\r\n";
		str=str+ "lowerWarningLimit:"+lowerWarningLimit+"\r\n";
		str=str+ "upperWarningLimit:"+upperWarningLimit+"\r\n";
		str=str+ "lowerAlarmLimit:"+lowerAlarmLimit+"\r\n";
		str=str+ "upperAlarmLimit:"+upperAlarmLimit+"\r\n";
		str=str+ "loweCtrlLimit:"+loweCtrlLimit+"\r\n";
		str=str+ "upperCtrlLimit:"+upperCtrlLimit+"\r\n";
		str=str+ "precision:"+precision+"\r\n";
		str=str+ "unit:"+unit+"\r\n";
		str=str+ "isVector:"+isVector+"\r\n";
		str=str+ "count:"+count+"\r\n";
		str=str+ "storageSize:"+storageSize+"\r\n";
		//eventCount
		str=str+ "eventCount:"+eventCount+"\r\n";
		str=str+ "second:"+second+"\r\n";
		str=str+ "aliaseName:"+aliasName+"\r\n";
		str=str+ "EventRate:"+this.getEventRate()+"events/second\r\n";
		str=str+ "storageRate:"+this.getStorageRate()+"Byte/second\r\n";
		str=str+ "DBRtype:"+archDBRTypes+"\r\n";
		for(String fieldName : otherMetaInfo.keySet()) {
			str=str+ fieldName+":"+ otherMetaInfo.get(fieldName)+"\r\n";
		}
		return str;
	}

	/**
	 * get the time when archiving this pv.
	 * @return the starting time and  the number of milliseconds since 1970/01/01
	 */
	public long getStartTime() {
		return startTime;
	}

	/**
	 * set the archiving DBRType for this pv
	 * @param archDBRTypes
	 */

	public void setArchDBRTypes(ArchDBRTypes archDBRTypes) {
		this.archDBRTypes = archDBRTypes;
	}

	/**
	 * save the other info such as MDEL,ADEL
	 * @param otherMetaInfo a hashmap including the other meta info.
	 */

	public void setOtherMetaInfo(HashMap<String, String> otherMetaInfo) {
		this.otherMetaInfo = otherMetaInfo;
	}

	/**
	 * set the lower Alarm Limit
	 * @param lowerAlarmLimit   the lower Alarm Limit
	 */
	public void setLowerAlarmLimit(double lowerAlarmLimit) {
		this.lowerAlarmLimit = lowerAlarmLimit;
	}

	/**
	 * set the lower ctrl limit
	 * @param loweCtrlLimit  the lower ctrl limit
	 */
	public void setLoweCtrlLimit(double loweCtrlLimit) {
		this.loweCtrlLimit = loweCtrlLimit;
	}

	/**
	 * set the lower display limit
	 * @param lowerDisplayLimit   the lower display limit
	 */
	public void setLowerDisplayLimit(double lowerDisplayLimit) {
		this.lowerDisplayLimit = lowerDisplayLimit;
	}

	/**
	 * set the lower warning limit
	 * @param lowerWarningLimit 
	 *     the lower warning limit
	 */
	public void setLowerWarningLimit(double lowerWarningLimit) {
		this.lowerWarningLimit = lowerWarningLimit;
	}

	/**
	 * set the upper alarm limit
	 * @param upperAlarmLimit the upper alarm limit
	 */
	public void setUpperAlarmLimit(double upperAlarmLimit) {
		this.upperAlarmLimit = upperAlarmLimit;
	}

	/**
	 * set the upper ctrl limit
	 * @param upperCtrlLimit the upper ctrl limit
	 */
	public void setUpperCtrlLimit(double upperCtrlLimit) {
		this.upperCtrlLimit = upperCtrlLimit;
	}

	/**
	 * set the upper display limit
	 * @param upperDisplayLimit  the upper display limit
	 */
	public void setUpperDisplayLimit(double upperDisplayLimit) {
		this.upperDisplayLimit = upperDisplayLimit;
	}

	/**
	 * set the upper warning limit
	 * @param upperWarningLimit  the upper warning limit
	 */
	public void setUpperWarningLimit(double upperWarningLimit) {
		this.upperWarningLimit = upperWarningLimit;
	}

	/**
	 * set the precision
	 * @param precision   precision
	 */
	public void setPrecision(int precision) {
		this.precision = precision;
	}

	/**
	 * set the unit
	 * @param unit  the unit
	 */
	public void setUnit(String unit) {
		this.unit = unit;
	}

	/**
	 * set the total event count
	 * @param eventCount
	 */
	public void setEventCount(long eventCount) {
		this.eventCount = eventCount;
	}

	/**
	 * set total storage size of the event until now
	 * @param storageSize
	 */
	public void setStorageSize(long storageSize) {
		this.storageSize = storageSize;
	}

	/**
	 * set the event rate
	 * @param eventRate the event rate
	 */
	public void setEventRate(double eventRate) {
		this.eventRate = eventRate;
	}

	/**
	 * set the storage rate
	 * @param storageRate storage rate
	 */
	public void setStorageRate(double storageRate) {
		this.storageRate = storageRate;
	}

	/**
	 * set the count of pv's value.
	 * @param count
	 */
	public void setCount(int count) {
		this.count = count;
	}

	/**
	 * set this pv to be a vector or not
	 * @param isVector
	 */
	public void setVector(boolean isVector) {
		this.isVector = isVector;
	}

	public void setLabel(String[] label) {
		this.label = label;
	}

	public void setSecond(double second) {
		this.second = second;
	}
	
	/**
	 * If we detect a change in the units or precision, we update the typeInfo in the configservice. 
	 * This should accommodate changes in EGU and precision.
	 * However, this should happen rarely or else performance will suffer.
	 * @param pvName
	 * @param configService
	 */
	private void updateTypeInfo(String pvName, ConfigService configService) {
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		boolean updated = false;
		if(typeInfo != null) { 
			if((typeInfo.getUnits() == null && this.unit != null) 
					|| (typeInfo.getUnits() != null && this.unit != null && !typeInfo.getUnits().equals(this.unit))) {
				configLogger.info("Updating units and precision for pv " + pvName + " to " + this.unit + " and " + this.precision);
				typeInfo.setUnits(this.unit);
				typeInfo.setPrecision(new Double(this.precision));
				updated = true;
			}
			
			if(typeInfo.getHostName() == null  || (typeInfo.getHostName() == null && this.hostName != null && !this.hostName.equals(typeInfo.getHostName()))) { 
				logger.debug("Updating hostname for pv " + pvName + " to " + this.hostName);
				typeInfo.setHostName(this.hostName);
				updated = true;
			}
			
			if(updated) { 
				logger.debug("Saving typeinfo in persistence for pv " + pvName);
				configService.updateTypeInfoForPV(pvName, typeInfo);
			}
		}
		
	}
	
	
	/**
	 * Add the latest metadata values to the dict
	 * @param retVal
	 */
	public void addToDict(HashMap<String, String> retVal) {
		retVal.put("LOPR", Double.toString(lowerDisplayLimit));
		retVal.put("HOPR", Double.toString(upperDisplayLimit));
		retVal.put("LOW", Double.toString(lowerWarningLimit));
		retVal.put("HIGH", Double.toString(upperWarningLimit));
		retVal.put("LOLO", Double.toString(lowerAlarmLimit));
		retVal.put("HIHI", Double.toString(upperAlarmLimit));
		retVal.put("DRVL", Double.toString(loweCtrlLimit));
		retVal.put("DRVH", Double.toString(upperCtrlLimit));
		retVal.put("PREC", Double.toString(precision));
		if(unit != null) { retVal.put("EGU", unit); } 
		retVal.put("NELM", Integer.toString(count));
		for(String fieldName : otherMetaInfo.keySet()) {
			retVal.put(fieldName, otherMetaInfo.get(fieldName));
		}
	}

    public void applyV4BasicInfo(String pvName, PVStructure pvStructure, ConfigService configService) {
        // TODO Copy over meta info from the pvStructure...
    }
}
