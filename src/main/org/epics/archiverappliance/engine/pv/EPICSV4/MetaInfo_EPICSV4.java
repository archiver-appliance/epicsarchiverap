package org.epics.archiverappliance.engine.pv.EPICSV4;

import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBR_Byte;
import gov.aps.jca.dbr.DBR_CTRL_Double;
import gov.aps.jca.dbr.DBR_CTRL_Int;
import gov.aps.jca.dbr.DBR_Double;
import gov.aps.jca.dbr.DBR_Float;
import gov.aps.jca.dbr.DBR_Int;
import gov.aps.jca.dbr.DBR_LABELS_Enum;
import gov.aps.jca.dbr.DBR_Short;
import gov.aps.jca.dbr.DBR_String;
import gov.aps.jca.dbr.DBR_TIME_Enum;
import gov.aps.jca.dbr.GR;
import gov.aps.jca.dbr.PRECISION;

import java.util.HashMap;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.JCA2ArchDBRType;
import org.epics.pvData.pv.PVStructure;

public class MetaInfo_EPICSV4 {

	// lowerAlarmLimit is responding to the LOLO field of ioc
	private double lowerAlarmLimit;
	// the lower control limit
	private double loweCtrlLimit;
	// lowerDisplayLimit is responding to the LOPR field of ioc
	private double lowerDisplayLimit;
	// lowerWarningLimit is responding to the LOW field of ioc
	private double lowerWarningLimit;
	// upperAlarmLimit is responding to the HIHI field of ioc
	private double upperAlarmLimit;
	// the upper control limit
	private double upperCtrlLimit;
	// upperDisplayLimit is responding to the HOPR field of ioc
	private double upperDisplayLimit;
	// upperWarningLimit is responding to the HIGH field of ioc
	private double upperWarningLimit;
	// the precision of the pv
	private int precision;
	// the unit of the pv
	private String unit;
	// the events count of the pv in one minute
	private long eventCount;
	// the alise name of pv, this is corresponding to the NAME field IOC
	private String aliasName;
	// the type of the pv
	private ArchDBRTypes archDBRTypes;
	// the event rate and the unit is events per second
	private double eventRate;
	// the total storage size of pv data in one minute and the unit is bytes.
	private long storageSize;
	// the storage rate of pv data and the unit is bytes per second
	private double storageRate;
	// the element count of the pv's value.
	private int count;
	// the pv is vector or not. when cout=nt >1 ,isVector=true. otherelse
	// isVector=false;
	private boolean isVector = false;
	// if the pv is a label (dbr.isLABELS()),the label of the pv is stored in
	// label[].
	private String label[];
	// time from the first data received(monitor change function called) to the
	// last data received
	private double second = 0;
	// the start time of monitorchange first called and
	// startTime=System.currentTimeMillis();
	private long startTime = 0L;
	// store other information include MDEL,ADEL &
	private HashMap<String, String> otherMetaInfo = new HashMap<String, String>();

	public void addOtherMetaInfo(String name, Double value) {
		otherMetaInfo.put(name, value.toString());
	}

	public void addOtherMetaInfo(String name, String value) {
		otherMetaInfo.put(name, value);
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public HashMap<String, String> getOtherMetaInfo() {
		return otherMetaInfo;
	}

	public void applyBasicalInfo(final PVStructure pvstructure) {

	}

	/*
	 * get the basic metadata from DBR_CTRL OR GR,including following
	 * lowerAlarmLimit; loweCtrlLimit; lowerDisplayLimit; lowerWarningLimit;
	 * upperAlarmLimit; upperCtrlLimit; upperDisplayLimit; upperWarningLimit;
	 * precision; unit ;
	 */

	public void applyBasicInfo(final DBR dbr) {

		if (dbr.isLABELS()) {
			final DBR_LABELS_Enum labels = (DBR_LABELS_Enum) dbr;
			label = labels.getLabels();

		} else if (dbr instanceof DBR_CTRL_Double) {
			final DBR_CTRL_Double ctrl = (DBR_CTRL_Double) dbr;

			this.lowerDisplayLimit = ctrl.getLowerDispLimit().doubleValue();
			this.upperDisplayLimit = ctrl.getUpperDispLimit().doubleValue();
			this.lowerWarningLimit = ctrl.getLowerWarningLimit().doubleValue();
			this.upperWarningLimit = ctrl.getUpperWarningLimit().doubleValue();
			this.lowerAlarmLimit = ctrl.getLowerAlarmLimit().doubleValue();
			this.loweCtrlLimit = ctrl.getLowerCtrlLimit().doubleValue();
			this.upperCtrlLimit = ctrl.getUpperCtrlLimit().doubleValue();
			this.upperAlarmLimit = ctrl.getUpperAlarmLimit().doubleValue();
			this.precision = ctrl.getPrecision();
			this.unit = ctrl.getUnits();
		} else if (dbr instanceof DBR_CTRL_Int) {
			final DBR_CTRL_Int ctrl = (DBR_CTRL_Int) dbr;
			this.lowerDisplayLimit = ctrl.getLowerDispLimit().doubleValue();
			this.upperDisplayLimit = ctrl.getUpperDispLimit().doubleValue();
			this.lowerWarningLimit = ctrl.getLowerWarningLimit().doubleValue();
			this.upperWarningLimit = ctrl.getUpperWarningLimit().doubleValue();
			this.loweCtrlLimit = ctrl.getLowerCtrlLimit().doubleValue();
			this.upperCtrlLimit = ctrl.getUpperCtrlLimit().doubleValue();
			this.lowerAlarmLimit = ctrl.getLowerAlarmLimit().doubleValue();
			this.upperAlarmLimit = ctrl.getUpperAlarmLimit().doubleValue();
			this.precision = 0;
			this.unit = ctrl.getUnits();

		} else if (dbr instanceof GR) {
			final GR ctrl = (GR) dbr;
			this.lowerDisplayLimit = ctrl.getLowerDispLimit().doubleValue();
			this.upperDisplayLimit = ctrl.getUpperDispLimit().doubleValue();
			this.lowerWarningLimit = ctrl.getLowerWarningLimit().doubleValue();
			this.upperWarningLimit = ctrl.getUpperWarningLimit().doubleValue();
			this.loweCtrlLimit = 0;
			this.upperCtrlLimit = 0;
			this.lowerAlarmLimit = ctrl.getLowerAlarmLimit().doubleValue();
			this.upperAlarmLimit = ctrl.getUpperAlarmLimit().doubleValue();
			this.precision = (dbr instanceof PRECISION) ? ((PRECISION) dbr)
					.getPrecision() : 0;
			this.unit = ctrl.getUnits();

		}

	}

	/* set AliaseName */
	public void setAliasName(String aliaseName) {
		this.aliasName = aliaseName;

	}

	public double getLowerAlarmLimit() {
		return lowerAlarmLimit;
	}

	public double getLoweCtrlLimit() {
		return loweCtrlLimit;
	}

	public double getLowerDisplayLimit() {
		return lowerDisplayLimit;
	}

	public double getLowerWarningLimit() {
		return lowerWarningLimit;
	}

	public double getUpperAlarmLimit() {
		return upperAlarmLimit;
	}

	public double getUpperCtrlLimit() {
		return upperCtrlLimit;
	}

	public double getUpperDisplayLimit() {
		return upperDisplayLimit;
	}

	public double getUpperWarningLimit() {
		return upperWarningLimit;
	}

	public int getPrecision() {
		return precision;
	}

	public String getUnit() {
		return unit;
	}

	public String getAliasName() {
		return aliasName;
	}

	public ArchDBRTypes getArchDBRTypes() {
		return archDBRTypes;
	}

	/*
	 * get the average eventrate in 1 minute
	 */
	public double getEventRate() {
		eventRate = ((double) eventCount) / 60;
		return eventRate;
	}

	/*
	 * get the average storagerate in 1 minute
	 */

	public double getStorageRate() {
		storageSize = storageSize / 60;
		return storageRate;
	}

	/*
	 * get the current eventrate.
	 */
	public double getCurrentEventRate() {
		// eventRate=((double)eventCount)/60;
		return eventRate;
	}

	/*
	 * get the current storagerate
	 */

	public double getCurrentStorageRate() {
		// storageSize=storageSize/60;
		return storageRate;
	}

	public int getCount() {
		return count;
	}

	public boolean isVector() {
		return isVector;
	}

	public String[] getLabel() {
		return label;
	}

	public long getEventCount() {
		return eventCount;
	}

	public long getStorageSize() {
		return storageSize;
	}

	public double getSecond() {
		return second;
	}

	private int computeCount(Data_EPICSV4 dataepicsv4) {
		String valueStr = dataepicsv4.getValue().toString();
		String[] tokens = valueStr.split("  ");
		return tokens.length;
	}

	public void computeRate(Data_EPICSV4 dataepicsv4, long now) {
		// System.out.println("computeRate");

		count = computeCount(dataepicsv4);
		if (count > 1)
			isVector = true;
		else
			isVector = false;

		long tempmax = Long.MAX_VALUE - 100000;
		if ((eventCount > tempmax) | (storageSize > tempmax)) {
			eventCount = 0;
			storageSize = 0;
			startTime = System.currentTimeMillis();
			this.second = 0.01;
		} else {
			this.second = (now - this.startTime) / 1000;
		}

		eventCount++;
		storageSize = storageSize + dataepicsv4.getValue().length;

		// storageSize
		eventRate = ((double) eventCount) / second;
		storageRate = ((double) storageSize) / second;
	}

	/*
	 * compute the dbr the DBR of channel now the current time
	 */
	public void computeRate(final DBR dbr, long now) {
		// System.out.println("computeRate");
		count = dbr.getCount();
		if (count > 1)
			isVector = true;
		else
			isVector = false;

		long tempmax = Long.MAX_VALUE - 100000;
		if ((eventCount > tempmax) | (storageSize > tempmax)) {
			eventCount = 0;
			storageSize = 0;
			startTime = System.currentTimeMillis();
			this.second = 0.01;
		} else {
			this.second = (now - this.startTime) / 1000;
		}

		eventCount++;

		archDBRTypes = JCA2ArchDBRType.valueOf(dbr);
		if (dbr.isDOUBLE()) {

			double v[];
			v = ((DBR_Double) dbr).getDoubleValue();
			storageSize = storageSize + v.length * 8;
		} else if (dbr.isFLOAT()) {
			float v[];

			v = ((DBR_Float) dbr).getFloatValue();
			storageSize = storageSize + v.length * 4;

		} else if (dbr.isINT()) {
			int v[];

			v = ((DBR_Int) dbr).getIntValue();
			storageSize = storageSize + v.length * 4;

		} else if (dbr.isSHORT()) {
			short v[];

			v = ((DBR_Short) dbr).getShortValue();
			storageSize = storageSize + v.length * 2;

		} else if (dbr.isSTRING()) {
			String v[];

			v = ((DBR_String) dbr).getStringValue();
			for (int m = 0; m < v.length; m++) {
				storageSize = storageSize + v[m].length() * 2;
			}

		} else if (dbr.isENUM()) {
			short v[];

			// 'plain' mode would subscribe to SHORT,
			// so this must be a TIME_Enum:
			final DBR_TIME_Enum dt = (DBR_TIME_Enum) dbr;
			v = dt.getEnumValue();
			storageSize = storageSize + v.length * 2;

		} else if (dbr.isBYTE()) {
			byte[] v;

			v = ((DBR_Byte) dbr).getByteValue();
			storageSize = storageSize + v.length;
		}
		// storageSize
		eventRate = ((double) eventCount) / second;
		storageRate = ((double) storageSize) / second;
	}

	@Override
	public String toString() {
		//
		String str = "lowerDisplayLimit:" + lowerDisplayLimit + "\r\n";
		str = str + "upperDisplayLimit:" + upperDisplayLimit + "\r\n";
		str = str + "lowerWarningLimit:" + lowerWarningLimit + "\r\n";
		str = str + "upperWarningLimit:" + upperWarningLimit + "\r\n";
		str = str + "lowerAlarmLimit:" + lowerAlarmLimit + "\r\n";
		str = str + "upperAlarmLimit:" + upperAlarmLimit + "\r\n";
		str = str + "loweCtrlLimit:" + loweCtrlLimit + "\r\n";
		str = str + "upperCtrlLimit:" + upperCtrlLimit + "\r\n";
		str = str + "precision:" + precision + "\r\n";
		str = str + "unit:" + unit + "\r\n";
		str = str + "isVector:" + isVector + "\r\n";
		str = str + "count:" + count + "\r\n";
		str = str + "storageSize:" + storageSize + "\r\n";
		// eventCount
		str = str + "eventCount:" + eventCount + "\r\n";
		str = str + "second:" + second + "\r\n";
		str = str + "aliaseName:" + aliasName + "\r\n";
		str = str + "EventRate:" + eventRate + "events/second\r\n";
		str = str + "storageRate:" + storageRate + "Byte/second\r\n";
		str = str + "DBRtype:" + archDBRTypes + "\r\n";

		// otherMetaInfo
		for (String fieldName : otherMetaInfo.keySet()) {
			str = str + fieldName + ":" + otherMetaInfo.get(fieldName) + "\r\n";
		}
		// DBRtype
		// aliaseName
		// EventRate
		return str;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setArchDBRTypes(ArchDBRTypes archDBRTypes) {
		this.archDBRTypes = archDBRTypes;
	}

	public void setOtherMetaInfo(HashMap<String, String> otherMetaInfo) {
		this.otherMetaInfo = otherMetaInfo;
	}

	public void setLowerAlarmLimit(double lowerAlarmLimit) {
		this.lowerAlarmLimit = lowerAlarmLimit;
	}

	public void setLoweCtrlLimit(double loweCtrlLimit) {
		this.loweCtrlLimit = loweCtrlLimit;
	}

	public void setLowerDisplayLimit(double lowerDisplayLimit) {
		this.lowerDisplayLimit = lowerDisplayLimit;
	}

	public void setLowerWarningLimit(double lowerWarningLimit) {
		this.lowerWarningLimit = lowerWarningLimit;
	}

	public void setUpperAlarmLimit(double upperAlarmLimit) {
		this.upperAlarmLimit = upperAlarmLimit;
	}

	public void setUpperCtrlLimit(double upperCtrlLimit) {
		this.upperCtrlLimit = upperCtrlLimit;
	}

	public void setUpperDisplayLimit(double upperDisplayLimit) {
		this.upperDisplayLimit = upperDisplayLimit;
	}

	public void setUpperWarningLimit(double upperWarningLimit) {
		this.upperWarningLimit = upperWarningLimit;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}

	public void setEventCount(long eventCount) {
		this.eventCount = eventCount;
	}

	public void setStorageSize(long storageSize) {
		this.storageSize = storageSize;
	}

	public void setEventRate(double eventRate) {
		this.eventRate = eventRate;
	}

	public void setStorageRate(double storageRate) {
		this.storageRate = storageRate;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public void setVector(boolean isVector) {
		this.isVector = isVector;
	}

	public void setLabel(String[] label) {
		this.label = label;
	}

	public void setSecond(double second) {
		this.second = second;
	}

}
