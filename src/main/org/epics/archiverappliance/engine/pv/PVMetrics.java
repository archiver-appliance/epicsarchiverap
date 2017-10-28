/*******************************************************************************

 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University

 * as Operator of the SLAC National Accelerator Laboratory.

 * Copyright (c) 2011 Brookhaven National Laboratory.

 * EPICS archiver appliance is distributed subject to a Software License Agreement found

 * in file LICENSE that is included with this distribution.

 *******************************************************************************/

package org.epics.archiverappliance.engine.pv;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
/***
 * PVMetrics includes the dynamic information of the pv
 * @author Luofeng Li
 *
 */

public class PVMetrics {
	private static Logger logger = Logger.getLogger(PVMetrics.class.getName());
	private static final long ROTATEEVENTS_OR_STORAGE_LIMIT = Long.MAX_VALUE - 100000;

	/**pv name*/
	private String pvName;
	/**
	 * the name of ioc where this pv is
	 */
	private String hostName;
	/**the name of pv who controlled this pv archived or not*/
	private String controlPVname;
	/**the status of archiving*/
	private boolean isArchving = false;
	/** The dbr type of this pv. This is what we used when initializing the PV */
	private ArchDBRTypes archDBRTypes = null;
	/** If we are dropping samples from type changes; this should reflect the new DBR type from the control system */
	private ArchDBRTypes newCADBRType = null;
	/**the element count of this pv's value*/
	private int elementCount = 0;
	/**is this pv archived in monitor mode?*/
	private boolean isMonitor = false;
	/**the sample period*/
	private double samplingPeriod = 0;
	/** The scan sampling period; including the jitter factor */
	private long scanPeriodMillis = 0;
	/**is this pv connected?*/
	private boolean isConnected = false;
	/**the time of last event and it is the the number of seconds since 1970/01/01*/
	private long secondsOfLastEvent;
	/**the time of last event stored in short term storage and it is the the number of seconds since 1970/01/01*/
	private long lastRotateLogsEpochSeconds;
	/**is this the fist connection?*/
	private boolean firstTimeConnection = true;
	/**is this the fist data after startup?*/
	private boolean firstDataAfterStartUp = true;
	/** Indicates some actions after pv is connected */
	private boolean isFirstDataAfterConnection=false;
	/**the time when connection established for the first time and it is the the number of seconds since 1970/01/01*/
	private long connectionFirstEstablishedEpochSeconds;
	/**the time when last connection reestablished and it is the the number of seconds since 1970/01/01*/
	private long connectionLastRestablishedEpochSeconds;
	/**the time when the connection request was sent and it is the the number of seconds since 1970/01/01*/
	private long connectionRequestMadeEpochSeconds;
	/**the amount of connection lost and regained*/
	private long connectionLossRegainCount;
	/**the time when last  connection was lost and it is the the number of seconds since 1970/01/01*/
	private long connectionLastLostEpochSeconds;
	/**the time when starting archiving and it is the the number of seconds since 1970/01/01
	 * When connection is lost , this time will be updated
	 * */
	private long lastStartEpochSeconds;
	private final long initialEpochSeconds;

	/**how many events so far? */
	private long eventCounts;
	/**how  many storage size of all the events so far?
	 * */
	private long storageSize;
    /**is this pv archiving or not?*/
	private boolean isEnable = true;
	/**how many events were dropped because of the wrong timestamp?*/
	private long timestampWrongEventCount = 0;

	/**how many events were dropped because of the over flow of sample buffer?*/
	private long sampleBufferFullLostEventCount = 0;
	
	/**
	 * how many events were dropped because of the incorrect DBR type
	 */
	private long invalidTypeLostEventCount = 0;
	
	/**
	 * This is the timestamp of the last event from the IOC regardless of whether the timestamp is accurate or not 
	 * Note this may not be what's written out into the archive that we used to compare against to enforce monotonically increasing eventstreams
	 */
	private Timestamp lastEventFromIOCTimeStamp = null;

	/**
	 * The state of the connection at the last connectionChangedEvent.
	 * Murali added this to show in PVMetrics. 
	 * Feel free to remove if not needed anymore
	 */
	private boolean lastConnectionEventState;
	
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

   /***
    * 
    * @return  the name of pv who controlled this pv archived or not
    */
	public String getcontrolPVname() {
		return this.controlPVname;
	}

	public void setConnectionLastLostEpochSeconds(long connectionLastLostEpochSeconds) {
		this.connectionLastLostEpochSeconds = connectionLastLostEpochSeconds;
		isFirstDataAfterConnection=false;
	}

    /**
     * 
     * @return the time when last  connection was lost and it is the the number of seconds since 1970/01/01
     */
	public long getConnectionLastLostEpochSeconds() {
		return this.connectionLastLostEpochSeconds;
	}

   /**
    * add 1 to the count of the events were dropped because of the wrong time stamp
    * We keep a copy of the last incorrect timestamp we obtained and only increment if the current incorrect timestamp is different.
    * @param incorrectTimeStamp  &emsp;
    */

	public void addTimestampWrongEventCount(Timestamp incorrectTimeStamp) {
		if(lastEventFromIOCTimeStamp != null && incorrectTimeStamp != null && incorrectTimeStamp.equals(lastEventFromIOCTimeStamp)) { 
			return;
		}
		
		lastEventFromIOCTimeStamp = incorrectTimeStamp;
		timestampWrongEventCount++;
	}

   /**
    * add 1 to the count of the events were dropped because of the over flow of the sample buffer
    */
	public void addSampleBufferFullLostEventCount() {
		sampleBufferFullLostEventCount++;
	}
	
	public void incrementInvalidTypeLostEventCount(ArchDBRTypes newCADBRType) {
		invalidTypeLostEventCount++;
		this.newCADBRType = newCADBRType;
	}
	
	/**
	 * If the PV changes DBR_Type, we do not add it to the sample buffer. This keeps track of how many event were lost because of a type change.
	 * @return invalidTypeLostEventCount &emsp;
	 */
	public long getInvalidTypeLostEventCount() {
		return invalidTypeLostEventCount;
	}


    /**
     * 
     * @return the count of the events were dropped because of the wrong time stamp
     */
	public long getTimestampWrongEventCount() {
		return timestampWrongEventCount;
	}

    /***
     * 
     * @return count of the events were dropped because of over flow of the sample buffer
     */
	public long getSampleBufferFullLostEventCount() {
		return sampleBufferFullLostEventCount;
	}

   /**
    * 
    * @return  true if this pv is archived. Otherwise false
    */
	public boolean isEnable() {
		return isEnable;
	}

	public void setEnable(boolean isEnable) {
		this.isEnable = isEnable;
	}

	public void setConnectionEstablishedEpochSeconds(long connectionEstablishedEpochSeconds) {
		eventCounts = 0;
		storageSize = 0;
		this.lastStartEpochSeconds = connectionEstablishedEpochSeconds;
		isFirstDataAfterConnection=true;
		if (firstTimeConnection) {
			this.connectionFirstEstablishedEpochSeconds = System.currentTimeMillis() / 1000;
			firstTimeConnection = false;
		} else {
			this.connectionLastRestablishedEpochSeconds = System.currentTimeMillis() / 1000;
			this.connectionLossRegainCount = this.connectionLossRegainCount + 1;
		}
	}

	/**
	 * update event count and the time of last event and running 
	 * */
	public void addEventCounts() {
		eventCounts++;
		if (eventCounts > ROTATEEVENTS_OR_STORAGE_LIMIT) {
			eventCounts = 0;
			storageSize = 0;
			lastStartEpochSeconds = System.currentTimeMillis() / 1000;
		}
	}

   /**
    * 
    * @return  the average event rate in count/second
    */
	public double getEventRate() {
		double eventRateTemp=0;
		if(lastStartEpochSeconds==0){
			eventRateTemp=(double) this.eventCounts / (System.currentTimeMillis() / 1000 - this.initialEpochSeconds);
		}else{
			eventRateTemp=(double) this.eventCounts / (System.currentTimeMillis() / 1000 - lastStartEpochSeconds);
		}

		if(Double.isNaN(eventRateTemp)|Double.isInfinite(eventRateTemp)) { 
			eventRateTemp=0;
		}

		return eventRateTemp;
	}

	/**
	 * 
	 * @return  the average storage rate in byte/second
	 */
	public double getStorageRate() {
		double storageRateTemp=0;
		if(lastStartEpochSeconds==0){
			storageRateTemp=(double) this.storageSize / (System.currentTimeMillis() / 1000 - this.initialEpochSeconds);
		} else {
			storageRateTemp=(double) this.storageSize / (System.currentTimeMillis() / 1000 - lastStartEpochSeconds);
		}

		if(Double.isNaN(storageRateTemp)|Double.isInfinite(storageRateTemp)) storageRateTemp=0;
		return storageRateTemp;
	}

	/**
	 * update the staorage size
	 * @param timeevent  DBRTimeEvent
	 */
	public void addStorageSize(final DBRTimeEvent timeevent) {
		// int count =timeevent.getSampleValue().getElementCount();
		storageSize = storageSize + timeevent.getRawForm().len;
		if (storageSize > ROTATEEVENTS_OR_STORAGE_LIMIT) {
			eventCounts = 0;
			storageSize = 0;
			lastStartEpochSeconds = System.currentTimeMillis() / 1000;
		}
	}

/**
 * Constructor for PVMetrics
 * @param pvName  the name of pv
 * @param controlPVname  the name of pv who controls this pv archived or not
 * @param startEpochSeconds  the starting  time and it is the the number of seconds since 1970/01/01
 * @param dbrTypes ArchDBRTypes
 */
	public PVMetrics(String pvName, String controlPVname, long startEpochSeconds, ArchDBRTypes dbrTypes) {
		this.controlPVname = controlPVname;
		this.pvName = pvName;
		this.lastStartEpochSeconds = startEpochSeconds;
		this.archDBRTypes = dbrTypes;
		this.newCADBRType = this.archDBRTypes;
		this.initialEpochSeconds=System.currentTimeMillis()/1000;
	}

   /**
    * 
    * @return the name of this pv
    */
	public String getPvName() {
		return pvName;
	}

	/**
	 * 
	 * @return true if this pv is archived.Otherwise, false
	 */
	public boolean isArchving() {
		return isArchving;
	}

	/**
	 * set the archiving status
	 * @param isArchving  &emsp;
	 */
	public void setArchving(boolean isArchving) {
		this.isArchving = isArchving;
	}

   /**
    * 
    * @return the archiving dbr type of this pv
    */
	public ArchDBRTypes getArchDBRTypes() {
		return archDBRTypes;
	}

	/**
	 * 
	 * @return the element count of this pv's value
	 */
	public int getElementCount() {
		return elementCount;
	}

   /**
    * set the element count of this pv
    * @param elementCount  &emsp;
    */
	public void setElementCount(int elementCount) {
		this.elementCount = elementCount;
	}

    /**
     * 
     * @return true if this pv is archived in monitor mode.Otherwise, false
     */
	public boolean isMonitor() {
		return isMonitor;
	}

	/***
	 * set the archiving mode
	 * @param isMonitor  &emsp;
	 */
	public void setMonitor(boolean isMonitor) {
		this.isMonitor = isMonitor;
	}

	/**
	 * 
	 * @return the sample period
	 */
	public double getSamplingPeriod() {
		return samplingPeriod;
	}

    /**
     * set the sample period
     * @param samplingPeriod  in second
     */
	public void setSamplingPeriod(double samplingPeriod) {
		this.samplingPeriod = samplingPeriod;
	}

   /**
    * 
    * @return true if this pv connected .Otherwise, false
    */
	public boolean isConnected() {
		return isConnected;
	}

	/***
	 * set the connection status
	 * @param isConnected  &emsp;
	 */
	public void setConnected(boolean isConnected) {
		this.isConnected = isConnected;
	}

  /**
   * 
   * @return the time of last event and it is the the number of seconds since 1970/01/01
   */
	public long getSecondsOfLastEvent() {
		return secondsOfLastEvent;
	}

   /**
    * set the last event time
    * @param secondsOfLastEvent   the time of last event and it is the the number of seconds since 1970/01/01
    */
	public void setSecondsOfLastEvent(long secondsOfLastEvent) {
		this.secondsOfLastEvent = secondsOfLastEvent;
	}

    /**
     * 
     * @return  the last time when this pv's data was stored in short term storage. and it is the the number of seconds since 1970/01/01
     */
	public long getLastRotateLogsEpochSeconds() {
		return lastRotateLogsEpochSeconds;
	}

   /**
    *  set the time when the last data of this pv was stored
    * @param lastRotateLogsEpochSeconds the last time when this pv's data was stored in short term storage. and it is the the number of seconds since 1970/01/01
    */
	public void setLastRotateLogsEpochSeconds(long lastRotateLogsEpochSeconds) {
		this.lastRotateLogsEpochSeconds = lastRotateLogsEpochSeconds;
	}

   /**
    * 
    * @return  the time when the first connection was established and it is the the number of seconds since 1970/01/01
    */
	public long getConnectionFirstEstablishedEpochSeconds() {
		return connectionFirstEstablishedEpochSeconds;
	}

   /**
    * set the time for the first connection
    * @param connectionFirstEstablishedEpochSeconds   the time when the first connection was established and it is the the number of seconds since 1970/01/01
    */
	public void setConnectionFirstEstablishedEpochSeconds(long connectionFirstEstablishedEpochSeconds) {
		this.connectionFirstEstablishedEpochSeconds = connectionFirstEstablishedEpochSeconds;
	}

    /**
     * 
     * @return   the time when the last connection was established and it is the the number of seconds since 1970/01/01
     */
	public long getConnectionLastRestablishedEpochSeconds() {
		return connectionLastRestablishedEpochSeconds;
	}

    /**
     * set the time when the last connection was established
     * @param connectionLastRestablishedEpochSeconds  the time when the last connection was established and it is the the number of seconds since 1970/01/01
     */
	public void setConnectionLastRestablishedEpochSeconds(long connectionLastRestablishedEpochSeconds) {
		this.connectionLastRestablishedEpochSeconds = connectionLastRestablishedEpochSeconds;
	}

   /**
    * 
    * @return the count of the connection lost and regained
    */
	public long getConnectionLossRegainCount() {
		return connectionLossRegainCount;
	}

	/**
	 * set the count of the connection lost and regained
	 * @param connectionLossRegainCount the count of the connection lost and regained
	 */
	public void setConnectionLossRegainCount(long connectionLossRegainCount) {
		this.connectionLossRegainCount = connectionLossRegainCount;
	}

	private static void addDetailedStatus(LinkedList<Map<String, String>> statuses, String name, String value) {
		Map<String, String> obj = new LinkedHashMap<String, String>();
		obj.put("name", name);
		obj.put("value", value);
		obj.put("source", "pv");
		statuses.add(obj);
	}

  /**
   * 
   * @return  the json string includes all the info in this class
   */
	public LinkedList<Map<String, String>> getDetailedStatus() {
		LinkedList<Map<String, String>> statuses = new LinkedList<Map<String, String>>();
		DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
		addDetailedStatus(statuses, "Channel Name", pvName);
		addDetailedStatus(statuses, "Host name", hostName==null?"":hostName);
		addDetailedStatus(statuses, "Controlling PV", controlPVname == null ? "" : controlPVname);
		addDetailedStatus(statuses, "Is engine currently archiving this?",this.isArchving ? "yes" : "no");
		addDetailedStatus(statuses, "Archiver DBR type (initial)", this.archDBRTypes == null ? "Unkown" : this.archDBRTypes.toString());
		addDetailedStatus(statuses, "Archiver DBR type (from CA)", this.newCADBRType == null ? "Unkown" : this.newCADBRType.toString());
		addDetailedStatus(statuses, "Number of elements per event (from CA)", "" + this.elementCount);
		addDetailedStatus(statuses, "Is engine using monitors?", this.isMonitor ? "yes" : "no");
		addDetailedStatus(statuses, "What's the engine's sampling period?", ""+ (float)this.samplingPeriod);
		addDetailedStatus(statuses, "The SCAN period (ms) after applying the jitter factor", ""+ this.scanPeriodMillis);
		addDetailedStatus(statuses, "Is this PV currently connected?", this.isConnected ? "yes" : "no");
		addDetailedStatus(statuses, "Connection state at last connection changed event", this.lastConnectionEventState ? "Connected" : "Not connected");
		addDetailedStatus(statuses, "When did we receive the last event?", TimeUtils.convertToHumanReadableString(this.secondsOfLastEvent));
		addDetailedStatus(statuses, "What did we last push the data to the short term store?", TimeUtils.convertToHumanReadableString(this.lastRotateLogsEpochSeconds));
		addDetailedStatus(statuses, "When did we request CA to make a connection to this PV?", TimeUtils.convertToHumanReadableString(connectionRequestMadeEpochSeconds));
		addDetailedStatus(statuses, "When did we first establish a connection to this PV?", TimeUtils.convertToHumanReadableString(connectionFirstEstablishedEpochSeconds));
		addDetailedStatus(statuses, "When did we last lose and reestablish a connection to this PV?", TimeUtils.convertToHumanReadableString(connectionLastRestablishedEpochSeconds));
		addDetailedStatus(statuses, "When did we last lose a connection to this PV?", TimeUtils.convertToHumanReadableString(this.connectionLastLostEpochSeconds));
		addDetailedStatus(statuses, "How many times have we lost and regained the connection to this PV?", Long.toString(connectionLossRegainCount));
		addDetailedStatus(statuses, "How many events so far?", Long.toString(this.eventCounts));
		addDetailedStatus(statuses, "How many events lost because the timestamp is in the far future or past so far?", Long.toString(this.timestampWrongEventCount));
		addDetailedStatus(statuses, "Timestamp of last event from the IOC - correct or not.", this.getLastEventFromIOCTimeStampStr());
		addDetailedStatus(statuses, "How many events lost because the sample buffer is full so far?", Long.toString(this.sampleBufferFullLostEventCount));
		addDetailedStatus(statuses, "How many events lost because the DBR_Type of the PV has changed from what it used to be?", Long.toString(this.invalidTypeLostEventCount));
		addDetailedStatus(statuses, "How many events lost totally so far?", Long.toString(this.timestampWrongEventCount + this.sampleBufferFullLostEventCount + this.invalidTypeLostEventCount));
		if (storageSize > 0 && eventCounts > 0) {
			addDetailedStatus(statuses, "Average bytes per event", twoSignificantDigits.format(((double) this.storageSize) / this.eventCounts));
		}
		double estimatedEventRate = this.getEventRate();
		addDetailedStatus(statuses, "Estimated event rate (events/sec)", (estimatedEventRate <= 0.0) ? "Not enough info" : twoSignificantDigits.format(estimatedEventRate));
		double estimatedStorageRateInBytesPerSec = this.getStorageRate();
		double estimatedStorageRateInKiloBytesPerHour = this.getStorageRate() * 60 * 60 / 1024;
		addDetailedStatus(statuses, "Estimated storage rate (KB/hour)", (estimatedStorageRateInBytesPerSec <= 0.0) ? "Not enough info" : twoSignificantDigits.format(estimatedStorageRateInKiloBytesPerHour));
		double estimatedStorageRateInMegaBytesPerDay = this.getStorageRate() * 60 * 60 * 24 / (1024 * 1024);
		addDetailedStatus(statuses, "Estimated storage rate (MB/day)", (estimatedStorageRateInBytesPerSec <= 0.0) ? "Not enough info" : twoSignificantDigits.format(estimatedStorageRateInMegaBytesPerDay));
		double estimatedStorageRateInGigaBytesPerYear = this.getStorageRate() * 60 * 60 * 24 * 365 / (1024 * 1024 * 1024);
		addDetailedStatus(statuses, "Estimated storage rate (GB/year)", (estimatedStorageRateInBytesPerSec <= 0.0) ? "Not enough info" : twoSignificantDigits.format(estimatedStorageRateInGigaBytesPerYear));
		return statuses;
	}

	/**
	 * 
	 * @return the time when the connection request was sent and it is the the number of seconds since 1970/01/01
	 */
	public long getConnectionRequestMadeEpochSeconds() {
		return connectionRequestMadeEpochSeconds;
	}

	/**
	 * set the the time when the connection request was sent
	 * @param connectionRequestMadeEpochSeconds the time when the connection request was sent and it is the the number of seconds since 1970/01/01
	 */
	public void setConnectionRequestMadeEpochSeconds(long connectionRequestMadeEpochSeconds) {
		this.connectionRequestMadeEpochSeconds = connectionRequestMadeEpochSeconds;
	}

	/**
	 * This is the timestamp of the last event from the IOC regardless of whether the timestamp is accurate or not 
	 * Note this may not be what's written out into the archive that we used to compare against to enforce monotonically increasing eventstreams
	 * @return HumanReadableLastEventFromIOCTimeStamp
	 */
	public String getLastEventFromIOCTimeStampStr() {
		if(lastEventFromIOCTimeStamp == null) return "N/A";
		return TimeUtils.convertToHumanReadableString(lastEventFromIOCTimeStamp);
	}

	public long getLastEventFromIOCTimeStamp() {
		return TimeUtils.convertToEpochSeconds(lastEventFromIOCTimeStamp);
	}


	public void setLastEventFromIOCTimeStamp(Timestamp lastEventFromIOCTimeStamp) {
		this.lastEventFromIOCTimeStamp = lastEventFromIOCTimeStamp;
	}

	public boolean isLastConnectionEventState() {
		return lastConnectionEventState;
	}

	public void setLastConnectionEventState(boolean lastConnectionEventState) {
		this.lastConnectionEventState = lastConnectionEventState;
	}

	public void resetConnectionLastLostEpochSeconds() {
		connectionLastLostEpochSeconds = 0;
	}
	
	/**
	 * Add the cnxlostepsecs and cnxregainedepsecs to the specified DBRTimeEvent and then reset local state. 
	 * @param event DBRTimeEvent
	 */
	public void addConnectionLostRegainedFields(DBRTimeEvent event) {
		if(firstDataAfterStartUp) {
			long cnxregainedsecs = System.currentTimeMillis()/1000;
			if(logger.isDebugEnabled()) { 
				logger.debug("Adding cnxlostepsecs and cnxregainedepsecs after startup for pv " + pvName + " at " + cnxregainedsecs + " onto event @ " + event.getEpochSeconds());
			}
			event.addFieldValue("cnxlostepsecs", Long.toString(connectionLastLostEpochSeconds));
			event.addFieldValue("cnxregainedepsecs", Long.toString(cnxregainedsecs));
			event.addFieldValue("startup", Boolean.TRUE.toString());
			connectionLastLostEpochSeconds = 0;
			firstDataAfterStartUp=false;
			isFirstDataAfterConnection=false;
		} else {
			long cnxregainedsecs = System.currentTimeMillis()/1000;
			if(isFirstDataAfterConnection) {
				if(connectionLastLostEpochSeconds != 0 && connectionLastLostEpochSeconds != cnxregainedsecs) { 
					if(logger.isDebugEnabled()) { 
						logger.debug("Adding cnxlostepsecs and cnxregainedepsecs after regaining connection for pv " + pvName + " at " + cnxregainedsecs + " onto event @ " + event.getEpochSeconds());
					}
					event.addFieldValue("cnxlostepsecs", Long.toString(connectionLastLostEpochSeconds));
					event.addFieldValue("cnxregainedepsecs", Long.toString(cnxregainedsecs));
					connectionLastLostEpochSeconds = 0;
				} else { 
					logger.debug("Skipping adding cnxlostepsecs and cnxregainedepsecs after regaining connection for pv " + pvName);
				}
				isFirstDataAfterConnection=false;
			}
		}
	}

	public long getScanPeriodMillis() {
		return scanPeriodMillis;
	}

	public void setScanPeriodMillis(long scanPeriodMillis) {
		this.scanPeriodMillis = scanPeriodMillis;
	}	
}
