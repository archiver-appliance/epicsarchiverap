/*******************************************************************************

 * Copyright (c) 2010 Oak Ridge National Laboratory.

 * All rights reserved. This program and the accompanying materials

 * are made available under the terms of the Eclipse Public License v1.0

 * which accompanies this distribution, and is available at

 * http://www.eclipse.org/legal/epl-v10.html

 ******************************************************************************/
package org.epics.archiverappliance.engine.model;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.pv.EPICS_V3_PV;
import org.epics.archiverappliance.engine.pv.EPICS_V4_PV;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PV;
import org.epics.archiverappliance.engine.pv.PVFactory;
import org.epics.archiverappliance.engine.pv.PVListener;
import org.epics.archiverappliance.engine.pv.PVMetrics;
/**
 * Base for archived channels. An ArchiveChannel has
 * <ol>
 * <li>A main PV; in the typical case, this would be the PV for the .VAL. The record processing timestamp from this PV constitutes identity.</li>
 * <li>A optional collection of metadata/field PVs. Data from these PV's is stored as part of the mainPV.</li>
 * <li>A SampleBuffer where the data from the main PV is stored.</li>
 * <li>A Writer, typically the STS, where the data in the SampleBuffer is periodically flushed.</li>
 * </ol>
 * 
 * @author Kay Kasemir
 * @version Initial version:CSS
 * @version 4-Jun-2012, Luofeng Li:added codes to support for the new archiver
 */

@SuppressWarnings("nls")
abstract public class ArchiveChannel {
	private static final Logger logger = LogManager.getLogger(ArchiveChannel.class);

	/** Throttled log for NaN samples */
	private static ThrottledLogger trouble_sample_log = new ThrottledLogger(LogLevel.info, 60);

	/**
	 * Channel name. This is the name by which the channel was created, not the PV name that might include decorations.
	 */
	final private String name;

	/** This is the actual control system PV. */
	final private PV pv;
	
	/**
	 * The control system PVs for the metafields
	 */
	private ConcurrentHashMap<String, PV> metaPVs = new ConcurrentHashMap<String, PV>();
	

	/**
	 * The name of the PV that control archiving of this PV.
	 */
	final private String controlPVname;

	/**
	 * Is this channel currently running?
	 * <p>
	 * PV sends another 'disconnected' event as the result of 'stop', but we
	 * don't want to log that, so we keep track of the 'running' state.
	 */
	private boolean is_running = false;

	/**
	 * Do we need to log a 'write error' sample?
	 * <p>
	 * The sample buffer will indicate write errors. While in error, we keep
	 * adding samples, which will probably cause overrides. When we can write
	 * again, we add one info sample.
	 */

	private boolean need_write_error_sample = false;

	/**
	 * Do we need to log a 'first' sample?
	 * <p>
	 * After startup, or after a network disconnect, the first sample we receive
	 * might be time-stamped days ago, while the archive has an 'off' or
	 * 'disconnected' info sample that's already newer. This flag is used to
	 * force one initial sample into the archive with current time stamp.
	 */

	private boolean need_first_sample = true;

	/** How channel affects its groups */
	final private Enablement enablement;

	/** Is this channel currently enabled? */
	private boolean enabled = true;

	/** Buffer of received samples, periodically written */
	private SampleBuffer buffer = null;

	/**
	 * The writer/storage plugin that receives the samples.
	 * We place samples into SampleBufffer and then the write thread comes along periodically and flushes these samples into this storageplugin.
	 */
	private final Writer writer;

	/** Counter for received values (monitor updates) */
	private long received_value_count = 0;

	/**
	 * The latest DBRTimeEvent received from the control system
	 */
	protected DBRTimeEvent latestDBRTimeEvent = null;

	/**
	 * The last DBRTimeEvent written to the archive.
	 */
	protected DBRTimeEvent lastDBRTimeEvent = null;

	/**
	 * pvMetrics for this channel. Ideally, we should move all metrics to this class.
	 */
	final protected PVMetrics pvMetrics;

	/**
	 * the last time stamp when this pv was archived
	 */
	private Timestamp last_archived_timestamp = null;
	
	/**
	 * The engine can use multiple contexts and can assign PV's to contexts based on some algorithm.
	 * 
	 */
	private int JCACommandThreadID = -1;
	

	/**
	 * if this channel is started or stopped archiving by a another pv, return the pv's name
	 * @return the pv's name who control this channel archiving
	 */
	public String getControlPVname() {
		return this.controlPVname;
	}

	/**
	 * set the time stamp of the  last value is stored in short term storage
	 * @param lastRotateLogsEpochSecond  the time stamp of last value and it is  
	 * the number of milliseconds since 1970/01/01
	 */
	public void setlastRotateLogsEpochSeconds(long lastRotateLogsEpochSecond) {
		this.pvMetrics.setLastRotateLogsEpochSeconds(lastRotateLogsEpochSecond);
	}

	/**
	 * get the writer for this channel
	 * @return  the writer for this channel
	 */
	public Writer getWriter() {
		return this.writer;
	}

	/**
	 * get pv metrics for this channel
	 * @return PVMetrics
	 */
	public PVMetrics getPVMetrics() {
		this.pvMetrics.setConnected(this.pv.isConnected());
		this.pvMetrics.setArchving(this.pv.isRunning());
		return pvMetrics;
	}

	/**
	 * create archive  channel
	 * @param name pv's name
	 * @param writer the writer for this pv
	 * @param enablement  start or stop archiving this pv when channel is created
	 * @param buffer_capacity the sample buffer's capacity for this pv
	 * @param last_archived_timestamp the last time stamp when this pv was archived
	 * @param configservice the configservice of new archiver
	 * @param archdbrtype the archiving dbr type
	 * @param controlPVname the pv's name who control this pv to start archiving or stop archiving
	 * @param commandThreadID - this is the index into the array of JCA command threads that processes this context.
	 * @param usePVAccess - Should we use PVAccess to connect to this PV.
	 * @throws Exception error when creating archive channel for this pv
	 */
	public ArchiveChannel(
			final String name, 
			final Writer writer,
			final Enablement enablement, 
			final int buffer_capacity,
			final Timestamp last_archived_timestamp,
			final ConfigService configservice,
			final ArchDBRTypes archdbrtype,
			final String controlPVname, 
			final int commandThreadID,
			final boolean usePVAccess)
					throws Exception {
		this.name = name;
		this.controlPVname = controlPVname;
		this.writer = writer;
		this.enablement = enablement;
		this.last_archived_timestamp = last_archived_timestamp;
		this.pvMetrics = new PVMetrics(name, controlPVname, System.currentTimeMillis() / 1000, archdbrtype);
		this.buffer = new SampleBuffer(name, buffer_capacity, archdbrtype,this.pvMetrics);
		this.JCACommandThreadID =  commandThreadID;

		this.pv = PVFactory.createPV(name, configservice, false, archdbrtype, commandThreadID, usePVAccess, false);

		pv.addListener(new PVListener() {
			@Override
			public void pvValueUpdate(final PV pv, final DBRTimeEvent temptimeevent) {
				// PV already suppresses updates after 'stop', but check anyway
				if (is_running) {
					try {
						if (enablement != Enablement.Passive)
							handleEnablement(temptimeevent);

						handleNewValue(temptimeevent);
					} catch (Exception e) {
						logger.error("exception in pvValueUpdate of PVListener for pv " + name, e);
					}
				}
			}

			@Override
			public void pvDisconnected(final PV pv) {
				try {
					if (is_running)
						pvMetrics.setConnectionLastLostEpochSeconds(System.currentTimeMillis()/1000);
					
					handleDisconnected();
				} catch (Exception e) {
					logger.error("exception in pvDisconnected of PVListener", e);
				}
			}

			@Override
			public void pvConnected(PV pv) {
				logger.debug("Connected to PV " + name);
				pvMetrics.setHostName(pv.getHostName());
				pvMetrics.setConnectionEstablishedEpochSeconds(System.currentTimeMillis() / 1000);
			}

			@Override
			public void pvConnectionRequestMade(PV pv) {
				pvMetrics.setConnectionRequestMadeEpochSeconds(System.currentTimeMillis()/1000);
			}

			@Override
			public void sampleDroppedTypeChange(PV pv, ArchDBRTypes newCAType) {
				pvMetrics.incrementInvalidTypeLostEventCount(newCAType);
			}
		});
	}

	
	/**
	 * Initialize the metafields for this channel. In addition to the metafields specified  here, we also generate PV's for the runtime fields.
	 * @param metaFields
	 * @param configservice
	 * @param usePVAccess
	 * @param useDBEProperties
	 * @throws IOException
	 */
	static HashSet<String> fieldsAvailableFromDBRControl = new HashSet<String>();
	static { 
		fieldsAvailableFromDBRControl.addAll(Arrays.asList(new String[] {"PREC", "EGU", "HOPR", "LOPR", "HIHI", "HIGH", "LOW", "LOLO", "DRVH", "DRVL" }));  
	}
	public void initializeMetaFieldPVS(final String[] metaFields, final ConfigService configservice, final boolean usePVAccess, final boolean useDBEProperties) throws IOException {
		HashSet<String> runtTimeFieldsCopy = new HashSet<String>();
		HashSet<String> nonDBEPropertiesFields = new HashSet<String>();
		HashSet<String> DBEPropertiesFields = new HashSet<String>();
		if(metaFields != null && metaFields.length > 0) {
			for(String metaField : metaFields) { 
				if(useDBEProperties && fieldsAvailableFromDBRControl.contains(metaField)) {
					DBEPropertiesFields.add(metaField);
				} else { 
					nonDBEPropertiesFields.add(metaField);
				}
			}
		}
		
		if(!configservice.getRuntimeFields().isEmpty()) { 
			for(String metaField : configservice.getRuntimeFields()) { 
				if(useDBEProperties && fieldsAvailableFromDBRControl.contains(metaField)) {
					DBEPropertiesFields.add(metaField);
				} else if (nonDBEPropertiesFields.contains(metaField)) { 
					
				} else { 
					runtTimeFieldsCopy.add(metaField);
				}
			}
		}
		
		
		if(!DBEPropertiesFields.isEmpty()) { 
			logger.debug("Adding a DBE_PROPERTY monitor for pv " + name);
			this.addMetaField(name, configservice, false, usePVAccess, true);
		}
		
		for (String fieldName : nonDBEPropertiesFields) {
			logger.debug("Adding non DBE_PROPERTY monitor for meta field " + fieldName);
			this.addMetaField(fieldName, configservice, false, usePVAccess, false);
			runtTimeFieldsCopy.remove(fieldName);
		}

		for(String runtimeField : runtTimeFieldsCopy) { 
			logger.debug("Adding non DBE_PROPERTY monitor for runtime field " + runtimeField);
			this.addMetaField(runtimeField, configservice, true, usePVAccess, false);
		}

	}

	
	// By default, the ArchDBRTypes for the metafields are the same as those of the PV.
	// In some cases, however, these are always different and fixed.
	static HashMap<String, ArchDBRTypes> metaFieldOverrideTypes = new  HashMap<String, ArchDBRTypes>();
	static { 
		metaFieldOverrideTypes.put("DESC", ArchDBRTypes.DBR_SCALAR_STRING);
	}

	
	/**
	 * Add a pv for this PV for the given metafield.
	 * @param fieldName
	 * @param configservice
	 * @param isRuntimeOnly
	 * @param usePVAccess
	 * @throws IOException
	 */
	private void addMetaField(String fieldName, ConfigService configservice, boolean isRuntimeOnly, boolean usePVAccess, boolean useDBEProperties) throws IOException {
		if(this.pv == null) throw new IOException("Cannot add metadata fields for channel that does not have its PV initialized.");

		if (!usePVAccess) {
			EPICS_V3_PV v3Pv = (EPICS_V3_PV) this.pv;
			v3AddMetaField(v3Pv, fieldName, configservice, isRuntimeOnly, useDBEProperties);
		} else {
			EPICS_V4_PV v4Pv = (EPICS_V4_PV) this.pv;
			v4Pv.addMetaField(fieldName);
		}
	}

	private void v3AddMetaField(EPICS_V3_PV v3Pv,  String fieldName, ConfigService configservice, boolean isRuntimeOnly, boolean useDBEProperties) {
		if(useDBEProperties) {
			// For a DBE_PROPERTIES, we use the name of the PV as the name of the channel
			v3Pv.setDBEroperties();
			return;
		}
		// This tells the main PV to create the hashmaps for the metafield storage
		v3Pv.markPVHasMetafields(true);
		String pvNameForField = PVNames.stripFieldNameFromPVName(this.name) + "." + fieldName;
		ArchDBRTypes metaFieldDBRType = v3Pv.getArchDBRTypes();
		if(metaFieldOverrideTypes.containsKey(fieldName)) {
			metaFieldDBRType = metaFieldOverrideTypes.get(fieldName);
		}
		logger.debug("Initializing the metafield for field " + pvNameForField + " as ArchDBRType " + metaFieldDBRType.toString() + " DBE_PROPERTIES is " + false);
		EPICS_V3_PV metaPV = (EPICS_V3_PV) PVFactory.createPV(pvNameForField, configservice, false, metaFieldDBRType, this.JCACommandThreadID, false, false);
		metaPV.setMetaFieldParentPV(v3Pv, isRuntimeOnly);
		this.metaPVs.put(fieldName, metaPV);
	}

	/**
	 * get the combined ArrayListEventStream of prevouse and current 
	 * @return ArrayListEventStream
	 */
	public ArrayListEventStream getPVData() {
		return this.buffer.getCombinedSamples();
	}



	/** @return Name of channel */
	final public String getName() {
		return name;
	}



	/** @return How channel affects its groups */
	final public Enablement getEnablement() {
		return enablement;
	}



	/** @return <code>true</code> if channel is currently enabled */
	final public boolean isEnabled() {
		return enabled;
	}

	/** @return Short description of sample mechanism */
	abstract public String getMechanism();

	/** @return <code>true</code> if connected */
	final public boolean isConnected() {
		return pv.isConnected();
	}

	/** Start archiving this channel. 
	 *
	 * @throws Exception  &emsp;
	 */
	public final void start() throws Exception {
		if (is_running)
			return;

		is_running = true;
		enabled = true;
		need_first_sample = true;
		pvMetrics.setEnable(true);
		pv.start();
	}



	/**
	 * Stop archiving this channel
	 * 
	 * @throws Exception  &emsp;
	 */
	public final void stop() throws Exception {
		if (!is_running)
			return;

		is_running = false;
		enabled = false;
		pv.stop();
		pvMetrics.setEnable(false);
	}



	/** @return Most recent value of the channel's PV */
	final public DBRTimeEvent getCurrentArchivedValue() {
		synchronized (this) {
			return latestDBRTimeEvent;
		}
	}

	/** @return Count of received values */
	synchronized public long getReceivedValues() {
		return received_value_count;
	}



	/** @return Last value written to archive */
	final public DBRTimeEvent getLastArchivedValue() {
		synchronized (this) {
			return lastDBRTimeEvent;
		}
	}

	/** @return Sample buffer */
	final public SampleBuffer getSampleBuffer() {
		return buffer;
	}



	/** Reset counters */
	public void reset() {
		synchronized (this) {
			received_value_count = 0;
		}
	}



	/**
	 * Enable or disable groups based on received value
	 * 
	 * @param temptimeevent DBRTimeEvent
	 * @throws Exception  &emsp;
	 */
	final private void handleEnablement(final DBRTimeEvent temptimeevent) throws Exception {
		if (enablement == Enablement.Passive)
			throw new Exception("Not to be called when passive");

		SampleValue sampleValue = temptimeevent.getSampleValue();
		final double number = ValueUtil.getDouble(sampleValue);
		final boolean yes = number > 0.0;

		// Do we enable or disable based on that value?
		final boolean enable = enablement == Enablement.Enabling ? yes : !yes;
		try {
			if (enable)
				updateEnabledState(true);
		} catch (Exception e) {
			logger.error("exception in handleEnablement", e);
		}
	}

	/**
	 * Called for each value received from PV.
	 * <p>
	 * Base class remembers the <code>most_recent_value</code>, and asserts that
	 * one 'first' sample is archived. Derived class <b>must</b> call
	 * <code>super()</code>.
	 * 
	 * @param timeevent DBRTimeEvent
	 * @return true if the value was already written because it's the first value after startup or error, so there's no need to write that sample again.
	 * @throws Exception  &emsp; 
	 */
	protected boolean handleNewValue(final DBRTimeEvent timeevent) throws Exception {
		synchronized (this) {
			latestDBRTimeEvent = timeevent;
		}

		if (!enabled)
			return false;

		// Did we recover from write errors?
		if (need_write_error_sample && SampleBuffer.isInErrorState() == false) {
			need_write_error_sample = false;
			need_first_sample = true;
		}

		if (!need_first_sample)
			return false;

		need_first_sample = false;
		addValueToBuffer(timeevent);
		return true;
	}

	
	/**
	 * Start the metachannels
	 * <p>
	 * @throws Exception &emsp; 
	 */
	public void startUpMetaChannels() throws Exception {
		logger.debug("Starting up monitors on the fields for pv " + name);
		for(PV metaPV : metaPVs.values()) { 
			metaPV.stop();
			metaPV.start();
		}
		logger.debug("Done starting up monitors on the fields for pv " + this.name);
	}

	public void shutdownMetaChannels() throws Exception {
		logger.debug("Shutting down monitors on the fields for pv " + this.name);
		for(PV metaPV : metaPVs.values()) { 
			metaPV.stop();
		}
		logger.debug("Done shutting down monitors on the fields for pv " + this.name);
	}



	/**
	 * Handle a disconnect event.
	 * <p>
	 * Base class clears the <code>most_recent_value</code> and adds a
	 * 'disconnected' info sample. Subclasses may override, but must call
	 * <code>super()</code>.
	 * 
	 * @throws Exception &emsp; 
	 */
	protected void handleDisconnected() throws Exception {
		synchronized (this) {
			latestDBRTimeEvent = null;
		}
		need_first_sample = true;

		shutdownMetaChannels();
	}


	/**
	 * Add given sample to buffer, performing a back-in-time check, updating the
	 * sample buffer error state.
	 * 
	 * @param timeevent DBRTimeEvent
	 * @return <code>false</code> if value failed back-in-time or future check, <code>true</code> if value was added.
	 * @throws Exception &emsp; 
	 */
	final protected boolean addValueToBuffer(final DBRTimeEvent timeevent) throws Exception {
		// TODO See if this is repeated information.
		this.pvMetrics.setElementCount(timeevent.getSampleValue().getElementCount());
		this.pvMetrics.setSecondsOfLastEvent(System.currentTimeMillis() / 1000);

		try { 
			if (isfutureorpastOrSame(timeevent)) {
				if(!isSameTimeStamp(timeevent)) {
					this.pvMetrics.addTimestampWrongEventCount(timeevent.getEventTimeStamp());
				} else { 
					// Don't incremement the TimestampWrongEventCount count if we get the same timestamp
					// This happens because of our SCAN implementation which picks off the latest value every sampling period.
					// In cases where the PV does not change, we get the same event over and over again and this incorrectly shows up in the report as missing data.
					// We also reset any info on connection loss because as far as we can determine, the PV has not processed since the last event we picked up.
					this.pvMetrics.resetConnectionLastLostEpochSeconds();
				}

				return false;
			}
		} catch(IllegalArgumentException ex) {
			// If the nanos part of the TS is incorrect, Java throws an IllegalArgumentException. We count that as a sample lost to an incorrect timestamp.
			Timestamp incorrectTs = TimeUtils.convertFromEpochSeconds(timeevent.getEpochSeconds(), 0);
			this.pvMetrics.addTimestampWrongEventCount(incorrectTs);
			return false;
		}

		this.pvMetrics.setLastEventFromIOCTimeStamp(timeevent.getEventTimeStamp());
		synchronized (this) {
			this.lastDBRTimeEvent = timeevent;
			this.last_archived_timestamp = lastDBRTimeEvent.getEventTimeStamp();
		}
		this.pvMetrics.addConnectionLostRegainedFields(timeevent);

		boolean incrementEventCounts = buffer.add(timeevent);
		if(incrementEventCounts) {
			this.pvMetrics.addEventCounts();
			this.pvMetrics.addStorageSize(timeevent);
			this.pv.sampleWrittenIntoStores();
		}

		if (SampleBuffer.isInErrorState())
			need_write_error_sample = true;
		
		return true;
	}



	/**
	 * Update the enablement state in case of change
	 * 
	 * @param new_enabled_state  &emsp;
	 * @throws Exception  &emsp;
	 */

	final private void updateEnabledState(final boolean new_enabled_state) throws Exception {
		// Any change?
		if (new_enabled_state == enabled)
			return;

		enabled = new_enabled_state;

		// In case this arrived after shutdown, don't log it.
		if (!is_running)
			return;
	}



	@Override
	public String toString() {
		return "Channel " + getName() + ", " + getMechanism();
	}



	private static final Timestamp PAST_CUTOFF_TIMESTAMP = TimeUtils.convertFromISO8601String("1991-01-01T00:00:00.000Z");

	private static final int FUTURE_CUTOFF_SECONDS = 30*60;

	/**
	 * Judge whether the time stamp for this time event is too old or far in the future.
	 * Increment appropriate counters in PVMetrics if this is the case.
	 * @param timeevent  DBRTimeEvent
	 * @return true when the time stamp of this timeevent is too old or far in the future. otherwise, false
	 */

	private boolean isfutureorpastOrSame(final DBRTimeEvent timeevent) {
		Timestamp currentEventTimeStamp = timeevent.getEventTimeStamp();

		if(currentEventTimeStamp.before(PAST_CUTOFF_TIMESTAMP)) {
			trouble_sample_log.log(getName() + ":" + " timestamp is too far in the past " + TimeUtils.convertToHumanReadableString(currentEventTimeStamp));
			return true;
		}

		Timestamp futureCutOffTimeStamp = TimeUtils.convertFromEpochSeconds(TimeUtils.getCurrentEpochSeconds() + FUTURE_CUTOFF_SECONDS, 0);
		if(currentEventTimeStamp.after(futureCutOffTimeStamp)) {
			trouble_sample_log.log(getName() + ":" + " record processing timestamp " + TimeUtils.convertToHumanReadableString(currentEventTimeStamp) + " is after the future cutoff " + TimeUtils.convertToHumanReadableString(futureCutOffTimeStamp));
			return true;
		}

		if (last_archived_timestamp != null) {
			Timestamp lastEventTimeStamp =  last_archived_timestamp;
			if(currentEventTimeStamp.before(lastEventTimeStamp)) {
				trouble_sample_log.log(getName() + ":" + " record processing timestamp " + TimeUtils.convertToHumanReadableString(currentEventTimeStamp) + " is before the previous event's timestamp " + TimeUtils.convertToHumanReadableString(lastEventTimeStamp));
				return true;
			} else if(currentEventTimeStamp.equals(lastEventTimeStamp)){
				trouble_sample_log.log(getName() + ":" + " record processing timestamp " + TimeUtils.convertToHumanReadableString(currentEventTimeStamp) + " is the same as  the previous event's timestamp " + TimeUtils.convertToHumanReadableString(lastEventTimeStamp));
				return true;
			}
		}
		return false;
	}



	/**
	 * Return true if this event has the same timestamp as the last recorded event 
	 * @param timeevent DBRTimeEvent
	 * @return boolean True or False
	 */
	private boolean isSameTimeStamp(final DBRTimeEvent timeevent) {
		Timestamp currentEventTimeStamp = timeevent.getEventTimeStamp();
		if (last_archived_timestamp != null) {
			Timestamp lastEventTimeStamp =  last_archived_timestamp;
			if(currentEventTimeStamp.equals(lastEventTimeStamp)){
				return true;
			}
		}

		return false;
	}


	/**
	 * Get the current value of all the meta fields. 
	 * @return - Can return null if this PV has no meta fields.
	 */
	public HashMap<String, String> getCurrentCopyOfMetaFields() { 
		if(this.pv != null) return pv.getLatestMetadata();
		return null;
	}


	public String getHostName() { 
		return pv.getHostName();
	}


	public void getLowLevelChannelStateInfo(List<Map<String, String>> statuses) { 
		this.pv.getLowLevelChannelInfo(statuses);
	}
	
	/**
	 * Get the archive channel for a particular metachannel.
	 * @param metaFieldName  &emsp;
	 * @return boolean True or False 
	 */
	public boolean isMetaPVConnected(String metaFieldName) {
		PV metaPV = metaPVs.get(metaFieldName);
		if(metaPV != null) { 
			return metaPV.isConnected();
		}
		return false;
	}
	
	/**
	 * Get the field names for which we have established channels.
	 * @return String the Meta PV names
	 */
	public Set<String> getMetaPVNames() {
		return metaPVs.keySet();
	}
	
	/**
	 * Number of field names for which we have established channels.
	 * @return int  &emsp;
	 */
	public int getMetaChannelCount() { 
		return metaPVs.size();
	}
	
	/**
	 * Get the number of connected field channels
	 * @return int  &emsp;
	 */
	public int getConnectedMetaChannelCount() { 
		int connectedMetaFieldCount = 0;
		for(PV metaPV : metaPVs.values()) { 
			if(metaPV.isConnected()) { 
				connectedMetaFieldCount++;
			}
		}
		return connectedMetaFieldCount;
	}
	
	
	/**
	 * Do any of the meta channels in this PV need starting up?
	 * @return boolean True or False 
	 */
	public boolean metaChannelsNeedStartingUp() {
		for(PV metaPV : metaPVs.values()) {
			logger.debug(metaPV.getName() + " connected is " + metaPV.isConnected());
			if(!metaPV.isConnected()) { 
				return true;
			}
		}
		
		return false;
	}

	/**
	 * Return the amount of time (in seconds) since we asked CAJ/JCA to connect to this channel.
	 * @return long  &emsp;
	 */
	public long getSecondsElapsedSinceSearchRequest() {
		if(this.is_running && need_first_sample) { 
			return TimeUtils.getCurrentEpochSeconds() - this.pvMetrics.getConnectionRequestMadeEpochSeconds();
		} else { 
			return -1;
		}
	}

	/**
	 * @return int the jCACommandThreadID
	 */
	public int getJCACommandThreadID() {
		return JCACommandThreadID;
	}

	/**
	 * @param jCACommandThreadID the jCACommandThreadID to set
	 */
	public void setJCACommandThreadID(int jCACommandThreadID) {
		JCACommandThreadID = jCACommandThreadID;
	}
	
	
	/**
	 * Use this method to do a caget on the metadata..
	 * @param context EngineContext
	 */
	public void updateMetadataOnceADay(EngineContext context) { 
		if(this.pv != null) { 
			context.getJCACommandThread(ArchiveChannel.this.JCACommandThreadID).addCommand(new Runnable() {
				@Override
				public void run() {
					try { 
						ArchiveChannel.this.pv.updateTotalMetaInfo();
					} catch(Throwable t) { 
						logger.error("Exception issuing request to update total meta Info for pv " + ArchiveChannel.this.name, t);
					}
				}
			});
		}
	}
	
	/**
	 * Combine the metadata from various sources and return the latest copy.
	 * @return HashMap  &emsp;
	 */
	public HashMap<String, String> getLatestMetadata() { 
		HashMap<String, String> retVal = new HashMap<String, String>();
		if(this.pv != null) {
			return this.pv.getLatestMetadata();
		}
		return retVal;
	}	
	
	public void aboutToWriteBuffer(DBRTimeEvent lastSample) {
		if(this.pv != null) {
			this.pv.aboutToWriteBuffer(lastSample);
		}
	}
}
