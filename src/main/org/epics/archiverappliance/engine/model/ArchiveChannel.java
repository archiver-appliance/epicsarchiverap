/*******************************************************************************

 * Copyright (c) 2010 Oak Ridge National Laboratory.

 * All rights reserved. This program and the accompanying materials

 * are made available under the terms of the Eclipse Public License v1.0

 * which accompanies this distribution, and is available at

 * http://www.eclipse.org/legal/epl-v10.html

 ******************************************************************************/
package org.epics.archiverappliance.engine.model;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.pv.EPICS_V3_PV;
import org.epics.archiverappliance.engine.pv.PV;
import org.epics.archiverappliance.engine.pv.PVListener;
import org.epics.archiverappliance.engine.pv.PVMetrics;
/**
 * Base for archived channels.
 * @author Kay Kasemir
 * @version Initial version:CSS
 * @version 4-Jun-2012, Luofeng Li:added codes to support for the new archiver
 */

@SuppressWarnings("nls")
abstract public class ArchiveChannel {
	private static final Logger logger = Logger.getLogger(ArchiveChannel.class);

	/** Throttled log for NaN samples */
	private static ThrottledLogger trouble_sample_log = new ThrottledLogger(LogLevel.info, 60);

	/**
	 * Channel name. This is the name by which the channel was created, not the PV name that might include decorations.
	 */
	final private String name;

	/** This is the actual control system PV. */
	final private EPICS_V3_PV pv;

	/**
	 * The name of the PV that control archiving of this PV.
	 */
	final private String controlPVname;

	/**
	 * Pointer to the config service.
	 */
	final private ConfigService configService;

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
	 * is this channel created for one meta field ,such as HIHI,LOLO?
	 */
	private boolean isMetaField = false;
	
	/**
	 * ArchiveChannels for the meta information monitors.
	 */
	private ConcurrentHashMap<String, ArchiveChannel> metaChannels = new ConcurrentHashMap<String, ArchiveChannel>();

	/**
	 * Does this channnel have metafields.
	 */
	private boolean hasMetaField=false;

	
	/**
	 * The engine can use multiple contexts and can assign PV's to contexts based on some algorithm.
	 * 
	 */
	private int JCACommandThreadID = -1;
	

	/**
	 * 
	 * @return true when this channel was created for a meta field .Otherwise, false 
	 */
	public boolean isMetaField() {
		return isMetaField;
	}

	public boolean hasMetaField(){
		return this.hasMetaField;
	}


	/**
	 * this channel has meta field or not 
	 * @param hasMetaField  true or false
	 */
	public void setHasMetaField(boolean hasMetaField) {
		this.hasMetaField=hasMetaField;
		this.pv.setHasMetaField(hasMetaField);
	}

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
	 * if this channel (PV.HIHI)  is  created for the meta field(such as HIHI), set the channel created for this meta field'pv (PV)
	 * and this method is used for meta field archiving
	 * @param channel  for PV
	 */
	public void setPVChannelWhereThisMetaFieldIn(ArchiveChannel channel) {
		this.pv.setParentChannel(channel);
		channel.addMetaChannel(PVNames.getFieldName(this.name), this);
	}

	public void setPVChannelWhereThisMetaFieldIn(ArchiveChannel channel, boolean isRuntimeOnly) {
		this.pv.setParentChannel(channel, isRuntimeOnly);
		channel.addMetaChannel(PVNames.getFieldName(this.name), this);
	}

	/**
	 * update the meta field value and this is used for meta filed archiving
	 * @param pvname
	 * @param fieldValue
	 */
	public void updataMetaField(final String pvname, final String fieldValue) {
		this.pv.updataMetaField(pvname, fieldValue);
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
		this.pvMetrics.setConnectionFirstEstablishedEpochSeconds(this.pv.getConnectionFirstEstablishedEpochSeconds());
		this.pvMetrics.setConnectionLastRestablishedEpochSeconds(this.pv.getConnectionLastRestablishedEpochSeconds());
		this.pvMetrics.setConnectionLossRegainCount(this.pv.getConnectionLossRegainCount());
		this.pvMetrics.setConnectionRequestMadeEpochSeconds(this.pv.getConnectionRequestMadeEpochSeconds());
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
	 * @param isMetaField this pv is a meta field or not (pv.HIHI or just pv)
	 * @param commandThreadID - this is the index into the array of JCA command threads that processes this context.
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
			final boolean isMetaField, 
			final int commandThreadID)
					throws Exception {
		this.name = name;
		this.controlPVname = controlPVname;
		this.writer = writer;
		this.enablement = enablement;
		this.last_archived_timestamp = last_archived_timestamp;
		this.pvMetrics = new PVMetrics(name, controlPVname, System.currentTimeMillis() / 1000, archdbrtype);
		this.isMetaField = isMetaField;
		this.configService = configservice;
		if (!isMetaField) {
			this.buffer = new SampleBuffer(name, buffer_capacity, archdbrtype,this.pvMetrics);
		}
		this.JCACommandThreadID =  commandThreadID;

		this.pv = new EPICS_V3_PV(name, configservice, false, archdbrtype, this.pvMetrics, commandThreadID);

		pv.addListener(new PVListener() {
			@Override
			public void pvValueUpdate(final PV pv) {
				// PV already suppresses updates after 'stop', but check anyway
				if (is_running) {
					try {
						final DBRTimeEvent temptimeevent = pv.getDBRTimeEvent();
						if (isMetaField)
							return;

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
		});
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

	/** @return Human-readable info on internal state of PV */
	public String getInternalState() {
		return pv.getStateInfo();
	}

	/** Start archiving this channel. */
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
	 * @throws Exception
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
		buffer.reset();
		synchronized (this) {
			received_value_count = 0;
		}
	}



	/**
	 * Enable or disable groups based on received value
	 * 
	 * @throws Exception
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
	 * @param value Value received from PV
	 * @return true if the value was already written because it's the first value after startup or error, so there's no need to write that sample again.
	 * @throws Exception
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
	 * @throws Exception
	 */
	public void startUpMetaChannels() throws Exception {
		logger.debug("Starting up monitors on the fields for pv " + name);
		for(ArchiveChannel metaChannel : metaChannels.values()) { 
			metaChannel.stop();
			metaChannel.start();
		}
		logger.debug("Done starting down monitors on the fields for pv " + this.name);
	}

	public void shutdownMetaChannels() throws Exception {
		logger.debug("Shutting down monitors on the fields for pv " + this.name);
		for(ArchiveChannel metaChannel : this.metaChannels.values()) { 
			metaChannel.stop();
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
	 * @throws Exception
	 */
	protected void handleDisconnected() throws Exception {
		synchronized (this) {
			latestDBRTimeEvent = null;
		}
		need_first_sample = true;

		if(!this.isMetaField) {
			// We also shut off all the meta fields and runtime fields in an attempt to make reconnect times for the main PVs faster...
			shutdownMetaChannels();
		}
	}


	/**
	 * Add given sample to buffer, performing a back-in-time check, updating the
	 * sample buffer error state.
	 * 
	 * @param value Value to archive
	 * @return <code>false</code> if value failed back-in-time or future check, <code>true</code> if value was added.
	 * @throws Exception
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
					this.pv.resetConnectionLastLostEpochSeconds();
				}

				return false;
			}
		} catch(IllegalArgumentException ex) {
			// If the nanos part of the TS is incorrect, Java throws an IllegalArgumentException. We count that as a sample lost to an incorrect timestamp. 
			this.pvMetrics.addTimestampWrongEventCount(timeevent.getEventTimeStamp());
			return false;
		}

		this.pvMetrics.setLastEventFromIOCTimeStamp(timeevent.getEventTimeStamp());
		synchronized (this) {
			this.lastDBRTimeEvent = timeevent;
			this.last_archived_timestamp = lastDBRTimeEvent.getEventTimeStamp();
		}
		this.pv.addConnectionLostRegainedFields(timeevent);

		boolean incrementEventCounts = buffer.add(timeevent);
		if(incrementEventCounts) {
			this.pvMetrics.addEventCounts();
			this.pvMetrics.addStorageSize(timeevent);
		}

		if (SampleBuffer.isInErrorState())
			need_write_error_sample = true;
		
		if(timeevent.hasFieldValues() && !timeevent.isActualChange()) { 
			// We have some field values and this is not an actual change.
			// This typically gets written by saveMetaDataOnceEveryDay in EPICS_V3_PV once a day.
			// Trying to schedule an update of the metadata and runtime fields here...
			logger.debug("Scheduling an update of the runtime fields and metadata in about 23 hours.");
			scheduleUpdateOfMetadataAndRuntimeFields();
		}

		return true;
	}



	/**
	 * Update the enablement state in case of change
	 * 
	 * @throws Exception
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
	 * @param timeevent 
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
	 * @param timeevent
	 * @return
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
		if(this.pv != null) return pv.getCurrentCopyOfMetaFields();
		return null;
	}


	public String getHostName() { 
		return pv.getHostName();
	}



	public String getLowLevelChannelStateInfo() { 
		return this.pv.getLowLevelChannelInfo();
	}
	
	private void scheduleUpdateOfMetadataAndRuntimeFields() { 
		Random rand = new Random();
		int randDelayInMinutes = 12*60 + rand.nextInt(12*60);
		logger.debug("Scheduling the update of metadata and runtime fields in " + randDelayInMinutes + "(mins) at " + TimeUtils.convertToHumanReadableString(System.currentTimeMillis()/1000 + randDelayInMinutes*60));
		this.configService.getEngineContext().getScheduler().schedule(new Runnable() {
			@Override
			public void run() {
				try { 
					if(ArchiveChannel.this.pv != null) { 
						ArchiveChannel.this.configService.getEngineContext().getJCACommandThread(ArchiveChannel.this.JCACommandThreadID).addCommand(new Runnable() {
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
				} catch(Throwable t) { 
					logger.error("Exception issuing request to update total meta Info for pv " + ArchiveChannel.this.name, t);
				}
			}
		}, randDelayInMinutes, TimeUnit.MINUTES);

	}
	
	/**
	 * Get the archive channels for the meta channels - this should include both runtime and otherwise
	 * @return
	 */
	public Collection<ArchiveChannel> getMetaChannels() { 
		return metaChannels.values();
	}
	
	/**
	 * Get the archive channel for a particular metachannel.
	 * @param metaFieldName
	 * @return
	 */
	public ArchiveChannel getMetaChannel(String metaFieldName) {
		return metaChannels.get(metaFieldName);
	}
	
	/**
	 * Get the field names for which we have established channels.
	 * @return
	 */
	public Set<String> getMetaChannelNames() {
		return metaChannels.keySet();
	}
	
	/**
	 * Number of field names for which we have established channels.
	 * @return
	 */
	public int getMetaChannelCount() { 
		return metaChannels.size();
	}
	
	/**
	 * Get the number of connected field channels
	 * @return
	 */
	public int getConnectedMetaChannelCount() { 
		int connectedMetaFieldCount = 0;
		for(ArchiveChannel metaChannel : getMetaChannels()) { 
			if(metaChannel.isConnected()) { 
				connectedMetaFieldCount++;
			}
		}
		return connectedMetaFieldCount;
	}
	
	
	/**
	 * Do any of the meta channels in this PV need starting up?
	 * @return
	 */
	public boolean metaChannelsNeedStartingUp() {
		for(ArchiveChannel metaChannel : getMetaChannels()) {
			logger.debug(metaChannel.getName() + " connected is " + metaChannel.isConnected());
			if(!metaChannel.isConnected()) { 
				return true;
			}
		}
		
		return false;
	}

	/**
	 * Register a field channel with its parent PV.
	 * @param metaFieldName
	 * @param metaChannel
	 */
	public void addMetaChannel(String metaFieldName, ArchiveChannel metaChannel) { 
		if(metaChannels.containsKey(metaFieldName)) { 
			logger.error("Channel for field " + metaFieldName + " for pv " + this.name + " already exists. Replacing it but this is not optimal");
		}
		metaChannels.put(metaFieldName, metaChannel);
	}
	
	
	/**
	 * Return the amount of time (in seconds) since we asked CAJ/JCA to connect to this channel.
	 * @return
	 */
	public long getSecondsElapsedSinceSearchRequest() {
		if(this.is_running && need_first_sample) { 
			return TimeUtils.getCurrentEpochSeconds() - this.pv.getConnectionRequestMadeEpochSeconds();
		} else { 
			return -1;
		}
	}

	/**
	 * @return the jCACommandThreadID
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
	
	
}