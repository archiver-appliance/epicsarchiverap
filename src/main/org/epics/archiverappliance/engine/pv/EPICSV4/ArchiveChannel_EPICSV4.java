package org.epics.archiverappliance.engine.pv.EPICSV4;

import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.Enablement;
import org.epics.archiverappliance.engine.model.LogLevel;
import org.epics.archiverappliance.engine.model.SampleBuffer;
import org.epics.archiverappliance.engine.model.ThrottledLogger;
import org.epics.archiverappliance.engine.model.ValueUtil;
import org.epics.archiverappliance.engine.pv.PV;
import org.epics.archiverappliance.engine.pv.PVListener;
import org.epics.archiverappliance.engine.pv.PVMetrics;

/**
 * Base for archived channels.
 */
@SuppressWarnings("nls")
abstract public class ArchiveChannel_EPICSV4 {
	/** Throttled log for NaN samples */
	private static ThrottledLogger trouble_sample_log = new ThrottledLogger(LogLevel.info, 60); //$NON-NLS-1$

	private static final Logger logger = Logger.getLogger(ArchiveChannel_EPICSV4.class);
	/**
	 * Channel name. This is the name by which the channel was created, not the
	 * PV name that might include decorations.
	 */
	final private String name;

	/** Control system PV */
	final private PV pv;

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

	/** Counter for received values (monitor updates) */
	private long received_value_count = 0;

	/**
	 * Last value in the archive, i.e. the one most recently written.
	 * <p>
	 * SYNC: Lock on <code>this</code> for access.
	 */

	/** Buffer of received samples, periodically written */
	private final SampleBuffer buffer;
	protected DBRTimeEvent latestDBRTimeEvent;
	protected DBRTimeEvent lastDBRTimeEvent = null;
	final protected PVMetrics pvMetrics;

	public void setlastRotateLogsEpochSeconds(long lastRotateLogsEpochSecond) {
		this.pvMetrics.setLastRotateLogsEpochSeconds(lastRotateLogsEpochSecond);
	}

	public PVMetrics getPVMetrics() {

		// final Timestamp time = timeevent.getEventTimeStamp();
		this.pvMetrics.setConnected(this.pv.isConnected());
		this.pvMetrics.setArchving(this.pv.isRunning());
		this.pvMetrics.setConnectionFirstEstablishedEpochSeconds(this.pv
				.getConnectionFirstEstablishedEpochSeconds());
		this.pvMetrics.setConnectionLastRestablishedEpochSeconds(this.pv
				.getConnectionLastRestablishedEpochSeconds());
		this.pvMetrics.setConnectionLossRegainCount(this.pv
				.getConnectionLossRegainCount());
		this.pvMetrics.setConnectionRequestMadeEpochSeconds(this.pv
				.getConnectionRequestMadeEpochSeconds());
		return pvMetrics;
	}

	public ArchiveChannel_EPICSV4(final String name,
			final Enablement enablement, final int buffer_capacity,
			final DBRTimeEvent last_archived_value,
			final ConfigService configservice, final ArchDBRTypes archdbrtype)
			throws Exception {
		this.name = name;
		this.enablement = enablement;
		this.lastDBRTimeEvent = last_archived_value;
		this.pvMetrics = new PVMetrics(name, null, System.currentTimeMillis() / 1000, archdbrtype);
		this.buffer = new SampleBuffer(name, buffer_capacity, archdbrtype,
				pvMetrics);

		pv = PVFactory_EPICSV4.createEPICSPV(name, configservice);
		pv.addListener(new PVListener() {
			@Override
			public void pvValueUpdate(final PV pv) {
				// PV already suppresses updates after 'stop', but check anyway
				if (is_running) {

					try {
						final DBRTimeEvent temptimeevent = pv.getDBRTimeEvent();
						if (enablement != Enablement.Passive)
							handleEnablement(temptimeevent);
						handleNewValue(temptimeevent);
					} catch (Exception e) {
						logger.error("exception in pvValueUpdate", e);

					}
				}
			}

			@Override
			public void pvDisconnected(final PV pv) {
				try {
					if (is_running)

						handleDisconnected();
				} catch (Exception e) {
					logger.error("exception in pvDisconnected", e);
				}
			}

			@Override
			public void pvConnected(PV pv) {
				//

				pvMetrics.setConnectionEstablishedEpochSeconds(System
						.currentTimeMillis() / 1000);
			}
		});
	}

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
	final private void handleEnablement(final DBRTimeEvent temptimeevent)
			throws Exception {
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
	 * @param value
	 *            Value received from PV
	 * 
	 * @return true if the value was already written because it's the first
	 *         value after startup or error, so there's no need to write that
	 *         sample again.
	 * @throws Exception
	 */
	protected boolean handleNewValue(final DBRTimeEvent timeevent)
			throws Exception {
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
	}

	/**
	 * Add given sample to buffer, performing a back-in-time check, updating the
	 * sample buffer error state.
	 * 
	 * @param value
	 *            Value to archive
	 * @return <code>false</code> if value failed back-in-time or future check,
	 *         <code>true</code> if value was added.
	 * @throws Exception
	 */
	final protected boolean addValueToBuffer(final DBRTimeEvent timeevent)
			throws Exception {

		this.pvMetrics.setElementCount(timeevent.getSampleValue()
				.getElementCount());
		this.pvMetrics.setSecondsOfLastEvent(System.currentTimeMillis() / 1000);
		this.pvMetrics.addEventCounts();
		this.pvMetrics.addStorageSize(timeevent);

		if (isfutureorpast(timeevent))
			return false;

		synchronized (this) {

			// else ...
			lastDBRTimeEvent = timeevent;
		}

		buffer.add(timeevent);

		if (SampleBuffer.isInErrorState())
			need_write_error_sample = true;
		return true;
	}

	/**
	 * Update the enablement state in case of change
	 * 
	 * @throws Exception
	 */
	final private void updateEnabledState(final boolean new_enabled_state)
			throws Exception {
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

	private boolean isfutureorpast(final DBRTimeEvent timeevent) {
		Timestamp timeTemp = timeevent.getEventTimeStamp();
		@SuppressWarnings("deprecation")
		int year = timeTemp.getYear() + 1900;
		long lastSecond = 0;
		if (lastDBRTimeEvent != null) {
			lastSecond = lastDBRTimeEvent.getEpochSeconds();
		}

		long seconds = timeevent.getEpochSeconds();
		long futrueseconds = TimeUtils.getCurrentEpochSeconds() + 30 * 60;

		if ((year < 1991) || (seconds > futrueseconds)
				|| (seconds <= lastSecond)) {

			trouble_sample_log
					.log(getName()
							+ ":"
							+ " timestamp is far future or past or equal to sample last sample time:"
							+ timeevent);

			return true;
		}

		return false;
	}
}
