/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.pv;

import gov.aps.jca.CAException;
import gov.aps.jca.Channel;
import gov.aps.jca.Channel.ConnectionState;
import gov.aps.jca.Monitor;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.DBR_String;
import gov.aps.jca.event.ConnectionEvent;
import gov.aps.jca.event.ConnectionListener;
import gov.aps.jca.event.GetEvent;
import gov.aps.jca.event.GetListener;
import gov.aps.jca.event.MonitorEvent;
import gov.aps.jca.event.MonitorListener;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.JCA2ArchDBRType;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;

import com.cosylab.epics.caj.CAJChannel;

/**
 * EPICS ChannelAccess implementation of the PV interface.
 * 
 * @see PV
 * @author Kay Kasemir
 * @version Initial version:CSS
 * @version 4-Jun-2012, Luofeng Li:added codes to support for the new archiver
 */
@SuppressWarnings("nls")
public class EPICS_V3_PV implements PV, ConnectionListener, MonitorListener {
	private static final Logger logger = Logger.getLogger(EPICS_V3_PV.class.getName());
	
	/**
	 * Use plain mode?
	 * @see #EPICS_V3_PV(String, boolean)
	 */
	final private boolean plain;

	/**
	 * indicates some actions after pv is connected
	 */
	private boolean isFirstDataAfterConnection=false;
	
	/** Channel name. */
	final private String name;
	
	/**the meta info for this pv*/
	private MetaInfo totalMetaInfo = new MetaInfo();
	
	/**
	 * If this pv is a meta field and parentChannel is the channel of this pv without the meta field.
	 * For example if this pv is "pv1.HIHI" ,Thus this parentChannel is the channel of"pv1".
	 **/
	private ArchiveChannel parentChannel = null;
	
	/**
	 * If this pv has many meta fields archived, allarchiveFieldsData includes the meta field names and their values.
	 * allarchiveFieldsData is updated when meta field changes
	 * if this pv doesn't have meta field archived, this  is always  null.
	 */
	private ConcurrentHashMap<String, String> allarchiveFieldsData = null;
	
	/** Runtime fields that are not archived/stored are stored here */
	private ConcurrentHashMap<String, String> runTimeFieldsData = new ConcurrentHashMap<String, String>();
	
	/** if this pv has many meta fields archived,changedarchiveFieldsData includes the changed meta values and the field names*/
	private ConcurrentHashMap<String, String> changedarchiveFieldsData = null;
	
	/**we save all meta field once every day and lastTimeStampWhenSavingarchiveFields is when we save all last meta fields*/
	private Calendar lastTimeStampWhenSavingarchiveFields = null;
	
	/**this pv is meta field  or not*/
	private boolean isarchiveFieldsField = false;
	
	/** Store the value for this only in the runtime and not into the stores...*/
	private boolean isruntimeFieldField = false;
	
	/**the pv.RTYP this is used to determine which meta fields of this pv are  archived*/
	private String reacordTypeName = null;
	
	/**
    * get the RTYP of this pv
    * @return record type name
    */
	public String getReacordTypeName() {
		return reacordTypeName;
	}
	
	private enum State {
		/** Nothing happened, yet */
		Idle,
		/** Trying to connect */
		Connecting,
		/** Got basic connection */
		Connected,
		/** Requested MetaData */
		GettingMetadata,
		/** Received MetaData */
		GotMetaData,
		/** Subscribing to receive value updates */
		Subscribing,
		/**
		 * Received Value Updates
		 * <p>
		 * This is the ultimate state!
		 */
		GotMonitor,
		/** Got disconnected */
		Disconnected
	}
    
	private State state = State.Idle;
	
	/**
	 *  Sourced from org/csstudio/platform/libs/epics/EpicsPlugin.java 
	 *  @author Original author unknown
	 *  @author Kay Kasemir
	 *  @author Sergei Chevtsov
	 */
	public enum MonitorMask {
		/** Listen to changes in value beyond 'MDEL' threshold or alarm state*/
		VALUE(1 | 4),

		/** Listen to changes in value beyond 'ADEL' archive limit */
		ARCHIVE(2),

		/** Listen to changes in alarm state */
		ALARM(4);

		final private int mask;

		private MonitorMask(final int mask) {
			this.mask = mask;
		}

		/** @return Mask bits used in underlying CA call */
		public int getMask() {
			return mask;
		}
	}


	/**configservice used by this pv*/
	final private ConfigService configservice;
	
	/** PVListeners of this PV */
	final private CopyOnWriteArrayList<PVListener> listeners = new CopyOnWriteArrayList<PVListener>();
	
	/** JCA channel. LOCK <code>this</code> on change. */
	private RefCountedChannel channel_ref = null;
	
	/**
	 * Either <code>null</code>, or the subscription identifier. LOCK
	 * <code>this</code> on change
	 */
	private Monitor subscription = null;
	
	/**
	 * isConnected? <code>true</code> if we are currently connected (based on
	 * the most recent connection callback).
	 * <p>
	 * EPICS_V3_PV also runs notifyAll() on <code>this</code> whenever the
	 * connected flag changes to <code>true</code>.
	 */
	private volatile boolean connected = false;
	
	/**
	 * isRunning? <code>true</code> if we want to receive value updates.
	 */
	private volatile boolean running = false;
	
	/**the DBRTimeEvent constructor for this pv*/
	private Constructor<? extends DBRTimeEvent> con;
	
	/**the current DBRTimeEvent*/
	private DBRTimeEvent dbrtimeevent;
	
	/**the ArchDBRTypes of this pv*/
	private ArchDBRTypes archDBRType = null;
	
	/**
	 * The pvMetrics for the channel for this PV.
	 * This could be null so make sure we always check for null when updating metrics.
	 */
	private PVMetrics pvMetrics;
	
	/**
	 * The JCA command thread that processes actions for this PV.
	 * This should be inherited from the ArchiveChannel.
	 */
	private int jcaCommandThreadId;
	
	/**
	 * The time when the first connections was established
	 * it is the the number of seconds since 1970/01/01
	 **/
	private long connectionFirstEstablishedEpochSeconds;
	
	/**
	 * The time when the last connections was reestablished
	 * it is the the number of seconds since 1970/01/01
	 **/
	private long connectionLastRestablishedEpochSeconds;

	/**
	 * When did we last lose the connection? This is initialized to when this PV object was created.
	 */
	private long connectionLastLostEpochSeconds = System.currentTimeMillis()/1000;
	
	/**the count of the connection lost and regained*/
	private long connectionLossRegainCount;
	
	/**is this the fist connection?*/
	private boolean firstTimeConnection = true;
	
	/**is this the fist data after startup?*/
	private boolean firstDataAfterStartUp = true;
	
	/**
	 * When the last connection was established
	 * it is the the number of seconds since 1970/01/01
	 **/
	private long connectionEstablishedEpochSeconds;

	/**
	 * When sending the connection request
	 * it is the the number of seconds since 1970/01/01
	 **/
	private long connectionRequestMadeEpochSeconds;
	
	/**
	 * The pvs' list who are controlled by this pv to stop or start archiving
	 **/
	private ArrayList<String> controlledPVList = null;

	/**
	 * The current status of all pvs who are controlled by this pv.
	 * if true, all pvs are archiving.
	 * else , all pvs are not
	 **/
	private boolean enableAllPV = true;

	/**Does this pv have one meta field archived?*/
	private boolean hasMetaField = false;

	/**
	 * the ioc host name where this pv is 
	 */
	private String hostName;
    
	@Override
	public String getHostName(){
		return hostName;
	}

	/**
	 * 
	 * @return the status of all pvs controlled by this pv
	 */
	public boolean isEnableAllPV() {
		return enableAllPV;
	}
    
	/**
     * @see PV#getConnectionRequestMadeEpochSeconds()
     */
	@Override
	public long getConnectionRequestMadeEpochSeconds() {
		return connectionRequestMadeEpochSeconds;
	}

	/**
	 * @see PV#getConnectionEstablishedEpochSeconds()
	 */
	@Override
	public long getConnectionEstablishedEpochSeconds() {
		return connectionEstablishedEpochSeconds;
	}
    
	/**
     * @see PV#getConnectionFirstEstablishedEpochSeconds()
     */
	@Override
	public long getConnectionFirstEstablishedEpochSeconds() {
		return connectionFirstEstablishedEpochSeconds;
	}
   
	/**
    * @see PV#getConnectionLastRestablishedEpochSeconds()
    * */
	@Override
	public long getConnectionLastRestablishedEpochSeconds() {
		return connectionLastRestablishedEpochSeconds;
	}
   
	/**
    * @see PV#getConnectionLossRegainCount()
    */
	@Override
	public long getConnectionLossRegainCount() {
		return connectionLossRegainCount;
	}
    
	/**
     * @see PV#getArchDBRTypes()
     */
	@Override
	public ArchDBRTypes getArchDBRTypes() {
		return archDBRType;
	}

	/***
	 * @see PV#getDBRTimeEvent()
	 */
	@Override
	public DBRTimeEvent getDBRTimeEvent() {
		return dbrtimeevent;
	}

	/***
    *get  the meta info for this pv 
    * @return MetaInfo 
    */
	public MetaInfo getToalMetaInfo() {
		return totalMetaInfo;
	}
	
	/** Listener to the get... for meta data */
	private final GetListener meta_get_listener = new GetListener() {
		@Override
		public void getCompleted(final GetEvent event) { // This runs in a CA
															// thread
			if (event.getStatus().isSuccessful()) {
				state = State.GotMetaData;
				final DBR dbr = event.getDBR();
				totalMetaInfo.applyBasicInfo(EPICS_V3_PV.this.name, dbr, EPICS_V3_PV.this.configservice);
			} else {
				logger.error("The meta get listener was not successful for EPICS_V3_PV " + name);
			}
			PVContext.scheduleCommand(EPICS_V3_PV.this.name, EPICS_V3_PV.this.jcaCommandThreadId, EPICS_V3_PV.this.channel_ref, "getCompleted", new Runnable() {
				@Override
				public void run() {
					subscribe();
				}
			});
		}
	};
   
	/**
    * @see PV#addControledPV(String)
    */
	@Override
	public void addControledPV(String pvName) {
		controlledPVList.add(pvName);
	}
	
	/**
	 * Generate an EPICS PV.
	 * 
	 * @param name
	 *            The PV name.
	 *  @param configservice  The config service used by this pv
	 *  @param isControlPV true if this is a pv controlling other pvs      
	 */
	public EPICS_V3_PV(final String name, ConfigService configservice, boolean isControlPV, ArchDBRTypes archDBRTypes, PVMetrics pvMetrics, int jcaCommandThreadId) {
		this(name, false, configservice, jcaCommandThreadId);
		this.archDBRType = archDBRTypes;
		if(archDBRTypes != null) { 
			this.con = configservice.getArchiverTypeSystem().getJCADBRConstructor(archDBRType);
		}
		if (isControlPV) {
			this.controlledPVList = new ArrayList<String>();
		}
		this.pvMetrics = pvMetrics;
	}
	
	/**
	 * Generate an EPICS PV.
	 * 
	 * @param name
	 *            The PV name.
	 * @param  configservice The config service used by this pv
	 */
	// isControlPV
	public EPICS_V3_PV(final String name, ConfigService configservice, int jcaCommandThreadId) {
		this(name, false, configservice, jcaCommandThreadId);
	}
	
	
	/**
	 * Generate an EPICS PV.
	 * 
	 * @param name
	 *            The PV name.
	 * @param plain
	 *            When <code>true</code>, only the plain value is requested. No
	 *            time etc. Some PVs only work in plain mode, example:
	 *            "record.RTYP".
	 */
	public EPICS_V3_PV(final String name, final boolean plain, ConfigService configservice, int jcaCommandThreadId) {
		this.name = name;
		this.plain = plain;
		this.configservice = configservice;
		this.jcaCommandThreadId = jcaCommandThreadId;
		PVContext.setConfigservice(configservice);
	}
	
	/** Use finalize as last resort for cleanup, but give warnings. */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (channel_ref != null) {
			stop();
		}
	}
	
	/** @return Returns the name. */
	@Override
	public String getName() {
		return name;
	}
	
	/** {@inheritDoc} */
	@Override
	public DBRTimeEvent getValue() {
		return dbrtimeevent;
	}
	
	/** {@inheritDoc} */
	@Override
	public void addListener(final PVListener listener) {
		listeners.add(listener);
		if (running && isConnected())
			listener.pvValueUpdate(this);
	}
	
	/** {@inheritDoc} */
	@Override
	public void removeListener(final PVListener listener) {
		listeners.remove(listener);
	}
	
	/**
	 * Try to connect to the PV. OK to call more than once.
	 */
	private void connect() throws Exception {
		logger.debug("pv of"+this.name+" connectting");
		PVContext.scheduleCommand(this.name, this.jcaCommandThreadId, this.channel_ref, "connect", new Runnable() {
			@Override
			public void run() {
				//
				try {
					state = State.Connecting;
					// Already attempted a connection?
					synchronized (this) {
						if (channel_ref == null) {
							channel_ref = PVContext.getChannel(name,EPICS_V3_PV.this.jcaCommandThreadId, EPICS_V3_PV.this);
						}
						connectionRequestMadeEpochSeconds = System
								.currentTimeMillis() / 1000;
						if (channel_ref.getChannel().getConnectionState() == ConnectionState.CONNECTED) {
							handleConnected(channel_ref.getChannel());
						} else {
						}
					}
				} catch (Exception e) {
					logger.error("exception when connecting pv "+name, e);
				}
			}
		});
	}
	
	/**
	 * Disconnect from the PV. OK to call more than once.
	 */
	private void disconnect() {
		// Releasing the _last_ channel will close the context,
		// which waits for the JCA Command thread to exit.
		// If a connection or update for the channel happens at that time,
		// the JCA command thread will send notifications to this PV,
		// which had resulted in dead lock:
		// This code locked the PV, then tried to join the JCA Command thread.
		// JCA Command thread tried to lock the PV, so it could not exit.
		// --> Don't lock while calling into the PVContext.
		RefCountedChannel channel_ref_copy;
		synchronized (this) {
			// Never attempted a connection?
			if (channel_ref == null)
				return;
			channel_ref_copy = channel_ref;
			channel_ref = null;
			connected = false;
		}
		try {
			PVContext.releaseChannel(channel_ref_copy, this);
		} catch (final IllegalStateException ile) {
			logger.warn("exception when disconnecting pv "+name, ile);			
		} catch (final Throwable e) {
			logger.error("exception when disconnecting pv "+name, e);
		}
		fireDisconnected();
	}
	
	/** Subscribe for value updates. */
	private void subscribe() {
		synchronized (this) {
			// Prevent multiple subscriptions.
			if (subscription != null) {
				return;
			}
			// Late callback, channel already closed?
			final RefCountedChannel ch_ref = channel_ref;
			if (ch_ref == null) {
				return;
			}
			final Channel channel = ch_ref.getChannel();
			// final Logger logger = Activator.getLogger();
			try {
				if(channel.getConnectionState()!=Channel.CONNECTED){
					return;
				}
				//
				// the RefCountedChannel should maintain a single
				// subscription to the underlying CAJ/JCA channel.
				// So even with N PVs for the same channel, it's
				// only one subscription on the network instead of
				// N subscriptions.
				final DBRType type = DBR_Helper.getTimeType(plain,
						channel.getFieldType());
				state = State.Subscribing;
				totalMetaInfo.setStartTime(System.currentTimeMillis());
				// isnotTimestampDBR
				if (this.name.endsWith(".RTYP")) {
					subscription = channel.addMonitor(MonitorMask.ARCHIVE.getMask(), this);
				} else {
					subscription = channel.addMonitor(type,
							channel.getElementCount(), MonitorMask.ARCHIVE.getMask(), this);
				}
			} catch (final Exception ex) {
				logger.error("exception when subscribing pv "+name, ex);
			}
		}
	}
	
	/** Unsubscribe from value updates. */
	private void unsubscribe() {
		isFirstDataAfterConnection=false;
		Monitor sub_copy;
		// Atomic access
		synchronized (this) {
			sub_copy = subscription;
			subscription = null;
		}
		if (sub_copy == null) {
			return;
		}
		try {
			sub_copy.clear();
		} catch(IllegalStateException ile) { 
			logger.warn("Illegal state exception when unsubscribing pv "+ name, ile);
		} catch (final Exception ex) {
			logger.error("exception when unsubscribing pv "+ name, ex);
		}
	}
	
	/** {@inheritDoc} */
	@Override
	public void start() throws Exception {
		if (running) {
			return;
		}
		running = true;
		connect();
	}
	
	/** {@inheritDoc} */
	@Override
	public boolean isRunning() {
		return running;
	}
	
	/** {@inheritDoc} */
	@Override
	public boolean isConnected() {
		return connected;
	}
	
	/**
	 * Has a CA search been issued on the wire. 
	 * Could be used to throttle connection requests.
	 * @return
	 */
	@Override
	public boolean hasSearchBeenIssued() {
		if(connected) {
			return true;
		} else { 
			if(this.state == State.Connecting) { 
				return true;
			} else { 
				return false;
			}
		}
	}
	
	/** {@inheritDoc} */
	@Override
	public boolean isWriteAllowed() {
		return connected && channel_ref.getChannel().getWriteAccess();
	}
	
	/** {@inheritDoc} */
	@Override
	public String getStateInfo() {
		StringBuilder buf = new StringBuilder();
		buf.append(state.toString().charAt(0));
		if(this.channel_ref.getChannel() != null && (this.channel_ref.getChannel() instanceof CAJChannel)) { 
			CAJChannel cajChannel = (CAJChannel)this.channel_ref.getChannel();
			int currSearchTimerIndex = cajChannel.getOwnerIndex();
			buf.append(" Timer: " + currSearchTimerIndex);
			int searchTries = cajChannel.getSearchTries();
			buf.append(" Searches: " + searchTries);
		}
		return buf.toString();
	}
	
	/** {@inheritDoc} */
	@Override
	public void stop() {
		running = false;
		PVContext.scheduleCommand(this.name, this.jcaCommandThreadId, this.channel_ref, "stop", new Runnable() {
			@Override
			public void run() {
				logger.debug("Stopping channel " + EPICS_V3_PV.this.name);
				unsubscribe();
				disconnect();
			}
		});
	}
	
	/** {@inheritDoc} */
	@Override
	public void setValue(final Object new_value) throws Exception {
		if (!isConnected()) {
			throw new Exception(name + " is not connected");
		}
		// Send strings as strings..
		if (new_value instanceof String) {
			channel_ref.getChannel().put((String) new_value);
		} else { // other types as double.
			if (new_value instanceof Double) {
				final double val = ((Double) new_value).doubleValue();
				channel_ref.getChannel().put(val);
			} else if (new_value instanceof Double[]) {
				final Double dbl[] = (Double[]) new_value;
				final double val[] = new double[dbl.length];
				for (int i = 0; i < val.length; ++i) {
					val[i] = dbl[i].doubleValue();
				}
				channel_ref.getChannel().put(val);
			} else if (new_value instanceof Integer) {
				final int val = ((Integer) new_value).intValue();
				channel_ref.getChannel().put(val);
			} else if (new_value instanceof Integer[]) {
				final Integer ival[] = (Integer[]) new_value;
				final int val[] = new int[ival.length];
				for (int i = 0; i < val.length; ++i) {
					val[i] = ival[i].intValue();
				}
				channel_ref.getChannel().put(val);
			} else {
				throw new Exception("Cannot handle type "
						+ new_value.getClass().getName());
			}
		}
	}
	
	/** ConnectionListener interface. */
	@Override
	public void connectionChanged(final ConnectionEvent ev) {
		logger.debug("Connection changed for pv " + this.name);
		// This runs in a CA thread
		if (ev.isConnected()) { // Transfer to JCACommandThread to avoid
								// deadlocks
								// The connect event can actually happen 'right
								// away'
								// when the channel is created, before we even
								// get to assign
								// the channel_ref. So use the channel from the
								// event, not
								// the channel_ref which might still be null.
								//
			// EngineContext.getInstance().getScheduler().execute(new Runnable()
			PVContext.scheduleCommand(this.name, this.jcaCommandThreadId, this.channel_ref, "Connection changed connected", new Runnable() {
				@Override
				public void run() {
					handleConnected((Channel) ev.getSource());
				}
			});
		} else {
			state = State.Disconnected;
			connected = false;
			this.connectionLastLostEpochSeconds = System.currentTimeMillis()/1000;
			PVContext.scheduleCommand(this.name, this.jcaCommandThreadId, this.channel_ref, "Connection changed disconnected", new Runnable() {
				@Override
				public void run() {
					unsubscribe();
					fireDisconnected();
				}
			});
		}
	}
	
	/**
	 * PV is connected. Get meta info, or subscribe right away.
	 */
	private void handleConnected(final Channel channel) {
		isFirstDataAfterConnection=true;
		try { 
			if(channel.getConnectionState()!=Channel.CONNECTED){
				return;
			}
		} catch(Exception ex) { 
			logger.warn("Exception handling connection state change for " + this.name, ex);
			return;
		}
		if (state == State.Connected)
			return;
		state = State.Connected;
		connectionEstablishedEpochSeconds = System.currentTimeMillis() / 1000;
		hostName=channel_ref.getChannel().getHostName();
		totalMetaInfo.setHostName(hostName);
		if (firstTimeConnection) {
			this.connectionFirstEstablishedEpochSeconds = System
					.currentTimeMillis() / 1000;
			firstTimeConnection = false;
		} else {
			this.connectionLastRestablishedEpochSeconds = System
					.currentTimeMillis() / 1000;
			this.connectionLossRegainCount = this.connectionLossRegainCount + 1;
		}
		for (final PVListener listener : listeners) {
			listener.pvConnected(this);
		}
		// If we're "running", we need to get the meta data and
		// then subscribe.
		// Otherwise, we're done.
		if (!running) {
			connected = true;
			// meta = null;
			synchronized (this) {
				this.notifyAll();
			}
			return;
		}
		// else: running, get meta data, then subscribe
		try {
			DBRType type = channel.getFieldType();
			if (!(plain || type.isSTRING())) {
				state = State.GettingMetadata;
				if (type.isDOUBLE() || type.isFLOAT())
					type = DBRType.CTRL_DOUBLE;
				else if (type.isENUM())
					type = DBRType.LABELS_ENUM;
				else if (type.isINT())
					type = DBRType.CTRL_INT;
				else
					type = DBRType.CTRL_SHORT;
				channel.get(type, 1, meta_get_listener);
				return;
			}
		} catch (final Exception ex) {
			logger.error("exception when handleConnect "+name, ex);
			return;
		}
		// Meta info is not requested, not available for this type,
		// or there was an error in the get call.
		// So reset it, then just move on to the subscription.
		// meta = null;
		subscribe();
	}
	
	/** MonitorListener interface. */
	@Override
	public void monitorChanged(final MonitorEvent ev) {
		// final Logger log = Activator.getLogger();
		// This runs in a CA thread.
		// Ignore values that arrive after stop()
		if (!running) {
			return;
		}
		if (subscription == null) {
			return;
		}
		if (ev.getStatus() == null || !ev.getStatus().isSuccessful()) {
			return;
		}
		if (controlledPVList != null) {
			// this pv is control pv.
			try {
				updateAllControlPVEnablMent(ev);
			} catch (Exception e) {
				logger.error(
						"exception in monitor changed function when updatinng controlled pvs' enablement for " + this.name,
						e);
			}
			return;
		}
		state = State.GotMonitor;
		if (!connected)
			connected = true;
		try {
			final DBR dbr = ev.getDBR();
			if (dbr == null) {
				return;
			}
			if (this.name.endsWith(".RTYP")) {
				this.reacordTypeName = (((DBR_String) dbr).getStringValue())[0];
				return;
			}
			// dbr.printInfo();
			try {
				ArchDBRTypes generatedDBRType = JCA2ArchDBRType.valueOf(dbr);
				if (archDBRType == null) {
					archDBRType = generatedDBRType;
					con = configservice.getArchiverTypeSystem().getJCADBRConstructor(archDBRType);
				} else {
					assert(con != null);
					if(generatedDBRType != archDBRType) { 
						logger.warn("The type of PV " + this.name + " has changed from " + archDBRType + " to " + generatedDBRType);
						if(this.pvMetrics != null) { 
							this.pvMetrics.incrementInvalidTypeLostEventCount();
						}
						return;
					}
				}
				dbrtimeevent = con.newInstance(dbr);
			} catch (Exception e) {
				logger.error(
						"exception in monitor changed function when converting DBR to dbrtimeevent for pv " + this.name,
						e);
			}
			totalMetaInfo.computeRate(dbr, System.currentTimeMillis(), dbrtimeevent);
			updataMetaDataInParentPV(dbrtimeevent);
			// if this pv has meta data , handle here
			if (hasMetaField) {
				// //////////handle the field value when it
				// changes//////////////
				if (changedarchiveFieldsData.size() > 0) {
					logger.debug("Adding changed field for pv " + name + " with " + changedarchiveFieldsData.size());
					HashMap<String, String> tempHashMap = new HashMap<String, String>();
					tempHashMap.putAll(changedarchiveFieldsData);
					// dbrtimeevent.s
					dbrtimeevent.setFieldValues(tempHashMap, true);
					changedarchiveFieldsData.clear();
				}
				// //////////////////////////
				// ////////////save all the fields once every day//////////////
				if (this.lastTimeStampWhenSavingarchiveFields == null) {
					if (allarchiveFieldsData.size() != 0) {
						saveMetaDataOnceEveryDay();
					}
				} else {
					Calendar currentCalendar = Calendar.getInstance();
					currentCalendar.add(Calendar.DAY_OF_MONTH, -1);
					if (currentCalendar
							.after(lastTimeStampWhenSavingarchiveFields)) {
						// Calendar currentCalendar2=Calendar.getInstance();
						saveMetaDataOnceEveryDay();
					}
				}
				// //////////////////////////////
			}
			fireValueUpdate();
		} catch (final Exception ex) {
			logger.error("exception in monitor changed for pv " + this.name, ex);
		}
	}

	/**
	 * Add the cnxlostepsecs and cnxregainedepsecs to the specified DBRTimeEvent and then reset local state. 
	 * @param event
	 */
	public void addConnectionLostRegainedFields(DBRTimeEvent event) {
		if(firstDataAfterStartUp) {
			long cnxregainedsecs = System.currentTimeMillis()/1000;
			if(connectionLastLostEpochSeconds != 0 && connectionLastLostEpochSeconds != cnxregainedsecs) {
				if(logger.isDebugEnabled()) { 
					logger.debug("Adding cnxlostepsecs and cnxregainedepsecs after startup for pv " + name + " at " + cnxregainedsecs + " onto event @ " + event.getEpochSeconds());
				}
				event.addFieldValue("cnxlostepsecs", Long.toString(connectionLastLostEpochSeconds));
				event.addFieldValue("cnxregainedepsecs", Long.toString(cnxregainedsecs));
				event.addFieldValue("startup", Boolean.TRUE.toString());
				connectionLastLostEpochSeconds = 0;
			} else { 
				logger.debug("Skipping adding cnxlostepsecs and cnxregainedepsecs after startup for pv " + name);
			}
			firstDataAfterStartUp=false;
		} else {
			long cnxregainedsecs = System.currentTimeMillis()/1000;
			if(isFirstDataAfterConnection) {
				if(connectionLastLostEpochSeconds != 0 && connectionLastLostEpochSeconds != cnxregainedsecs) { 
					if(logger.isDebugEnabled()) { 
						logger.debug("Adding cnxlostepsecs and cnxregainedepsecs after regaining connection for pv " + name + " at " + cnxregainedsecs + " onto event @ " + event.getEpochSeconds());
					}
					event.addFieldValue("cnxlostepsecs", Long.toString(connectionLastLostEpochSeconds));
					event.addFieldValue("cnxregainedepsecs", Long.toString(cnxregainedsecs));
					connectionLastLostEpochSeconds = 0;
				} else { 
					logger.debug("Skipping adding cnxlostepsecs and cnxregainedepsecs after regaining connection for pv " + name);
				}
				isFirstDataAfterConnection=false;
			}
		}
	}
	
	/** Notify all listeners. */
	private void fireValueUpdate() {
		for (final PVListener listener : listeners) {
			listener.pvValueUpdate(this);
		}
	}
	
	/** Notify all listeners. */
	private void fireDisconnected() {
		for (final PVListener listener : listeners) {
			listener.pvDisconnected(this);
		}
	}
	@Override
	public String toString() {
		return "EPICS_V3_PV '" + name + "'";
	}
   
	/***
	 * if  this is a pv control other pvs,  when  this pv's value changes, it will stop or restart all controlled pvs.
	 * @param ev  
	 * @throws Exception error when update all controlled pv's archiving status
	 */
	private void updateAllControlPVEnablMent(MonitorEvent ev) throws Exception {
		final DBR dbr = ev.getDBR();
		boolean enable = DBR_Helper.decodeBooleanValue(dbr);
		ArrayList<String> copyOfControlledPVList = new ArrayList<String>(controlledPVList);
		if (enable) {
			enableAllPV = true;
			for (String pvName : copyOfControlledPVList) {
				logger.debug(pvName+" will be resumed");
				ArchiveEngine.resumeArchivingPV(pvName, configservice);
			}
		} else {
			enableAllPV = false;
			for (String pvName : copyOfControlledPVList) {
				logger.debug(pvName+" will be paused");
				ArchiveEngine.pauseArchivingPV(pvName, configservice);
			}
		}
	}

	/***
	 * @see PV#setParentChannel(ArchiveChannel)
	 */
	@Override
	public void setParentChannel(ArchiveChannel channel) {
		this.parentChannel = channel;
		isarchiveFieldsField = true;
	}

	@Override
	public void setParentChannel(ArchiveChannel channel, boolean isRuntimeOnly) {
		setParentChannel(channel);
		this.isruntimeFieldField = isRuntimeOnly;
	}

	/**
	 * update the meta field value in the parent pv.
	 * @param dbrtimeevent 
	 */
	private void updataMetaDataInParentPV(final DBRTimeEvent dbrtimeevent) {
		if (isarchiveFieldsField)
			parentChannel.updataMetaField(this.name, ""
					+ dbrtimeevent.getSampleValue().toString());
	}

	/**
	 * @see PV#updataMetaField(String,String)
	 */
	@Override
	public void updataMetaField(String PVname, String fieldValue) {
		String[] strs = PVname.split("\\.");
		String fieldName = strs[strs.length - 1];
		if(isruntimeFieldField) { 
			logger.debug("Not storing value change for runtime field " + fieldName);
			runTimeFieldsData.put(fieldName, fieldValue);
		} else { 
			logger.debug("Storing value change for meta field " + fieldName);
			allarchiveFieldsData.put(fieldName, fieldValue);
			changedarchiveFieldsData.put(fieldName, fieldValue);
		}
	}

	/***
	 * @See PV#setHasMetaField(boolean)
	 */
	@Override
	public void setHasMetaField(boolean hasMetaField) {
		if (hasMetaField) {
			allarchiveFieldsData = new ConcurrentHashMap<String, String>();
			changedarchiveFieldsData = new ConcurrentHashMap<String, String>();
		}
		this.hasMetaField = hasMetaField;
	}

	/**
	 * save the meta data
	 */
	private void saveMetaDataOnceEveryDay() {
		HashMap<String, String> tempHashMap = new HashMap<String, String>();
		tempHashMap.putAll(allarchiveFieldsData);
		if(runTimeFieldsData != null && !runTimeFieldsData.isEmpty()) {
			// This should store fields like the description at least once every day.
			tempHashMap.putAll(runTimeFieldsData);
		}
		if(this.totalMetaInfo != null) {
			if(this.totalMetaInfo.getUnit() != null) { 
				tempHashMap.put("EGU", this.totalMetaInfo.getUnit());
			}
			if(this.totalMetaInfo.getPrecision() != 0) { 
				tempHashMap.put("PREC", Integer.toString(this.totalMetaInfo.getPrecision()));
			}
		}
		// dbrtimeevent.s
		dbrtimeevent.setFieldValues(tempHashMap, false);
		lastTimeStampWhenSavingarchiveFields = Calendar.getInstance();
	}

	/**
	 * Get the current value of all the meta fields. 
	 * @return
	 */
	public HashMap<String, String> getCurrentCopyOfMetaFields() { 
		HashMap<String, String> retval = new HashMap<String, String>();
		if(totalMetaInfo != null && totalMetaInfo.getUnit() != null) { 
			retval.put("EGU", totalMetaInfo.getUnit());
			retval.put("PREC", Integer.toString(totalMetaInfo.getPrecision()));
		}
		
		if(allarchiveFieldsData != null && !allarchiveFieldsData.isEmpty()) { 
			retval.putAll(allarchiveFieldsData);
		}
		if(runTimeFieldsData != null && !runTimeFieldsData.isEmpty()) { 
			retval.putAll(runTimeFieldsData);
		}
		return retval;
	}
	
	public long getConnectionLastLostEpochSeconds() {
		return connectionLastLostEpochSeconds;
	}
	
	public void resetConnectionLastLostEpochSeconds() {
		connectionLastLostEpochSeconds = 0;
	}
	
	public String getLowLevelChannelInfo() {
		// Commented out when using JCA. This seems to work in CAJ but not in JCA.
/*		if(channel_ref != null) { 
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			PrintStream out = new PrintStream(os);
			channel_ref.getChannel().printInfo(out);
			out.close();
			return os.toString();
		}
*/		return null;
	}
	
	
	public void updateTotalMetaInfo() throws IllegalStateException, CAException { 
		GetListener getListener = new GetListener() {
			@Override
			public void getCompleted(final GetEvent event) {
				// This runs in a CA thread
				if (event.getStatus().isSuccessful()) {
					state = State.GotMetaData;
					final DBR dbr = event.getDBR();
					logger.debug("Updating metadata (EGU/PREC etc) for pv " + EPICS_V3_PV.this.name);
					totalMetaInfo.applyBasicInfo(EPICS_V3_PV.this.name, dbr, EPICS_V3_PV.this.configservice);
				} else {
					logger.error("The meta get listener was not successful for EPICS_V3_PV " + name);
				}
			}
		};
		if(channel_ref != null) { 
			if(channel_ref.getChannel().getConnectionState() == ConnectionState.CONNECTED) { 
				DBRType type = channel_ref.getChannel().getFieldType();
				if (!(plain || type.isSTRING())) {
					state = State.GettingMetadata;
					if (type.isDOUBLE() || type.isFLOAT())
						type = DBRType.CTRL_DOUBLE;
					else if (type.isENUM())
						type = DBRType.LABELS_ENUM;
					else if (type.isINT())
						type = DBRType.CTRL_INT;
					else
						type = DBRType.CTRL_SHORT;
					channel_ref.getChannel().get(type, 1, getListener);
				}
			}
		}
	}
	
	
	/**
	 * Combine the metadata from various sources and return the latest copy.
	 * @return
	 */
	public HashMap<String, String> getLatestMetadata() { 
		HashMap<String, String> retVal = new HashMap<String, String>();
		// The totalMetaInfo is updated once every 24hours...
		MetaInfo metaInfo = this.getToalMetaInfo();
		if(metaInfo != null) {
			metaInfo.addToDict(retVal);
		}
		// Add the latest value of the fields we are monitoring.
		if(allarchiveFieldsData != null) { 
			retVal.putAll(allarchiveFieldsData);
		}
		if(runTimeFieldsData != null) { 
			retVal.putAll(runTimeFieldsData);
		}
		
		return retVal;
	}
	
	
	/**
	 * Returns the current CAJ search timer; use only for debugging. This will be removed later.
	 * @return
	 */
	public int getCurrentSearchTimer() { 
		if(this.channel_ref.getChannel() != null && (this.channel_ref.getChannel() instanceof CAJChannel)) { 
			CAJChannel cajChannel = (CAJChannel)this.channel_ref.getChannel();
			int currSearchTimerIndex = cajChannel.getOwnerIndex();
			return currSearchTimerIndex;
		}
		return -1;
	}
}
