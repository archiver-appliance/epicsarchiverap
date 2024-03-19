/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.pv;

import com.cosylab.epics.caj.CAJChannel;
import com.cosylab.epics.caj.CAJMonitor;
import gov.aps.jca.CAException;
import gov.aps.jca.Channel;
import gov.aps.jca.Channel.ConnectionState;
import gov.aps.jca.Monitor;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.DBR_CTRL_Double;
import gov.aps.jca.dbr.DBR_CTRL_Int;
import gov.aps.jca.dbr.DBR_LABELS_Enum;
import gov.aps.jca.dbr.DBR_String;
import gov.aps.jca.event.ConnectionEvent;
import gov.aps.jca.event.ConnectionListener;
import gov.aps.jca.event.GetEvent;
import gov.aps.jca.event.GetListener;
import gov.aps.jca.event.MonitorEvent;
import gov.aps.jca.event.MonitorListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.JCA2ArchDBRType;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.engine.ArchiveEngine;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * EPICS ChannelAccess implementation of the PV interface.
 * 
 * @see PV
 * @author Kay Kasemir
 * @version Initial version:CSS
 * @version 4-Jun-2012, Luofeng Li:added codes to support for the new archiver
 */
public class EPICS_V3_PV implements PV, ControllingPV, ConnectionListener, MonitorListener {
	private static final Logger logger = LogManager.getLogger(EPICS_V3_PV.class.getName());
	
	/**
	 * Use plain mode?
	 * @see EPICS_V3_PV(String, boolean)
	 */
	final private boolean plain;

	/** Channel name. */
	final private String name;
	
	/**the meta info for this pv*/
	private MetaInfo totalMetaInfo = new MetaInfo();
	
	/**
	 * If this pv is a meta field, then the metafield parent PV is where the data for this metafield is stored.
	 **/
	private EPICS_V3_PV parentPVForMetaField = null;
	
	/**
	 * If this pv has many meta fields archived, allarchiveFieldsData includes the meta field names and their values.
	 * allarchiveFieldsData is updated when meta field changes
	 * if this pv doesn't have meta field archived, this  is always  null.
	 */
	private ConcurrentHashMap<String, String> allarchiveFieldsData = null;
	
	/** Runtime fields that are not archived/stored are stored here */
	private final ConcurrentHashMap<String, String> runTimeFieldsData = new ConcurrentHashMap<String, String>();
	
	/** if this pv has many meta fields archived,changedarchiveFieldsData includes the changed meta values and the field names*/
	private ConcurrentHashMap<String, String> changedarchiveFieldsData = null;
	
	/**we save all meta field once every day and lastTimeStampWhenSavingarchiveFields is when we save all last meta fields*/
	private long archiveFieldsSavedAtEpSec = 0;
	
	/**this pv is meta field  or not*/
	private boolean isarchiveFieldsField = false;
	
	/** Store the value for this only in the runtime and not into the stores...*/
	private boolean isruntimeFieldField = false;
	
	/**
	 * Establish the monitor using a DBE_PROPERTY mask. DBE_PROPERTY events are also processed a little differently.
	 */
	private boolean isDBEProperties = false;
	
	private PVConnectionState state = PVConnectionState.Idle;
	
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
		ARCHIVE(2 | 4),

		/** Listen to changes in alarm state */
		ALARM(4),
		
		/** Listen to changes in property  */
		PROPERTY(8);

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
	
	private Monitor dbePropertiesSubscription = null;
	
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
	 * The JCA command thread that processes actions for this PV.
	 * This should be inherited from the ArchiveChannel.
	 */
	private int jcaCommandThreadId;

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
	 * The server time when we last received a monitor event.
	 */
	private long lastMonitorSecs = 0;
	
	/**
	 * Misc bucket for catching various transient errors that we are aware of. Probably not all that useful.
	 */
	private long transientErrorCount = 0;

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
	@Override
	public boolean isEnableAllPV() {
		return enableAllPV;
	}
        
	/**
     * @see PV#getArchDBRTypes()
     */
	@Override
	public ArchDBRTypes getArchDBRTypes() {
		return archDBRType;
	}

	/***
    *get  the meta info for this pv 
    * @return MetaInfo 
    */
	@Override
	public MetaInfo getTotalMetaInfo() {
		return totalMetaInfo;
	}
	
	/** Listener to the get... for meta data */
	private final GetListener meta_get_listener = new GetListener() {
		@Override
		public void getCompleted(final GetEvent event) { // This runs in a CA
															// thread
			if (event.getStatus().isSuccessful()) {
				state = PVConnectionState.GotMetaData;
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
	 * @param pvName
	 *            The PV name
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
	 * @param configservice  The config service used by this pv
	 * @param isControlPV true if this is a pv controlling other pvs  
	 * @param archDBRTypes ArchDBRTypes
	 * @param jcaCommandThreadId The JCA Command thread. 
	 * @param isDBEProperties  &emsp;
	 */
	EPICS_V3_PV(final String name, ConfigService configservice, boolean isControlPV, ArchDBRTypes archDBRTypes, int jcaCommandThreadId, boolean isDBEProperties) {
		this(name, false, configservice, jcaCommandThreadId);
		this.archDBRType = archDBRTypes;
		this.isDBEProperties = isDBEProperties;
		if(archDBRTypes != null) { 
			this.con = configservice.getArchiverTypeSystem().getJCADBRConstructor(archDBRType);
		}
		if (isControlPV) {
			this.controlledPVList = new ArrayList<String>();
		}
	}
	
	/**
	 * Generate an EPICS PV.
	 * 
	 * @param name
	 *            The PV name.
	 * @param  configservice The config service used by this pv
	 * @param jcaCommandThreadId The JCA Command thread 
	 */
	// isControlPV
	EPICS_V3_PV(final String name, ConfigService configservice, int jcaCommandThreadId) {
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
	 * @param configservice  The config service used by this pv
	 * @param jcaCommandThreadId  The JCA Command thread 
	 */
	private EPICS_V3_PV(final String name, final boolean plain, ConfigService configservice, int jcaCommandThreadId) {
		this.name = name;
		this.plain = plain;
		this.configservice = configservice;
		this.jcaCommandThreadId = jcaCommandThreadId;
		PVContext.setConfigservice(configservice);
	}
	
	/** @return Returns the name. */
	@Override
	public String getName() {
		return name;
	}
	
	/** {@inheritDoc} */
	@Override
	public void addListener(final PVListener listener) {
		listeners.add(listener);
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
					state = PVConnectionState.Connecting;
					// Already attempted a connection?
					synchronized (this) {
						if (channel_ref == null) {
							channel_ref = PVContext.getChannel(name,EPICS_V3_PV.this.jcaCommandThreadId, EPICS_V3_PV.this);
						}
						fireConnectionRequestMade();
						if (channel_ref.getChannel().getConnectionState() == ConnectionState.CONNECTED) {
							handleConnected(channel_ref.getChannel());
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
				logger.error("When trying to establish a subscription, there is already a subscription for " + this.name);
				return;
			}
			// Late callback, channel already closed?
			final RefCountedChannel ch_ref = channel_ref;
			if (ch_ref == null) {
				logger.error("When trying to establish a subscription, the refcounted channel is closed for " + this.name);
				return;
			}
			final Channel channel = ch_ref.getChannel();
			// final Logger logger = Activator.getLogger();
			try {
				if(channel.getConnectionState()!=Channel.CONNECTED){
					logger.error("When trying to establish a subscription, the CA channel is not connected for " + this.name);
					return;
				}
				//
				// the RefCountedChannel should maintain a single
				// subscription to the underlying CAJ/JCA channel.
				// So even with N PVs for the same channel, it's
				// only one subscription on the network instead of
				// N subscriptions.
				final DBRType type = DBR_Helper.getTimeType(plain, channel.getFieldType());
				state = PVConnectionState.Subscribing;
				totalMetaInfo.setStartTime(System.currentTimeMillis());
				// isnotTimestampDBR
				if (this.name.endsWith(".RTYP")) {
					subscription = channel.addMonitor(MonitorMask.ARCHIVE.getMask(), this);
				} else {
					if(this.isDBEProperties && dbePropertiesSubscription == null) { 
						logger.debug("Adding a DBE_PROPERTIES monitor for " + this.name);
						dbePropertiesSubscription = channel.addMonitor(DBR_Helper.getControlType(channel.getFieldType()),
								1, MonitorMask.PROPERTY.getMask(), new MonitorListener() {
									
									@Override
									public void monitorChanged(MonitorEvent monitorEvent) {
										logger.debug("Got a DBE_PROPERTIES event for " + name);
										processDBEPropertiesMonitorEvent(monitorEvent);
									}
								});
					} 
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
	 * @return
	 */
	@Override
	public PVConnectionState connectionState() {
		return this.state;
	}

	/** {@inheritDoc} */
	@Override
	public void stop() {
		running = false;
		PVContext.scheduleCommand(this.name, this.jcaCommandThreadId, this.channel_ref, "stop", () -> {
			logger.debug("Stopping channel " + EPICS_V3_PV.this.name);
			unsubscribe();
			disconnect();
		});
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
			state = PVConnectionState.Disconnected;
			connected = false;
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
		try { 
			if(channel.getConnectionState()!=Channel.CONNECTED){
				logger.error("While processing a handleConnected for " + this.name + "; the channel is not in a connected state.");
				return;
			}
		} catch(Exception ex) { 
			logger.error("Exception handling connection state change for " + this.name, ex);
			return;
		}
		if (state == PVConnectionState.Connected) {
			logger.error("While processing a handleConnected for " + this.name + "; the state is already in a connected state.");
			return;
		}
		state = PVConnectionState.Connected;
		hostName=channel.getHostName();
		totalMetaInfo.setHostName(hostName);
		for (final PVListener listener : listeners) {
			listener.pvConnected(this);
		}
		// If we're "running", we need to get the meta data and
		// then subscribe.
		// Otherwise, we're done.
		if (!running) {
			logger.warn("While processing a handleConnected for " + this.name + " the channel is not running.");
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
				state = PVConnectionState.GettingMetadata;
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
		this.lastMonitorSecs = TimeUtils.getCurrentEpochSeconds();
		this.dbrtimeevent = null; // Now that this is private; we should be able to set this to null on each monitor event

		// This runs in a CA thread.

		if (!running) {
			logger.error("Ignoring monitor events that arrive after stop for " + this.name);
			this.transientErrorCount++;
			return;
		}
		if (subscription == null) {
			logger.error("Ignoring monitor events that arrive after we clean up the subscription for " + this.name);
			this.transientErrorCount++;
			return;
		}
		if (ev.getStatus() == null || !ev.getStatus().isSuccessful()) {
			logger.error("Ignoring monitor events that have an invalid CA status for " + this.name);
			this.transientErrorCount++;
			return;
		}
		if (controlledPVList != null) {
			// this pv is control pv.
			try {
				updateAllControlPVEnablement(ev);
			} catch (Exception e) {
				logger.error(
						"exception in monitor changed function when updatinng controlled pvs' enablement for " + this.name,
						e);
			}
			logger.warn("This " + this.name + " is a PV that controls other PV's.");
			return;
		}
		state = PVConnectionState.GotMonitor;
		if (!connected)
			connected = true;
		try {
			try {
				DBR dbr = ev.getDBR();
				if (dbr == null) {
					logger.error("Ignoring monitor events that does not have a valid DBR for " + this.name);
					return;
				}
				if (this.name.endsWith(".RTYP")) {
					String rtypName = (((DBR_String) dbr).getStringValue())[0];
					dbrtimeevent = new POJOEvent(ArchDBRTypes.DBR_SCALAR_STRING, TimeUtils.now(), new ScalarStringSampleValue(rtypName), 0, 0);
					fireValueUpdate(dbrtimeevent);
					return;
				}
				// dbr.printInfo();

				ArchDBRTypes generatedDBRType = JCA2ArchDBRType.valueOf(dbr);
				if (archDBRType == null) {
					archDBRType = generatedDBRType;
					con = configservice.getArchiverTypeSystem().getJCADBRConstructor(archDBRType);
				} else {
					assert(con != null);
					if(generatedDBRType != archDBRType) { 
						logger.warn("The type of PV " + this.name + " has changed from " + archDBRType + " to " + generatedDBRType);
						fireDroppedSampleTypeChange(generatedDBRType);
						return;
					}
				}
				dbrtimeevent = con.newInstance(dbr);
				totalMetaInfo.computeRate(dbrtimeevent);
				dbr = null;
			} catch (Exception e) {
				logger.error(
						"exception in monitor changed function when converting DBR to dbrtimeevent for pv " + this.name,
						e);
				this.transientErrorCount++;
			}
			
			updataMetaDataInParentPV(dbrtimeevent);
			fireValueUpdate(dbrtimeevent);
		} catch (final Exception ex) {
			logger.error("exception in monitor changed for pv " + this.name, ex);
			this.transientErrorCount++;
		}
	}

	
	/** Notify all listeners. */
	private void fireValueUpdate(DBRTimeEvent ev) {
		for (final PVListener listener : listeners) {
			listener.pvValueUpdate(this, ev);
		}
	}
	
	/** Notify all listeners. */
	private void fireDisconnected() {
		for (final PVListener listener : listeners) {
			listener.pvDisconnected(this);
		}
	}
	
	/** Notify all listeners. */
	private void fireConnectionRequestMade() {
		for (final PVListener listener : listeners) {
			listener.pvConnectionRequestMade(this);
		}
	}
	
	private void fireDroppedSampleTypeChange(ArchDBRTypes newCAType) { 
		for (final PVListener listener : listeners) {
			listener.sampleDroppedTypeChange(this,  newCAType);
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
	private void updateAllControlPVEnablement(MonitorEvent ev) throws Exception {
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
	 * Set the "parent" PV for this meta field pv. The data from this PV is stored as a metafield in the parentPV.
	 * @param parentPV - Store data from this PV as a metafield in the parentPV.
	 * @param isRuntimeOnly - Only store values in the runtime hashMaps.
	 */
	public void setMetaFieldParentPV(EPICS_V3_PV parentPV, boolean isRuntimeOnly) {
		this.parentPVForMetaField = parentPV;
		isarchiveFieldsField = true;
		this.isruntimeFieldField = isRuntimeOnly;
	}

	/**
	 * update the meta field value in the parent pv.
	 * @param dbrtimeevent DBRTimeEvent
	 */
	private void updataMetaDataInParentPV(final DBRTimeEvent dbrtimeevent) {
		if (isarchiveFieldsField) { 
			parentPVForMetaField.updataMetaFieldValue(this.name, "" + dbrtimeevent.getSampleValue().toString());
		}
	}

	/***
	 * Update the value in the parent pv hashmaps for this field
	 * @param pvName  this meta field pv 's name - this is the full PV names - for example, a:b:c.HIHI
	 * @param fieldValue - this meta field pv's value as a string.
	 */
	public void updataMetaFieldValue(String pvName, String fieldValue) {
		String[] strs = pvName.split("\\.");
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
	 * Making this PV as having metafields or not
	 * If the PV has metafields, then internal state is created to maintain the latest values of these metafields.
	 * @param hasMetaField  &emsp;
	 */
	public void markPVHasMetafields(boolean hasMetaField) {
		if (hasMetaField) {
			allarchiveFieldsData = new ConcurrentHashMap<String, String>();
			changedarchiveFieldsData = new ConcurrentHashMap<String, String>();
		}
		this.hasMetaField = hasMetaField;
	}
	
	@Override
	public void getLowLevelChannelInfo(List<Map<String, String>> statuses) {
		// Commented out when using JCA. This seems to work in CAJ but not in JCA.
/*		if(channel_ref != null) { 
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			PrintStream out = new PrintStream(os);
			channel_ref.getChannel().printInfo(out);
			out.close();
			return os.toString();
		}		
*/
		class AddDetail {
			private final List<Map<String, String>> statuses;
			AddDetail(List<Map<String, String>> statuses) {
				this.statuses = statuses;
			}
			void addKV(String k, String v) {
				HashMap<String, String> h = new HashMap<String, String>();
				h.put("name", k);
				h.put("value", v == null ? "N/A" : v);
				h.put("source", "CA");
				this.statuses.add(h);
			}
		}
		
		AddDetail ad = new AddDetail(statuses);
		ad.addKV("PV connection state machine state", state.toString());
		ad.addKV("Last monitor received at", TimeUtils.convertToHumanReadableString(this.lastMonitorSecs));
		ad.addKV("Last monitor had a valid DBR?", Boolean.toString(this.dbrtimeevent != null));
		if(this.dbrtimeevent != null) {
			ad.addKV("Last monitor event timestamp", TimeUtils.convertToHumanReadableString(this.dbrtimeevent.getEventTimeStamp()));			
		}
		ad.addKV("Various transient errors", Long.toString(transientErrorCount));
		
		ad.addKV("Do we have a CA channel?", Boolean.toString(this.channel_ref != null && this.channel_ref.getChannel() != null));
		ad.addKV("Do we have a subscription?", Boolean.toString(this.subscription != null));
		if (this.channel_ref != null && this.channel_ref.getChannel() != null && (this.channel_ref.getChannel() instanceof CAJChannel cajChannel)) {
			ad.addKV("CAJ Searches", Integer.toString(cajChannel.getSearchTries()));
			ad.addKV("CAJ channel ID (CID)", Integer.toString(cajChannel.getChannelID()));
			ad.addKV("CAJ server channel ID (SID)", Integer.toString(cajChannel.getServerChannelID()));
			if(this.subscription != null && this.subscription instanceof CAJMonitor) {
				ad.addKV("CAJ subscription ID", Integer.toString(((CAJMonitor)subscription).getSID()));
			}
			ad.addKV("CAJ connection state", cajChannel.getConnectionState().getName());
		}		
		ad.addKV("Daily metadata last saved at", TimeUtils.convertToHumanReadableString(archiveFieldsSavedAtEpSec));
		ad.addKV("Do we use DBE_Properties?", Boolean.toString(isDBEProperties));		
		ad.addKV("Do we have a DBE Properties subscription?", Boolean.toString(this.dbePropertiesSubscription != null));
		ad.addKV("The internal connected bool", Boolean.toString(this.connected));
		ad.addKV("The internal running bool", Boolean.toString(this.running));
		ad.addKV("Do we have a valid DBR Type constructor", Boolean.toString(con != null));
		ad.addKV("The CAJ command thread id", Integer.toString(jcaCommandThreadId));
		ad.addKV("Any other PV's being controlled?", Boolean.toString(this.controlledPVList != null));
		if(this.controlledPVList != null) {
			ad.addKV("Number of other PV's being controlled", Integer.toString(this.controlledPVList.size()));
			ad.addKV("Status of controlled PVs", Boolean.toString(this.enableAllPV));
		}
		ad.addKV("Has metafields?", Boolean.toString(this.hasMetaField));
		ad.addKV("Hostname of PV from CA", this.hostName);
		
		return;
	}
	
	
	@Override
	public void updateTotalMetaInfo() throws IllegalStateException, CAException {
		GetListener getListener = event -> {
			// This runs in a CA thread
			if (event.getStatus().isSuccessful()) {
				final DBR dbr = event.getDBR();
				logger.debug("Updating metadata (EGU/PREC etc) for pv " + EPICS_V3_PV.this.name);
				totalMetaInfo.applyBasicInfo(EPICS_V3_PV.this.name, dbr, EPICS_V3_PV.this.configservice);
			} else {
				logger.error("The meta get listener was not successful for EPICS_V3_PV " + name);
			}
		};
		if(channel_ref != null) { 
			if(channel_ref.getChannel().getConnectionState() == ConnectionState.CONNECTED) { 
				DBRType type = channel_ref.getChannel().getFieldType();
				if (!(plain || type.isSTRING())) {
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
	 * @see PV#getLatestMetadata
	 */
	@Override
	public HashMap<String, String> getLatestMetadata() { 
		HashMap<String, String> retVal = new HashMap<String, String>();
		// The totalMetaInfo is updated once every 24hours...
		MetaInfo metaInfo = this.getTotalMetaInfo();
		if(metaInfo != null) {
			metaInfo.addToDict(retVal);
		}
		// Add the latest value of the fields we are monitoring.
		if(allarchiveFieldsData != null) { 
			retVal.putAll(allarchiveFieldsData);
		}
		retVal.putAll(runTimeFieldsData);

		return retVal;
	}
	
	public void setDBEroperties() { 
		this.isDBEProperties = true;
	}
	
	private void addUpdateChangedDBEPropertyMetaField(String fieldName, String value) { 
		String currentValue = this.allarchiveFieldsData.get(fieldName);
		if(currentValue != null && value != null && !currentValue.equals(value)) { 
			this.changedarchiveFieldsData.put(fieldName, value);
		}
		assert value != null;
		this.allarchiveFieldsData.put(fieldName, value);
	}
	
	private void processDBEPropertiesMonitorEvent(MonitorEvent monitorEvent) {
		DBR dbr = monitorEvent.getDBR();
		if (dbr.isLABELS()) {
			logger.debug("Updating DBE_PROPERTIES labels for ENUM pv " + name);
			final DBR_LABELS_Enum labels = (DBR_LABELS_Enum)dbr;
			addUpdateChangedDBEPropertyMetaField("LABELS", String.join(",", labels.getLabels()));
		} else if (dbr instanceof DBR_CTRL_Double || dbr instanceof DBR_CTRL_Int) {
			logger.debug("Updating DBE_PROPERTIES metafields for DBR_CTRL_Double for pv " + name);
			assert dbr instanceof DBR_CTRL_Double;
			final DBR_CTRL_Double ctrl = (DBR_CTRL_Double) dbr;
			// fieldsAvailableFromDBRControl = new String[] {"PREC", "EGU", "HOPR", "LOPR", "HIHI", "HIGH", "LOW", "LOLO", "DRVH", "DRVL" };  
			addUpdateChangedDBEPropertyMetaField("PREC", Short.toString(ctrl.getPrecision()));
			addUpdateChangedDBEPropertyMetaField("EGU", ctrl.getUnits());
			addUpdateChangedDBEPropertyMetaField("HOPR", ctrl.getUpperDispLimit().toString());
			addUpdateChangedDBEPropertyMetaField("LOPR", ctrl.getLowerDispLimit().toString());

			addUpdateChangedDBEPropertyMetaField("HIHI", ctrl.getUpperAlarmLimit().toString());
			addUpdateChangedDBEPropertyMetaField("HIGH", ctrl.getUpperWarningLimit().toString());
			addUpdateChangedDBEPropertyMetaField("LOW", ctrl.getLowerWarningLimit().toString());
			addUpdateChangedDBEPropertyMetaField("LOLO", ctrl.getLowerAlarmLimit().toString());

			addUpdateChangedDBEPropertyMetaField("DRVH", ctrl.getUpperCtrlLimit().toString());
			addUpdateChangedDBEPropertyMetaField("DRVL", ctrl.getLowerCtrlLimit().toString());
		} else {
			logger.error("In applyBasicInfo, cannot determine dbr type for " + dbr.getClass().getName());
		}
	}

	@Override
	public void sampleWrittenIntoStores() {
		
	}
	
	/**
	 * save the meta data
	 */
	private void saveMetaDataOnceEveryDay(DBRTimeEvent lastEvent) {
		HashMap<String, String> tempHashMap = new HashMap<String, String>(allarchiveFieldsData);
		if (!runTimeFieldsData.isEmpty()) {
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
		lastEvent.setFieldValues(tempHashMap, false);
		archiveFieldsSavedAtEpSec = TimeUtils.getCurrentEpochSeconds();
	}


	@Override
	public void aboutToWriteBuffer(DBRTimeEvent lastEvent) {
		// if this pv has meta data , handle here
		if (hasMetaField) {
			// //////////handle the field value when it
			// changes//////////////
			if (!changedarchiveFieldsData.isEmpty()) {
				logger.debug("Adding changed field for pv " + name + " with " + changedarchiveFieldsData.size());
				HashMap<String, String> tempHashMap = new HashMap<>(changedarchiveFieldsData);
				// dbrtimeevent.s
				lastEvent.setFieldValues(tempHashMap, true);
				synchronized(this) {
					changedarchiveFieldsData.clear();
				}
			}
			if (!allarchiveFieldsData.isEmpty()) {
				long nowES = TimeUtils.getCurrentEpochSeconds();
				if ((nowES - archiveFieldsSavedAtEpSec) >= 86400) {
					saveMetaDataOnceEveryDay(lastEvent);
				}
			}
		}
		
	}
}
