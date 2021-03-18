package org.epics.archiverappliance.engine.pv;

import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.pvaccess.client.Channel;
import org.epics.pvaccess.client.Channel.ConnectionState;
import org.epics.pvaccess.client.ChannelGet;
import org.epics.pvaccess.client.ChannelGetRequester;
import org.epics.pvaccess.client.ChannelProvider;
import org.epics.pvaccess.client.ChannelRequester;
import org.epics.pvdata.copy.CreateRequest;
import org.epics.pvdata.misc.BitSet;
import org.epics.pvdata.monitor.Monitor;
import org.epics.pvdata.monitor.MonitorElement;
import org.epics.pvdata.monitor.MonitorRequester;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVBoolean;
import org.epics.pvdata.pv.PVByte;
import org.epics.pvdata.pv.PVDouble;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.PVFloat;
import org.epics.pvdata.pv.PVInt;
import org.epics.pvdata.pv.PVLong;
import org.epics.pvdata.pv.PVScalar;
import org.epics.pvdata.pv.PVShort;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVUByte;
import org.epics.pvdata.pv.PVUInt;
import org.epics.pvdata.pv.PVULong;
import org.epics.pvdata.pv.PVUShort;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Structure;

public class EPICS_V4_PV implements PV, ChannelGetRequester, ChannelRequester, MonitorRequester {
	private static final Logger logger = Logger.getLogger(EPICS_V4_PV.class.getName());

	/** Channel name. */
	private String name = null;
	
	/**the meta info for this pv*/
	private MetaInfo totalMetaInfo = new MetaInfo();
	
	private PVConnectionState state = PVConnectionState.Idle;
	
	private Channel channel;
	
	/**configservice used by this pv*/
	private ConfigService configservice = null;
	
	/** PVListeners of this PV */
	final private CopyOnWriteArrayList<PVListener> listeners = new CopyOnWriteArrayList<PVListener>();
	
	/**
	 * isConnected? <code>true</code> if we are currently connected (based on
	 * the most recent connection callback).
	 * <p>
	 * EPICS_V3_PV also runs notifyAll() on <code>this</code> whenever the
	 * connected flag changes to <code>true</code>.
	 */
	private volatile boolean connected = false;
	
	private boolean monitorIsDestroyed = false;
	
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
     * Mapping between the bitset indexes and the full name of the field.
     * For example, bit 7 could be display.units aka EGU
     */
    private ArrayList<String> bit2fieldNames = new ArrayList<String>();
    
    /**
     * The bitset bits for the timestamp; we use this to see if record processing happened.
     * This is all the bits that could indicate if the timestamp has changed.
     * We automatically add 0 to this bitset.
     * Normally, for EPICS PVAccess PV's the timestamp is in the top level structure; so we add that and all the child fields of the timestamp.
     */
    private BitSet timeStampBits = new BitSet();
    
    /**
     * Current value of the fields; V3 name of the field to the string version of the value.
     * For example, EGU => KiloJoules
     */
    private Map<String, String> currentFieldValues = new HashMap<String, String>();

    /**
     * Map the V4 names to the V3 names. 
     */
    static private HashMap<String, String> v4FieldNames2v3FieldNames = new HashMap<String, String>();
    static {
    	v4FieldNames2v3FieldNames.put("display.limitLow", "LOPR");
    	v4FieldNames2v3FieldNames.put("display.limitHigh", "HOPR");
    	v4FieldNames2v3FieldNames.put("display.description", "DESC");
    	v4FieldNames2v3FieldNames.put("display.format", null);
    	v4FieldNames2v3FieldNames.put("display.units", "EGU");
    	v4FieldNames2v3FieldNames.put("control.limitLow", "DRVL");
    	v4FieldNames2v3FieldNames.put("control.limitHigh", "DRVH");
    	v4FieldNames2v3FieldNames.put("control.minStep", "PREC");
    	v4FieldNames2v3FieldNames.put("valueAlarm.lowAlarmLimit", "LOLO");
    	v4FieldNames2v3FieldNames.put("valueAlarm.lowWarningLimit", "LOW");
    	v4FieldNames2v3FieldNames.put("valueAlarm.highWarningLimit", "HIGH");
    	v4FieldNames2v3FieldNames.put("valueAlarm.highAlarmLimit", "HIHI");
    	v4FieldNames2v3FieldNames.put("valueAlarm.lowAlarmSeverity", null);
    	v4FieldNames2v3FieldNames.put("valueAlarm.lowWarningSeverity", null);
    	v4FieldNames2v3FieldNames.put("valueAlarm.highWarningSeverity", null);
    	v4FieldNames2v3FieldNames.put("valueAlarm.highAlarmSeverity", null);
    	v4FieldNames2v3FieldNames.put("valueAlarm.hysteresis", "HYST");
    }
    
	/**
	 *  The field values changed for this event.
	 */
	private HashMap<String, String> changedFieldValuesForThisEvent = new HashMap<String, String>();
	
	/**we save all meta field once every day and lastTimeStampWhenSavingarchiveFields is when we save all last meta fields*/
	private long archiveFieldsSavedAtEpSec = 0;
	
	/**
	 * the ioc host name where this pv is 
	 */
	private String hostName;

	private Monitor subscription = null;

	EPICS_V4_PV(final String name, ConfigService configservice, boolean isControlPV, ArchDBRTypes archDBRTypes, int jcaCommandThreadId) {
		this(name, configservice, jcaCommandThreadId);
		this.archDBRType = archDBRTypes;
		if(archDBRTypes != null) { 
			this.con = configservice.getArchiverTypeSystem().getJCADBRConstructor(this.archDBRType);
		}
	}
	
	EPICS_V4_PV(final String name, ConfigService configservice, int jcaCommandThreadId) {
		this.name = name;
		this.configservice = configservice;
		this.jcaCommandThreadId = jcaCommandThreadId;
	}
	

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void addListener(PVListener listener) {
		listeners.add(listener);
		if (running && isConnected()) { 
			listener.pvValueUpdate(this);
		}
	}

	@Override
	public void removeListener(PVListener listener) {
		listeners.remove(listener);
	}
	
	/** Notify all listeners. */
	private void fireDisconnected() {
		for (final PVListener listener : listeners) {
			listener.pvDisconnected(this);
		}
	}



	/** Notify all listeners. */
	private void fireValueUpdate() {
		for (final PVListener listener : listeners) {
			listener.pvValueUpdate(this);
		}
	}




	@Override
	public void start() throws Exception {
		if (running) {
			return;
		}

		running = true;
		this.connect();
	}

	@Override
	public void stop() {
		running = false;
		this.scheduleCommand(new Runnable() {
			@Override
			public void run() {
				unsubscribe();
				disconnect();
			}
		});
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	public boolean isConnected() {
		return connected;
	}

	@Override
	public DBRTimeEvent getDBRTimeEvent() {
		return this.dbrtimeevent;
	}

	@Override
	public ArchDBRTypes getArchDBRTypes() {
		return archDBRType;
	}

	@Override
	public void markPVHasMetafields(boolean hasMetaField) {
		// We don't use this for PVAccess
	}

	@Override
	public void setMetaFieldParentPV(PV parentPV, boolean isRuntimeOnly) {
		// We don't use this for PVAccess
	}

	@Override
	public void updataMetaFieldValue(String pvName, String fieldValue) {
		// We don't use this for PVAccess
	}

	@Override
	public HashMap<String, String> getLatestMetadata() {
		HashMap<String, String> retVal = new HashMap<String, String>();
		// The totalMetaInfo is updated once every 24hours...
		MetaInfo metaInfo = this.totalMetaInfo;
		if(metaInfo != null) {
			metaInfo.addToDict(retVal);
		}
		// Add the latest value of the fields we are monitoring.
		if(currentFieldValues != null) { 
			retVal.putAll(currentFieldValues);
		}
		return retVal;
	}

	@Override
	public void updateTotalMetaInfo() throws IllegalStateException {
		// We should not need to do anyting here as we should get updates on any field change for PVAccess and we do not need to do an explicit get. 
	}

	@Override
	public String getHostName() {
		return hostName;
	}

	@Override
	public void getLowLevelChannelInfo(List<Map<String, String>> statuses) {
		return;
	}
	
	@Override
	public String getRequesterName() {
		return this.getClass().getName() + "\tchannelName:" + this.name;
	}
	
	private static HashMap<MessageType, Level> lvl2lvl = getPVAccessMessageType2Log4jLevels();
	private static HashMap<MessageType, Level> getPVAccessMessageType2Log4jLevels() { 
		HashMap<MessageType, Level> ret = new HashMap<MessageType, Level>();
		ret.put(MessageType.info, Level.INFO);
		ret.put(MessageType.warning, Level.WARN);
		ret.put(MessageType.error, Level.ERROR);
		ret.put(MessageType.fatalError, Level.FATAL);
		return ret;
	}

	@Override
	public void message(String message, MessageType mtype) {
		logger.log(lvl2lvl.get(mtype), message);
	}

	@Override
	public void monitorConnect(Status status, Monitor channelMonitor, Structure structure) {
		if (monitorIsDestroyed)
			return;

		synchronized (this) {
			if (status.isSuccess()) {
				logger.debug("monitorConnect:" + "connect successfully");
				String structureID = structure.getID();
				logger.debug("Type from structure in monitorConnect is " + structureID);
				
				Field valueField = structure.getField("value");
				if(valueField == null) { 
					archDBRType = ArchDBRTypes.DBR_V4_GENERIC_BYTES;
				} else {
					logger.debug("Value field in monitorConnect is of type " + valueField.getID());					
					archDBRType = this.determineDBRType(structureID, valueField.getID());
				}
				
				con = configservice.getArchiverTypeSystem().getV4Constructor(archDBRType);
				logger.debug("Determined ArchDBRTypes for " + this.name + " as " + archDBRType);

				channelMonitor.start();
				this.notify();
			} else {
				logger.debug("monitorConnect:" + "connect failed");
			}
		}
	}

	@Override
	public void monitorEvent(Monitor monitor) {
		MonitorElement monitorElement = null;
		try {
			if (monitorIsDestroyed)
				return;

			if (!running) {
				return;
			}

			if (subscription == null) {
				return;
			}

			state = PVConnectionState.GotMonitor;

			monitorElement = monitor.poll();

			while (monitorElement != null)  { 
				if(logger.isDebugEnabled()) { 
					logger.debug("Obtained monitor event for pv " + this.name);
				}
				
				if(archDBRType == null || con == null) { 
					logger.error("Have not determined the DBRTYpes yet for " + this.name);
					return;
				}

				PVStructure totalPVStructure = monitorElement.getPVStructure();
	    		logger.debug("Changed bitset: " + monitorElement.getChangedBitSet());
				
				try { 
					dbrtimeevent = con.newInstance(totalPVStructure);
					totalMetaInfo.computeRate(dbrtimeevent);
					
					BitSet bs = monitorElement.getChangedBitSet();
		    		for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
		    			
		    			String fName = bit2fieldNames.get(i);
		    			// We filter => map => filter => compare => save
						if(fName.equals("") || fName.startsWith("value.") || fName.startsWith("timeStamp.") || fName.startsWith("alarm.")) {
		    				// logger.fine("Filtering out field that is already stored with event " + fName);
		    			} else {
		    				logger.debug("Field " + fName + " has changed");
		    				updateCurrentFieldValueForKey(monitorElement.getPVStructure(), fName);
		    			}
		    		}
		    		
		    		// Check to see if we got the monitor-event as part of record processing.
		    		// We use the timestamp to ascertain this fact.
		    		// We store fields as part of the next record processing event.
		    		// If this is not a record processing event, skip this.
		    		if(!bs.intersects(this.timeStampBits)) {
		    			logger.debug("Timestamp has not changed; most likely this is a update to the properties for pv " + this.name);
		    			logger.debug("Timestamp bits " + this.timeStampBits + " Changed bits " + monitorElement.getChangedBitSet());
//		    			for(PVField fld : totalPVStructure.getPVFields()) {
//		    				logger.debug("Field " + fld.getFieldName() + " has offset " + fld.getFieldOffset());
//		    			}
		    			return;
		    		}
		    		
		    		if(!this.changedFieldValuesForThisEvent.isEmpty()) {
		    			for(String key: this.changedFieldValuesForThisEvent.keySet()) {
		    				String value = this.changedFieldValuesForThisEvent.get(key);
		    				dbrtimeevent.addFieldValue(key, value);
		    			}
		    		}
		    		
		    		
					// //////////////////////////
					// ////////////save all the fields once every day//////////////
					if (this.archiveFieldsSavedAtEpSec <= 0) {
						if (currentFieldValues.size() != 0) {
							saveMetaDataOnceEveryDay();
						}
					} else {
						long nowES = TimeUtils.getCurrentEpochSeconds();
						if ((nowES - archiveFieldsSavedAtEpSec) >= 86400) {
							saveMetaDataOnceEveryDay();
						}
					}



					fireValueUpdate();
										
				} catch (Exception e) {
					logger.error("exception in monitor changed function when converting DBR to dbrtimeevent", e);
				} finally {
					monitor.release(monitorElement);
				}

				if (!connected)
					connected = true;

				monitorElement = monitor.poll();
			}

		} catch (final Exception ex) {
			logger.error("exception in monitor changed ", ex);
		}
	}

	@Override
	public void unlisten(Monitor monitor) {
		monitor.stop();
		monitor.destroy();
	}

	@Override
	public void channelCreated(Status status, Channel createdChannel) {
		logger.info("Channel has been created" + createdChannel.getChannelName() + " Status: " + status.toString());
	}

	@Override
	public void channelStateChange(final Channel channelChangingState, final org.epics.pvaccess.client.Channel.ConnectionState connectionStatus) {
		this.scheduleCommand(new Runnable() {
			@Override
			public void run() {
				if (connectionStatus == ConnectionState.CONNECTED) {
					logger.info("channelStateChange:connected " + channelChangingState.getChannelName());
					handleConnected(channelChangingState);
				} else if (connectionStatus == ConnectionState.DISCONNECTED) {
					logger.info("channelStateChange:disconnected " + channelChangingState.getChannelName());
					state = PVConnectionState.Disconnected;
					connected = false;
					unsubscribe();
					fireDisconnected();
				}
			}
		});
	}

	@Override
	public void channelGetConnect(final Status status, final ChannelGet channelGet, Structure arg2) {
		this.scheduleCommand(new Runnable() {
			@Override
			public void run() {
				if (status.isSuccess()) {
					channelGet.get();
				} else {
					System.err.println(status.getMessage());
				}
			}
		});
	}

	@Override
	public void getDone(final Status status, ChannelGet arg1, final PVStructure pvStructure, BitSet arg3) {
		logger.debug("Construct the bitset to field name mapping for PV " + this.getName());
		this.timeStampBits.set(0, true);
		add2BitFieldMapping("", pvStructure.getStructure(), bit2fieldNames);
		if(this.timeStampBits.isEmpty()) {
			logger.error("Cannot determine the timestamp bitset for PV " + this.name + ". This means we may not save any data at all for this PV.");
		} else {
			logger.debug("The timestamp bits for the PV " + this.name + " are " + this.timeStampBits);
		}
		updateCurrentFieldValues(null, pvStructure);

		this.scheduleCommand(new Runnable() {
			@Override
			public void run() {
				if (status.isSuccess()) {
					logger.info("Obtained  meta info for PV " + EPICS_V4_PV.this.name);
					totalMetaInfo.applyV4BasicInfo(EPICS_V4_PV.this.name, pvStructure, EPICS_V4_PV.this.configservice);
					EPICS_V4_PV.this.subscribe();
				}
			}
		});
	}
	
	private void scheduleCommand(final Runnable command) {
		configservice.getEngineContext().getJCACommandThread(jcaCommandThreadId).addCommand(command);
	}
	
	private void connect() {
		logger.info("Connecting to PV " + this.name);
		this.scheduleCommand(new Runnable() {
			@Override
			public void run() {
				try {
					state = PVConnectionState.Connecting;
					synchronized (this) {
						if (channel == null) {
							channel = configservice.getEngineContext().getChannelProvider().createChannel(name, EPICS_V4_PV.this, ChannelProvider.PRIORITY_DEFAULT);
						}

						if (channel == null)
							return;

						if (channel.getConnectionState() == ConnectionState.CONNECTED) {
							handleConnected(channel);
						}
					}
				} catch (Exception e) {
					logger.error("exception when connecting pv", e);
				}
			}
		});
	}



	/**
	 * PV is connected. Get meta info, or subscribe right away.
	 */
	private void handleConnected(final Channel channel) {
		if (state == PVConnectionState.Connected)
			return;

		state = PVConnectionState.Connected;

		for (final PVListener listener : listeners) {
			listener.pvConnected(this);
		}

		if (!running) {
			connected = true;
			synchronized (this) {
				this.notifyAll();
			}
			return;
		}

		PVStructure pvRequest = CreateRequest.create().createRequest("field()"); 
		channel.createChannelGet(this, pvRequest);
	}

	private void disconnect() {
		Channel channel_copy;
		synchronized (this) {
			if (channel == null)
				return;
			channel_copy = channel;
			connected = false;
			channel = null;
		}

		try {
			channel_copy.destroy();
		} catch (final Throwable e) {
			logger.error("exception when disconnecting pv", e);
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
			if (channel == null) {
				return;
			}

			try {
				state = PVConnectionState.Subscribing;
				totalMetaInfo.setStartTime(System.currentTimeMillis());
				PVStructure pvRequest = CreateRequest.create().createRequest("field()"); 
				subscription = channel.createMonitor(this, pvRequest);
			} catch (final Exception ex) {
				logger.error("exception when subscribing pv", ex);
			}
		}
	}



	/** Unsubscribe from value updates. */
	private void unsubscribe() {
		Monitor sub_copy;
		synchronized (this) {
			sub_copy = subscription;
			subscription = null;
			archDBRType = null;
			con = null;
		}

		if (sub_copy == null) {
			return;
		}

		try {
			sub_copy.stop();
			sub_copy.destroy();
		} catch (final Exception ex) {
			logger.error("exception when unsubscribing pv", ex);
		}
	}
	
	private void saveMetaDataOnceEveryDay() {
		HashMap<String, String> tempHashMap = new HashMap<String, String>();
		tempHashMap.putAll(currentFieldValues);
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
		archiveFieldsSavedAtEpSec = TimeUtils.getCurrentEpochSeconds();
	}

	
	
	private ArchDBRTypes determineDBRType(String structureID, String valueTypeId) { 
		if(structureID == null || valueTypeId == null) { 
			return ArchDBRTypes.DBR_V4_GENERIC_BYTES;
		}

		if(structureID.contains("epics:nt/NTScalarArray") || structureID.contains("structure")) {
			switch(valueTypeId) { 
			case "string[]":
				return ArchDBRTypes.DBR_WAVEFORM_STRING;
			case "double[]":
				return ArchDBRTypes.DBR_WAVEFORM_DOUBLE;
			case "int[]":
				return ArchDBRTypes.DBR_WAVEFORM_INT;
			case "byte[]":
				return ArchDBRTypes.DBR_WAVEFORM_BYTE;
			case "float[]":
				return ArchDBRTypes.DBR_WAVEFORM_FLOAT;
			case "short[]":
				return ArchDBRTypes.DBR_WAVEFORM_SHORT;
			case "enum_t":
				return ArchDBRTypes.DBR_WAVEFORM_ENUM;
			case "structure":
				return ArchDBRTypes.DBR_V4_GENERIC_BYTES;
			default:
				logger.error("Cannot determine arch dbrtypes for " + structureID + " and " + valueTypeId + " for PV " + this.name);
				return ArchDBRTypes.DBR_V4_GENERIC_BYTES;
			}
		} else {
			switch(valueTypeId) { 
			case "string":
				return ArchDBRTypes.DBR_SCALAR_STRING;
			case "double":
				return ArchDBRTypes.DBR_SCALAR_DOUBLE;
			case "int":
				return ArchDBRTypes.DBR_SCALAR_INT;
			case "byte":
				return ArchDBRTypes.DBR_SCALAR_BYTE;
			case "float":
				return ArchDBRTypes.DBR_SCALAR_FLOAT;
			case "short":
				return ArchDBRTypes.DBR_SCALAR_SHORT;
			case "enum_t":
				return ArchDBRTypes.DBR_SCALAR_ENUM;
			case "structure":
				return ArchDBRTypes.DBR_V4_GENERIC_BYTES;
			default:
				logger.error("Cannot determine arch dbrtypes for " + structureID + " and " + valueTypeId + " for PV " + this.name);
				return ArchDBRTypes.DBR_V4_GENERIC_BYTES;
			}
		}
	}
	
	/***
	 *get  the meta info for this pv 
	 * @return MetaInfo 
	 */
	@Override
	public MetaInfo getTotalMetaInfo() {
		return totalMetaInfo;
	}
	
    private String makeFullFieldName(String... parts) {
    	StringWriter buf = new StringWriter();
    	boolean firstDone = false;
    	for(String part : parts) {
    		if(part == null || part.isEmpty()) { continue; }
    		if(firstDone) {
    			buf.write(".");
    		} else {
    			firstDone = true;
    		}
			buf.write(part);
    	}
    	return buf.toString();
    }
        
    private void add2BitFieldMapping(String fldName, Field fld, ArrayList<String> mapping) {
        switch(fld.getType()) {
            case scalar:
            case scalarArray:
                mapping.add(fldName);
                break;
            case structure:
                mapping.add(fldName);
                for(String fieldName : ((Structure)fld).getFieldNames()) {
                    String fulFldName = makeFullFieldName(fldName, fieldName);
                    if(fulFldName.startsWith("timeStamp")) {
                    	this.timeStampBits.set(mapping.size(), true);
                    }
					add2BitFieldMapping(fulFldName, ((Structure)fld).getField(fieldName), mapping);
                }
                break;
            case structureArray:
            	break;
            case union:
            	break;
            case unionArray:
            	break;
            default:
            	break;
        }
    }
    
    private String getScalarField(PVField pvField) {
    	switch(((PVScalar)pvField).getScalar().getScalarType()) {
		case pvBoolean:
			return Boolean.toString(((PVBoolean)pvField).get());
		case pvByte:
			return Byte.toString(((PVByte)pvField).get());
		case pvDouble:
			return Double.toString(((PVDouble)pvField).get());
		case pvFloat:
			return Float.toString(((PVFloat)pvField).get());
		case pvInt:
			return Integer.toString(((PVInt)pvField).get());
		case pvLong:
			return Long.toString(((PVLong)pvField).get());
		case pvShort:
			return Short.toString(((PVShort)pvField).get());
		case pvString:
			return ((PVString)pvField).get();
		case pvUByte:
			return Byte.toString(((PVUByte)pvField).get());
		case pvUInt:
			return Integer.toString(((PVUInt)pvField).get());
		case pvULong:
			return Long.toString(((PVULong)pvField).get());
		case pvUShort:
			return Short.toString(((PVUShort)pvField).get());
		default:
			throw new UnsupportedOperationException();    	
    	}
    }

    private void updateCurrentFieldValues(String rootName, PVStructure pvStructure) {
    	for(PVField pvField: pvStructure.getPVFields()) {
    		String fieldName = pvField.getFieldName();
    		if(fieldName.equals("value")) continue;
            switch(pvField.getField().getType()) {
            case scalar:
            	this.currentFieldValues.put(makeFullFieldName(rootName , pvField.getFieldName()), getScalarField(pvField));
            	break;
            case scalarArray:
    			break;
            case structure:
            	updateCurrentFieldValues(makeFullFieldName(rootName, pvField.getFieldName()), ((PVStructure)pvField));
                break;
            case structureArray:
    			break;
            case union:
    			break;
            case unionArray:
    			break;
            default:
    			break;
            }
    	}
    }
    
    private void updateCurrentFieldValueForKey(PVStructure pvStructure, String fullFieldName) {
    	// logger.info("Updating current value for " + fullFieldName);
    	if(fullFieldName.trim().isEmpty()) {
    		return;
    	}
    	
		// Map to EPICS v3 Names.
		String v3fName = v4FieldNames2v3FieldNames.get(fullFieldName);
		if(v3fName == null) {
			return;
		}
	
    	String[] parts = fullFieldName.split("\\.");
    	for(int i = 0; i < parts.length; i++) {
    		if(i < parts.length-1) {
    			pvStructure = pvStructure.getStructureField(parts[i]);
    		} else {
    			String val = getScalarField(pvStructure.getSubField(parts[i]));
    			if(this.currentFieldValues.containsKey(v3fName)) {
    				if(!this.currentFieldValues.get(v3fName).equals(val)) {
            			logger.info("Setting value of " + v3fName + " to " + val);
            			this.currentFieldValues.put(v3fName, val);
            			this.changedFieldValuesForThisEvent.put(v3fName, val);
    				}
    			} else {
        			logger.info("Setting value of " + v3fName + " to " + val);
        			this.currentFieldValues.put(v3fName, val);
        			this.changedFieldValuesForThisEvent.put(v3fName, val);
    			}    			
    		}
    	}
    }

	@Override
	public void sampleWrittenIntoStores() {
		this.changedFieldValuesForThisEvent.clear();
	}
}
