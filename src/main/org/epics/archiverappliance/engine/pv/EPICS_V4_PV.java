package org.epics.archiverappliance.engine.pv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.pva.client.ClientChannelListener;
import org.epics.pva.client.ClientChannelState;
import org.epics.pva.client.MonitorListener;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVAStructure;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class EPICS_V4_PV implements PV, ClientChannelListener, MonitorListener {
    private static final Logger logger = LogManager.getLogger(EPICS_V4_PV.class.getName());

    /** Channel name. */
    private final String name;

    /**the meta info for this pv*/
    private final MetaInfo totalMetaInfo = new MetaInfo();

    private PVConnectionState state = PVConnectionState.Idle;

    private PVAChannel pvaChannel;

    /**configservice used by this pv*/
    private final ConfigService configservice;

    /** PVListeners of this PV */
    private final CopyOnWriteArrayList<PVListener> listeners = new CopyOnWriteArrayList<>();

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

    /**the ArchDBRTypes of this pv*/
    private ArchDBRTypes archDBRType = null;

    /**
     * The JCA command thread that processes actions for this PV.
     * This should be inherited from the ArchiveChannel.
     */
    private final int jcaCommandThreadId;

    /**
     * The bitset bits for the timestamp; we use this to see if record processing happened.
     * This is all the bits that could indicate if the timestamp has changed.
     * We automatically add 0 to this bitset.
     * Normally, for EPICS PVAccess PV's the timestamp is in the top level structure; so we add that and all the child fields of the timestamp.
     */
    private BitSet timeStampBits = new BitSet();

    /**
     *  The field values changed for this event.
     */
    private FieldValuesCache fieldValuesCache;
    /**
     *  The field values changed for this event.
     */
    private final List<String> metaFields = new ArrayList<>();

    /**we save all meta field once every day and lastTimeStampWhenSavingarchiveFields is when we save all last meta fields*/
    private long archiveFieldsSavedAtEpSec = 0;

    /**
     * the ioc host name where this pv is
     */
    private String hostName;

    private AutoCloseable subscriptionCloseable = null;

    EPICS_V4_PV(
            final String name,
            ConfigService configservice,
            boolean isControlPV,
            ArchDBRTypes archDBRTypes,
            int jcaCommandThreadId) {
        this(name, configservice, jcaCommandThreadId);
        this.archDBRType = archDBRTypes;
        if (archDBRTypes != null) {
            this.con = configservice.getArchiverTypeSystem().getV4Constructor(this.archDBRType);
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
    private void fireConnected() {
        for (final PVListener listener : listeners) {
            listener.pvConnected(this);
        }
    }

    /** Notify all listeners. */
    private void fireValueUpdate(DBRTimeEvent ev) {
        for (final PVListener listener : listeners) {
            listener.pvValueUpdate(this, ev);
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
        this.scheduleCommand(() -> {
            unsubscribe();
            disconnect();
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

    /**
     * @return
     */
    @Override
    public PVConnectionState connectionState() {
        return this.state;
    }

    @Override
    public ArchDBRTypes getArchDBRTypes() {
        return archDBRType;
    }

    @Override
    public HashMap<String, String> getLatestMetadata() {
        HashMap<String, String> retVal = new HashMap<>();
        // The totalMetaInfo is updated once every 24hours...
        MetaInfo metaInfo = this.totalMetaInfo;
        if (metaInfo != null) {
            metaInfo.addToDict(retVal);
        }
        if (fieldValuesCache != null) {
            retVal.putAll(fieldValuesCache.getCurrentFieldValues());
        }
        return retVal;
    }

    @Override
    public void updateTotalMetaInfo() throws IllegalStateException {
        // We should not need to do anyting here as we should get updates on any field change for PVAccess and we do not
        // need to do an explicit get.
    }

    @Override
    public String getHostName() {
        return hostName;
    }

    @Override
    public void getLowLevelChannelInfo(List<Map<String, String>> statuses) {
        // We don't use this for PVAccess

    }

    @Override
    public void channelStateChanged(PVAChannel channel, ClientChannelState clientChannelState) {

        logger.info(channel.getName() + " channelStateChanged:" + clientChannelState);
        if (clientChannelState == ClientChannelState.CONNECTED) {
            this.scheduleCommand(this::handleConnected);
        } else if (connected && clientChannelState != ClientChannelState.FOUND) {
            this.scheduleCommand(this::handleDisconnected);
        }
    }


    private void setupDBRType(PVAStructure data) {
        logger.debug("Construct the fieldValuesCache for PV " + this.getName());
        boolean excludeV4Changes = true;
        this.fieldValuesCache = new FieldValuesCache(data, excludeV4Changes);
        this.timeStampBits = this.fieldValuesCache.getTimeStampBits();
        if (this.timeStampBits.isEmpty()) {
            logger.error("Cannot determine the timestamp bitset for PV " + this.name
                    + ". This means we may not save any data at all for this PV.");
        } else {
            logger.debug("The timestamp bits for the PV " + this.name + " are " + this.timeStampBits);
        }

        if (archDBRType == null || con == null) {
            String structureID = data.formatType();
            logger.debug("Type from structure in monitorConnect is " + structureID);

            PVAData valueField = data.get("value");
            if (valueField == null) {
                archDBRType = ArchDBRTypes.DBR_V4_GENERIC_BYTES;
            } else {
                logger.debug("Value field in monitorConnect is of type " + valueField.getType());
                archDBRType = determineDBRType(structureID, valueField.getType(), valueField.formatType());
            }

            con = configservice.getArchiverTypeSystem().getV4Constructor(archDBRType);
            logger.debug("Determined ArchDBRTypes for " + this.name + " as " + archDBRType);
        }
    }

    private static boolean timeStampUpdated(BitSet changes, BitSet timeStampBits) {
        if (!timeStampBits.isEmpty()) {
            return changes.intersects(timeStampBits);
        }
        return false;
    }

    private boolean newMetaDataSavePeriod(long lastSaveSecs, long periodLengthSecs) {
        long nowES = TimeUtils.getCurrentEpochSeconds();
        return lastSaveSecs <= 0 || (nowES - lastSaveSecs) >= periodLengthSecs;
    }

    private DBRTimeEvent fromStructure(PVAStructure data, BitSet changes) throws Exception {

        DBRTimeEvent dbrtimeevent = con.newInstance(data);
        this.totalMetaInfo.computeRate(dbrtimeevent);

        this.fieldValuesCache.updateFieldValues(data, changes);
        dbrtimeevent.setFieldValues(this.fieldValuesCache.getUpdatedFieldValues(false, this.metaFields), false);

        return dbrtimeevent;
    }

    @Override
    public void handleMonitor(PVAChannel channel, BitSet changes, BitSet overruns, PVAStructure data) {
        logger.debug("handleMonitor: {}", data);
        if (data == null) {
            logger.warn("Server ends subscription for " + this.name);
            this.scheduleCommand(this::handleDisconnected);
        }

        state = PVConnectionState.GotMonitor;

        if (!connected) connected = true;

        if (logger.isDebugEnabled()) {
            logger.debug("Obtained monitor event for pv " + this.name);
        }

        if (archDBRType == null || con == null) {
            logger.error("Have not determined the DBRTYpes yet for " + this.name);
            this.setupDBRType(data);
        }

        logger.debug("Changed bitset: " + changes);

        try {

            // Check to see if we got the monitor-event as part of record processing.
            // We use the timestamp to ascertain this fact.
            // We store fields as part of the next record processing event.
            // If this is not a record processing event, skip this.
            if (!timeStampUpdated(changes, this.timeStampBits)) {
                logger.debug("Timestamp has not changed; most likely this is a update to the properties for pv "
                        + this.name);
                logger.debug("Timestamp bits " + this.timeStampBits + " Changed bits " + changes);
                return;
            }

            DBRTimeEvent dbrtimeevent = fromStructure(data, changes);

            // Update listeners
            fireValueUpdate(dbrtimeevent);

        } catch (Exception e) {
            logger.error("exception in monitor changed function when converting DBR to dbrtimeevent", e);
        }
    }

    private void scheduleCommand(final Runnable command) {
        configservice.getEngineContext().getJCACommandThread(jcaCommandThreadId).addCommand(command);
    }

    private void connect() {
        logger.debug("Connecting to PV " + this.name);
        this.scheduleCommand(new Runnable() {
            @Override
            public void run() {
                try {
                    state = PVConnectionState.Connecting;
                    synchronized (this) {
                        if (pvaChannel == null) {
                            pvaChannel = configservice
                                    .getEngineContext()
                                    .getPVAClient()
                                    .getChannel(name, EPICS_V4_PV.this);
                        }

                        if (pvaChannel == null) {
                            logger.error("No pvaChannel when trying to connect to pv " + name);
                            return;
                        }

                        if (pvaChannel.isConnected()) {
                            handleConnected();
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
    private void handleConnected() {
        if (state == PVConnectionState.Connected) return;

        state = PVConnectionState.Connected;

        fireConnected();

        if (!running) {
            synchronized (this) {
                connected = true;
                this.notifyAll();
            }
            return;
        }

        subscribe();
    }

    private void disconnect() {
        PVAChannel channelCopy;
        synchronized (this) {
            if (pvaChannel == null) return;
            channelCopy = pvaChannel;
            connected = false;
            pvaChannel = null;
        }

        try {
            channelCopy.close();
        } catch (final Exception e) {
            logger.error("exception when disconnecting pv", e);
        }

        fireDisconnected();
    }

    private void handleDisconnected() {
        if (state == PVConnectionState.Disconnected) return;
        state = PVConnectionState.Disconnected;
        synchronized (this) {
            connected = false;
        }

        unsubscribe();
        fireDisconnected();
    }

    /** Subscribe for value updates. */
    private void subscribe() {
        synchronized (this) {
            // Prevent multiple subscriptions.
            if (subscriptionCloseable != null) {
                logger.error("When trying to establish a subscription, subscription already exists " + this.name);
                return;
            }

            // Late callback, channel already closed?
            if (pvaChannel == null) {
                logger.error("When trying to establish a subscription, channel already closed " + this.name);
                return;
            }

            try {
                var pvaStructure = pvaChannel.read("").get();
                this.setupDBRType(pvaStructure);
                DBRTimeEvent dbrTimeEvent = fromStructure(pvaStructure, null);
                saveAllMetaData(dbrTimeEvent);
                fireValueUpdate(dbrTimeEvent);
            } catch (Exception e) {
                logger.error("exception when reading pv", e);
            }
            try {
                if (pvaChannel.getState() != ClientChannelState.CONNECTED) {
                    logger.error("When trying to establish a subscription, the PVA channel is not connected for "
                            + this.name);
                    return;
                }
                state = PVConnectionState.Subscribing;
                totalMetaInfo.setStartTime(System.currentTimeMillis());
                int pipeline = 0;
                subscriptionCloseable = pvaChannel.subscribe("", pipeline, this);
            } catch (final Exception ex) {
                logger.error("exception when subscribing pv", ex);
            }
        }
    }

    /** Unsubscribe from value updates. */
    private void unsubscribe() {
        AutoCloseable subCopy;
        synchronized (this) {
            subCopy = subscriptionCloseable;
            subscriptionCloseable = null;
            archDBRType = null;
            con = null;
        }

        if (subCopy == null) {
            return;
        }

        try {
            subCopy.close();
        } catch (final Exception ex) {
            logger.error("exception when unsubscribing pv", ex);
        }
    }

    private static HashMap<String, String> metaInfoToStore(MetaInfo totalMetaInfo) {
        HashMap<String, String> tempHashMap = new HashMap<>();
        if (totalMetaInfo != null) {
            if (totalMetaInfo.getUnit() != null) {
                tempHashMap.put("EGU", totalMetaInfo.getUnit());
            }
            if (totalMetaInfo.getPrecision() != 0) {
                tempHashMap.put("PREC", Integer.toString(totalMetaInfo.getPrecision()));
            }
        }
        return tempHashMap;
    }

    private static ArchDBRTypes determineDBRType(String structureID, String valueTypeId, String valueFormatType) {
        if (structureID == null || valueTypeId == null) {
            return ArchDBRTypes.DBR_V4_GENERIC_BYTES;
        }

        switch (valueTypeId) {
            case "string[]" -> {
                return ArchDBRTypes.DBR_WAVEFORM_STRING;
            }
            case "double[]" -> {
                return ArchDBRTypes.DBR_WAVEFORM_DOUBLE;
            }
            case "int[]", "uint[]" -> {
                return ArchDBRTypes.DBR_WAVEFORM_INT;
            }
            case "byte[]", "ubyte[]" -> {
                return ArchDBRTypes.DBR_WAVEFORM_BYTE;
            }
            case "float[]" -> {
                return ArchDBRTypes.DBR_WAVEFORM_FLOAT;
            }
            case "short[]", "ushort[]" -> {
                return ArchDBRTypes.DBR_WAVEFORM_SHORT;
            }
            case "enum_t[]" -> {
                return ArchDBRTypes.DBR_WAVEFORM_ENUM;
            }
            case "string" -> {
                return ArchDBRTypes.DBR_SCALAR_STRING;
            }
            case "double" -> {
                return ArchDBRTypes.DBR_SCALAR_DOUBLE;
            }
            case "int", "uint" -> {
                return ArchDBRTypes.DBR_SCALAR_INT;
            }
            case "byte", "ubyte" -> {
                return ArchDBRTypes.DBR_SCALAR_BYTE;
            }
            case "float" -> {
                return ArchDBRTypes.DBR_SCALAR_FLOAT;
            }
            case "short", "ushort" -> {
                return ArchDBRTypes.DBR_SCALAR_SHORT;
            }
            case "enum_t" -> {
                return ArchDBRTypes.DBR_SCALAR_ENUM;
            }
            case "structure[]" -> {
                if (valueFormatType.startsWith("enum_t[]")) {
                    return ArchDBRTypes.DBR_WAVEFORM_ENUM;
                }
                return ArchDBRTypes.DBR_V4_GENERIC_BYTES;
            }
            case "structure" -> {
                if (valueFormatType.startsWith("enum")) {
                    return ArchDBRTypes.DBR_SCALAR_ENUM;
                }
                return ArchDBRTypes.DBR_V4_GENERIC_BYTES;
            }
            default -> {
                logger.error("Cannot determine arch dbrtypes for " + structureID + " and " + valueTypeId);
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

    @Override
    public void sampleWrittenIntoStores() {
        // No more need for this method
    }

    /**
     * Updates field values when about to write to the buffer.
     * @param lastEvent Last event got.
     */
    @Override
    public void aboutToWriteBuffer(DBRTimeEvent lastEvent) {
        // save all the fields once every period
        // 24 hours
        int saveMetaDataPeriodSecs = 86400;
        if (newMetaDataSavePeriod(this.archiveFieldsSavedAtEpSec, saveMetaDataPeriodSecs)) {
            saveAllMetaData(lastEvent);
        }
    }

    private void saveAllMetaData(DBRTimeEvent lastEvent) {
        HashMap<String, String> fieldValues = new HashMap<>();
        fieldValues.putAll(metaInfoToStore(totalMetaInfo));
        fieldValues.putAll(fieldValuesCache.getUpdatedFieldValues(true, this.metaFields));
        this.archiveFieldsSavedAtEpSec = TimeUtils.getCurrentEpochSeconds();
        lastEvent.setFieldValues(fieldValues, false);
    }

    /**
     * Add a meta field to archive along with the value.
     * @param fieldName Name of the meta field.
     */
    public void addMetaField(String fieldName) {
        metaFields.add(fieldName);
    }
}
