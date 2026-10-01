package org.epics.archiverappliance.engine.pv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.pva.client.ClientChannelListener;
import org.epics.pva.client.ClientChannelState;
import org.epics.pva.client.MonitorListener;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.RecordOptions;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVAStructure;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class EPICS_V4_PV implements PV, ClientChannelListener, MonitorListener {
    private static final Logger logger = LogManager.getLogger(EPICS_V4_PV.class.getName());

    /** Channel name. */
    private final String name;

    /**the meta info for this pv*/
    private final MetaInfo totalMetaInfo = new MetaInfo();

    private volatile PVConnectionState state = PVConnectionState.Idle;

    private final AtomicReference<PVAChannel> pvaChannelReference = new AtomicReference<>();

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

    private volatile AutoCloseable subscriptionCloseable = null;

    /** Low Level Details */
    private final AtomicLong lastMonitorSecs = new AtomicLong();

    private final AtomicLong monitorEventCount = new AtomicLong();
    private final AtomicLong connectCallbackCount = new AtomicLong();
    private final AtomicLong disconnectCallbackCount = new AtomicLong();
    private final AtomicLong transientErrorCount = new AtomicLong();
    private volatile String lastReadError = "";
    private final PVAConfiguration pvaConfiguration;

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
        this.pvaConfiguration = new PVAConfiguration(configservice);
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
        logLifecycle("stop");
        this.scheduleCommand("stop", () -> {
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
        PVAChannel channelSnapshot = pvaChannelReference.get();
        AutoCloseable subscription = subscriptionCloseable;
        addLowLevelDetail(statuses, "PV connection state machine state", state.toString());
        addLowLevelDetail(statuses, "Do we have a PVA channel?", Boolean.toString(channelSnapshot != null));
        addLowLevelDetail(
                statuses,
                "PVA channel state",
                channelSnapshot == null ? "N/A" : channelSnapshot.getState().toString());
        addLowLevelDetail(
                statuses, "PVA remote address", channelSnapshot == null ? "N/A" : channelSnapshot.getRemoteAddress());
        addLowLevelDetail(
                statuses, "PVA TLS enabled?", Boolean.toString(channelSnapshot != null && channelSnapshot.isTLS()));
        addLowLevelDetail(statuses, "Do we have a subscription?", Boolean.toString(subscription != null));
        addLowLevelDetail(
                statuses, "Last monitor received at", TimeUtils.convertToHumanReadableString(lastMonitorSecs.get()));
        addLowLevelDetail(statuses, "PVA monitor event count", Long.toString(monitorEventCount.get()));
        addLowLevelDetail(statuses, "PVA connect callback count", Long.toString(connectCallbackCount.get()));
        addLowLevelDetail(statuses, "PVA disconnect callback count", Long.toString(disconnectCallbackCount.get()));
        addLowLevelDetail(statuses, "PVA transient error count", Long.toString(transientErrorCount.get()));
        addLowLevelDetail(statuses, "Last PVA read error", lastReadError.isEmpty() ? "N/A" : lastReadError);
        addLowLevelDetail(statuses, "The internal connected bool", Boolean.toString(connected));
        addLowLevelDetail(statuses, "The internal running bool", Boolean.toString(running));
        addLowLevelDetail(statuses, "Do we have a valid DBR Type constructor", Boolean.toString(con != null));
        addLowLevelDetail(statuses, "Command thread id", Integer.toString(jcaCommandThreadId));
        addLowLevelDetail(statuses, "Hostname of PV from PVA", hostName);
    }

    @Override
    public void channelStateChanged(PVAChannel channel, ClientChannelState clientChannelState) {

        logLifecycle("channelStateChanged:" + clientChannelState);
        if (clientChannelState == ClientChannelState.CONNECTED) {
            connectCallbackCount.incrementAndGet();
            scheduleCommand("handleConnected", () -> {
                if (pvaChannelReference.get() == channel) handleConnected();
            });
        } else {
            disconnectCallbackCount.incrementAndGet();
            scheduleCommand("handleDisconnected", () -> {
                if (pvaChannelReference.get() == channel && shouldHandleDisconnectedCallback(connected, state)) {
                    handleDisconnected();
                }
            });
        }
    }

    static boolean shouldHandleDisconnectedCallback(boolean connected, PVConnectionState state) {
        return connected
                || state == PVConnectionState.Connected
                || state == PVConnectionState.Subscribing
                || state == PVConnectionState.GotMonitor;
    }

    private void setupDBRType(PVAStructure data) {
        logger.debug("Construct the fieldValuesCache for PV " + this.getName());
        this.fieldValuesCache = new FieldValuesCache(data, false);
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

    static boolean timeStampUpdated(BitSet changes, BitSet timeStampBits) {
        // Bit 0 marks the entire structure as changed - as in the initial monitor snapshot on
        // (re)connect - which includes the timestamp.
        if (changes.get(0)) {
            return true;
        }
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
        dbrtimeevent.setFieldValues(this.fieldValuesCache.getUpdatedFieldValues(false), false);

        return dbrtimeevent;
    }

    @Override
    public void handleMonitor(PVAChannel channel, BitSet changes, BitSet overruns, PVAStructure data) {
        if (channel != pvaChannelReference.get() || !running) return;
        logger.debug("handleMonitor: {}", data);
        if (data == null) {
            logger.warn("Server ends subscription for " + this.name);
            transientErrorCount.incrementAndGet();
            this.scheduleCommand("monitorEnded", () -> {
                if (pvaChannelReference.get() == channel) handleDisconnected();
            });
            return;
        }

        lastMonitorSecs.set(TimeUtils.getCurrentEpochSeconds());
        monitorEventCount.incrementAndGet();
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
            logger.error(
                    "exception in monitor changed function when converting DBR to dbrtimeevent for pv " + this.name, e);
        }
    }

    private void scheduleCommand(final String label, final Runnable command) {
        configservice.getEngineContext().getJCACommandThread(jcaCommandThreadId).addCommand(label, name, command);
    }

    private void connect() {
        logger.debug("Connecting to PV " + this.name);
        logLifecycle("connect");
        this.scheduleCommand("connect", () -> {
            try {
                state = PVConnectionState.Connecting;
                synchronized (EPICS_V4_PV.this) {
                    PVAChannel channel = pvaChannelReference.get();
                    if (channel == null) {
                        var pvaClient = configservice.getEngineContext().getPVAClient();
                        if (pvaClient == null) {
                            logger.warn("PVA client is unavailable while connecting PV {}", name);
                            transientErrorCount.incrementAndGet();
                            return;
                        }
                        channel = pvaClient.getChannel(name, EPICS_V4_PV.this);
                        pvaChannelReference.set(channel);
                    }

                    if (channel == null) {
                        logger.error("No pvaChannel when trying to connect to pv " + name);
                        return;
                    }

                    if (channel.isConnected()) {
                        handleConnected();
                    }
                }
            } catch (Exception e) {
                transientErrorCount.incrementAndGet();
                logger.error("Exception when connecting pv {}", name, e);
            }
        });
    }

    /**
     * PV is connected. Get meta info, or subscribe right away.
     */
    private void handleConnected() {
        if (state == PVConnectionState.Connected
                || (subscriptionCloseable != null
                        && (state == PVConnectionState.Subscribing || state == PVConnectionState.GotMonitor))) return;

        state = PVConnectionState.Connected;
        PVAChannel channel = pvaChannelReference.get();
        if (channel != null) {
            hostName = channel.getRemoteAddress();
        }

        logLifecycle("connected");

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
            channelCopy = pvaChannelReference.get();
            if (channelCopy == null) return;
            connected = false;
            pvaChannelReference.set(null);
            state = PVConnectionState.Disconnected;
        }

        try {
            channelCopy.close();
        } catch (final Exception e) {
            transientErrorCount.incrementAndGet();
            logger.error("Exception when disconnecting pv {}", name, e);
        }

        logLifecycle("disconnect");
        fireDisconnected();
    }

    private void handleDisconnected() {
        if (state == PVConnectionState.Disconnected) return;
        state = PVConnectionState.Disconnected;
        synchronized (this) {
            connected = false;
        }

        unsubscribe();

        logLifecycle("disconnected");
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
            if (pvaChannelReference.get() == null) {
                logger.error("When trying to establish a subscription, channel already closed " + this.name);
                return;
            }
            PVAChannel pvaChannel = this.pvaChannelReference.get();

            if (pvaChannel.getState() != ClientChannelState.CONNECTED) {
                logger.debug("Skipping initial PVA read for disconnected PV {}", name);
                return;
            }

            try {
                CompletableFuture<PVAStructure> readFuture = pvaChannel.read("");
                var pvaStructure =
                        awaitInitialRead(readFuture, pvaConfiguration.pvaReadTimeoutSecs(), TimeUnit.SECONDS);
                this.setupDBRType(pvaStructure);
                DBRTimeEvent dbrTimeEvent = fromStructure(pvaStructure, null);
                saveAllMetaData(dbrTimeEvent);
                fireValueUpdate(dbrTimeEvent);
                lastReadError = "";
            } catch (TimeoutException e) {
                transientErrorCount.incrementAndGet();
                lastReadError = "TimeoutException after " + pvaConfiguration.pvaReadTimeoutSecs() + " seconds";
                logger.warn(
                        "Timed out after {} seconds reading initial PVA value for PV {} on command thread {}; continuing to subscribe",
                        pvaConfiguration.pvaReadTimeoutSecs(),
                        name,
                        jcaCommandThreadId);
            } catch (Exception e) {
                transientErrorCount.incrementAndGet();
                lastReadError = e.getClass().getSimpleName() + ": " + e.getMessage();
                logger.error("exception when reading pv {}", this.name, e);
            }
            try {
                if (pvaChannel.getState() != ClientChannelState.CONNECTED) {
                    logger.error("When trying to establish a subscription, the PVA channel is not connected for "
                            + this.name);
                    return;
                }
                state = PVConnectionState.Subscribing;
                totalMetaInfo.setStartTime(System.currentTimeMillis());
                logLifecycle("subscribe");
                int pipeline = 0;
                subscriptionCloseable = pvaChannel.subscribe(
                        "",
                        RecordOptions.builder()
                                .dbeMask(RecordOptions.DBEMask.DBE_ARCHIVE)
                                .pipeline(pipeline)
                                .build(),
                        this);
                logLifecycle("subscribed");
            } catch (final Exception ex) {
                transientErrorCount.incrementAndGet();
                logger.error("exception when subscribing pv {}", name, ex);
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
            logLifecycle("unsubscribed");
        } catch (final Exception ex) {
            transientErrorCount.incrementAndGet();
            logger.error("exception when unsubscribing pv {}", name, ex);
        }
    }

    static PVAStructure awaitInitialRead(CompletableFuture<PVAStructure> readFuture, long timeout, TimeUnit unit)
            throws Exception {
        try {
            return readFuture.get(timeout, unit);
        } catch (TimeoutException ex) {
            readFuture.cancel(true);
            throw ex;
        }
    }

    private void logLifecycle(String event) {
        logger.info("pv={} protocol=PVA thread={} event={}", name, jcaCommandThreadId, event);
    }

    private static void addLowLevelDetail(List<Map<String, String>> statuses, String name, String value) {
        Map<String, String> detail = new HashMap<>();
        detail.put("name", name);
        detail.put("value", value == null ? "N/A" : value);
        detail.put("source", "PVA");
        statuses.add(detail);
    }

    static HashMap<String, String> metaInfoToStore(MetaInfo totalMetaInfo) {
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

    static ArchDBRTypes determineDBRType(String structureID, String valueTypeId, String valueFormatType) {
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
        if (newMetaDataSavePeriod(this.archiveFieldsSavedAtEpSec, ArchiveChannel.getConfiguredMetaDataPeriodSecs())) {
            saveAllMetaData(lastEvent);
        }
    }

    private void saveAllMetaData(DBRTimeEvent lastEvent) {
        HashMap<String, String> fieldValues = new HashMap<>();
        if (lastEvent.hasFieldValues()) {
            fieldValues.putAll(lastEvent.getFields());
        }
        fieldValues.putAll(metaInfoToStore(totalMetaInfo));
        fieldValues.putAll(fieldValuesCache.getUpdatedFieldValues(true));
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
