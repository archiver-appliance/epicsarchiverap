package org.epics.archiverappliance.engine.generic;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.RejectedExecutionException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import org.apache.log4j.Logger;
import gov.aps.jca.configuration.Configuration;
import gov.aps.jca.configuration.DefaultConfigurationBuilder;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.config.TypeSystem;
import org.epics.archiverappliance.engine.writer.WriterRunnable;
import org.epics.archiverappliance.engine.epics.JCAConfigGen;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.SampleBuffer;
import org.epics.archiverappliance.engine.model.ThrottledLogger;
import org.epics.archiverappliance.engine.model.LogLevel;
import org.epics.archiverappliance.engine.model.EngineWritable;
import org.epics.archiverappliance.engine.metadata.MetaCompletedListener;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.generic.GenericEngine;
import org.epics.archiverappliance.engine.generic.GenericEngineDefinition;
import org.epics.archiverappliance.engine.generic.GenericEngineRequester;
import org.epics.archiverappliance.engine.generic.GenericChannel;
import org.epics.archiverappliance.engine.generic.GenericChannelParams;
import org.epics.archiverappliance.engine.generic.GenericChannelRequester;
import org.epics.archiverappliance.engine.generic.GenericMetaGet;
import org.epics.archiverappliance.engine.generic.GenericMetaGetRequester;
import org.epics.archiverappliance.engine.generic.ScopedLogger;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;

public class GenericEngineMediator
{
	private static final Logger logger = Logger.getLogger("GenericEngine");
	
	public GenericEngineMediator(GenericEngineDefinition engine_def, ConfigService config_service) throws Exception
	{
		m_engine_def = engine_def;
		m_config_service = config_service;
		m_logger = new ScopedLogger(ScopedLogger.parentRoot(logger), engine_def.getName());
		m_trouble_logger = new ThrottledLogger(LogLevel.error, 60);
		m_channel_logger = new ScopedLogger(ScopedLogger.parentScope(m_logger), "channel");
		m_metaget_logger = new ScopedLogger(ScopedLogger.parentScope(m_logger), "metaget");
		m_channels = new HashMap<String, MediatorChannel>();
		m_metagets = new HashMap<String, MediatorMetaGet>();
		
		m_bulk_flush_executor_threads = Integer.parseInt(getProperty("NumBulkFlushThreads", "4"));
		if (m_bulk_flush_executor_threads <= 0) {
			throw new Exception("NumBulkFlushThreads is out of range");
		}
	}
	
	public void complete() throws Exception
	{
		m_engine = m_engine_def.createEngine(m_this_requester);
		
		m_logger.info("Engine created");
	}
	
	public synchronized void destroy()
	{
		if (m_destroyed) {
			return;
		}
		
		for (MediatorChannel med_channel : m_channels.values()) {
			med_channel.destroy();
		}
		
		for (MediatorMetaGet med_metaget : m_metagets.values()) {
			med_metaget.destroy();
		}
		
		m_engine.destroy();
		
		if (m_bulk_flush_executor != null) {
			m_bulk_flush_executor.shutdown();
		}
		
		m_destroyed = true;
		
		m_logger.info("Engine destroyed");
	}
	
	public synchronized void addChannel(String channelName, String fullChannelName, float samplingPeriod, SamplingMethod mode, int secondsToBuffer, Writer writer, ArchDBRTypes dbrType, Timestamp last_archived_timestamp) throws Exception
	{
		m_logger.info(String.format("addChannel %s", channelName));
		
		this.destroyed_check();
		
		if (m_channels.containsKey(channelName)) {
			throw new Exception(String.format("Channel '%s' already exists", channelName));
		}
		
		GenericChannelParams params = new GenericChannelParams(samplingPeriod, mode, dbrType);
		
		add_channel_common(channelName, fullChannelName, writer, last_archived_timestamp, params);
	}
	
	public synchronized void changeChannelParameters(String channelName, float samplingPeriod, SamplingMethod mode, Writer writer) throws Exception
	{
		m_logger.info(String.format("changeChannelParameters %s", channelName));
		
		this.destroyed_check();
		
		MediatorChannel old_med_channel = m_channels.remove(channelName);
		if (old_med_channel == null) {
			throw new Exception(String.format("Channel '%s' does not exist", channelName));
		}
		
		old_med_channel.destroy();
		
		GenericChannelParams params = new GenericChannelParams(samplingPeriod, mode, old_med_channel.m_type);
		
		add_channel_common(channelName, old_med_channel.m_full_channel_name, writer, old_med_channel.m_last_archived_timestamp, params);
	}
	
	public synchronized void destroyChannel(String channelName) throws Exception
	{
		m_logger.info(String.format("destroyChannel %s", channelName));
		
		this.destroyed_check();
		
		MediatorChannel med_channel = m_channels.remove(channelName);
		
		if (med_channel != null) {
			med_channel.destroy();
		}
	}
	
	public synchronized boolean haveChannel(String channelName) throws Exception
	{
		m_logger.debug(String.format("haveChannel %s", channelName));
		
		this.destroyed_check();
		
		return m_channels.containsKey(channelName);
	}
	
	public synchronized void getMetaInfo(String channelName, String metadatafields[], MetaCompletedListener listener) throws Exception
	{
		m_logger.debug(String.format("getMetaInfo %s", channelName));
		
		this.destroyed_check();
		
		if (m_metagets.containsKey(channelName)) {
			throw new Exception(String.format("MetaGet already in progress for channel '%s'", channelName));
		}
		
		MediatorMetaGet med_metaget = new MediatorMetaGet(this, channelName, listener);
		
		GenericMetaGet metaget = m_engine.addMetaGet(channelName, metadatafields, med_metaget);
		
		med_metaget.complete(metaget);
		
		m_metagets.put(channelName, med_metaget);
	}
	
	public synchronized boolean abortMetaInfo(String channelName) throws Exception
	{
		m_logger.info(String.format("abortMetaInfo %s", channelName));
		
		this.destroyed_check();
		
		MediatorMetaGet med_metaget = m_metagets.remove(channelName);
		if (med_metaget == null) {
			return false;
		}
		
		med_metaget.destroy();
		
		return true;
	}
	
	public synchronized PVMetrics getChannelMetrics(String channelName) throws Exception
	{
		m_logger.debug(String.format("getChannelMetrics %s", channelName));
		
		this.destroyed_check();
		
		MediatorChannel med_channel = m_channels.get(channelName);
		if (med_channel == null) {
			return null;
		}
		
		return med_channel.m_metrics;
	}
	
	public synchronized HashMap<String, String> getChannelMetadata(String channelName) throws Exception
	{
		m_logger.debug(String.format("getChannelMetadata %s", channelName));
		
		this.destroyed_check();
		
		MediatorChannel med_channel = m_channels.get(channelName);
		if (med_channel == null) {
			return null;
		}
		
		return new HashMap(med_channel.m_metadata);
	}
	
	public synchronized ArrayList<String> getChannelNames() throws Exception
	{
		m_logger.debug("getChannelNames");
		
		this.destroyed_check();
		
		ArrayList<String> result = new ArrayList<String>();
		result.addAll(m_channels.keySet());
		return result;
	}
	
	private void destroyed_check() throws Exception
	{
		if (m_destroyed) {
			throw new Exception("Engine destroyed");
		}
	}
	
	private String getProperty(String name, String default_value)
	{
		String full_name = String.format("org.epics.archiverappliance.engine.%s.%s", m_engine_def.getName(), name);
		return m_config_service.getInstallationProperties().getProperty(full_name, default_value);
	}
	
	private GenericEngineRequester m_this_requester = new GenericEngineRequester() {
		@Override
		public ScopedLogger getLogger()
		{
			return m_logger;
		}
		
		@Override
		public TypeSystem getTypeSystem()
		{
			return m_config_service.getArchiverTypeSystem();
		}
		
		@Override
		public Configuration getJcaConfiguration() throws Exception
		{
			ByteArrayInputStream bis = JCAConfigGen.generateJCAConfig(m_config_service);
			DefaultConfigurationBuilder configBuilder = new DefaultConfigurationBuilder();
			Configuration configuration = configBuilder.build(bis);
			return configuration;
		}
		
		@Override
		public ScheduledThreadPoolExecutor getScheduler()
		{
			return m_config_service.getEngineContext().getScheduler();
		}
		
		@Override
		public ConfigService getConfigService()
		{
			return m_config_service;
		}
		
		@Override
		public String getProperty(String name, String default_value)
		{
			return GenericEngineMediator.this.getProperty(name, default_value);
		}
	};
	
	private void add_channel_common(String channelName, String fullChannelName, Writer writer, Timestamp last_archived_timestamp, GenericChannelParams params) throws Exception
	{
		WriterRunnable writer_runnable = m_config_service.getEngineContext().getWriteThead();
		
		MediatorChannel med_channel = new MediatorChannel(this, channelName, fullChannelName, writer, last_archived_timestamp, writer_runnable, params);
		
		GenericChannel channel;
		try {
			channel = m_engine.addChannel(channelName, params, med_channel);
		} catch (Exception ex) {
			med_channel.destroy();
			throw ex;
		}
		
		med_channel.complete(channel);
		
		m_channels.put(channelName, med_channel);
	}
	
	protected synchronized ExecutorService get_bulk_flush_executor()
	{
		if (m_bulk_flush_executor == null && !m_destroyed) {
			m_bulk_flush_executor = Executors.newFixedThreadPool(m_bulk_flush_executor_threads);
		}
		return m_bulk_flush_executor;
	}
	
	protected final GenericEngineDefinition m_engine_def;
	protected final ConfigService m_config_service;
	protected final ScopedLogger m_logger;
	protected final ThrottledLogger m_trouble_logger;
	protected final ScopedLogger m_channel_logger;
	protected final ScopedLogger m_metaget_logger;
	private boolean m_destroyed;
	private final HashMap<String, MediatorChannel> m_channels;
	protected final HashMap<String, MediatorMetaGet> m_metagets;
	protected final int m_bulk_flush_executor_threads;
	protected GenericEngine m_engine;
	protected ExecutorService m_bulk_flush_executor;
}

class MediatorChannel implements GenericChannelRequester, EngineWritable
{
	protected MediatorChannel(GenericEngineMediator mediator, String channelName, String fullChannelName, Writer writer,
		Timestamp last_archived_timestamp, WriterRunnable writer_runnable, GenericChannelParams params)
	{
		m_mediator = mediator;
		m_full_channel_name = fullChannelName;
		m_writer = writer;
		m_type = params.dbrType;
		m_last_archived_timestamp = last_archived_timestamp;
		m_writer_runnable = writer_runnable;
		
		m_logger = new ScopedLogger(ScopedLogger.parentScope(mediator.m_channel_logger), channelName);
		
		m_metrics = new PVMetrics(m_full_channel_name, null, System.currentTimeMillis() / 1000, null);
		m_metrics.setMonitor(true);
	}
	
	protected void complete(GenericChannel channel)
	{
		m_channel = channel;
		
		m_logger.info("Channel created");
	}
	
	protected void destroy()
	{
		if (m_channel != null) {
			m_channel.destroy();
		}
		
		synchronized (m_data_lock) {
			m_destroyed = true;
			
			if (m_buffer != null) {
				m_writer_runnable.removeChannel(m_full_channel_name);
			}
		}
		
		if (m_channel != null) {
			m_logger.info("Channel destroyed");
		}
	}
	
	@Override
	public ScopedLogger getLogger()
	{
		return m_logger;
	}
	
	@Override
	public void initSampleBuffer(double expectedFrequency)
	{
		// This can be called from GenericEngine.addChannel(), between MediatorChannel() and complete().
		// Se we need to do the buffer cleanup in destroy() if addChannel fails.
		
		synchronized (m_data_lock) {
			if (m_destroyed) {
				return;
			}
			
			if (m_buffer == null) {
				m_buffer = new SampleBuffer(m_full_channel_name, calc_buffer_capacity(expectedFrequency), m_type, m_metrics);
				
				m_writer_runnable.addChannel(this);
				
				m_metrics.setSamplingPeriod(1.0 / expectedFrequency);
			}
		}
	}
	
	@Override
	public boolean addSample(DBRTimeEvent timeevent)
	{
		synchronized (m_data_lock) {
			if (m_destroyed) {
				return false;
			}
			
			m_metrics.setElementCount(timeevent.getSampleValue().getElementCount());
			m_metrics.setSecondsOfLastEvent(System.currentTimeMillis() / 1000);

			try { 
				if (isfutureorpastOrSame(timeevent)) {
					if (!isSameTimeStamp(timeevent)) {
						m_metrics.addTimestampWrongEventCount(timeevent.getEventTimeStamp());
					}
					return false;
				}
			} catch (IllegalArgumentException ex) {
				m_metrics.addTimestampWrongEventCount(timeevent.getEventTimeStamp());
				return false;
			}

			m_metrics.setLastEventFromIOCTimeStamp(timeevent.getEventTimeStamp());
			//this.lastDBRTimeEvent = timeevent;
			m_last_archived_timestamp = timeevent.getEventTimeStamp();

			boolean incrementEventCounts = false;
			if (m_buffer != null) {
				incrementEventCounts = m_buffer.add(timeevent);
			}
			
			if (incrementEventCounts) {
				m_metrics.addEventCounts();
				m_metrics.addStorageSize(timeevent);
			}

			//if (SampleBuffer.isInErrorState()) {
			//	need_write_error_sample = true;
			//}
		}
		
		return true;
	}
	
	@Override
	public void reportConnected(boolean connected)
	{
		synchronized (m_data_lock) {
			boolean old_connected = m_connected;
			m_connected = connected;
			
			if (connected != old_connected) {
				long now = System.currentTimeMillis() / 1000;
				if (connected) {
					if (m_first_connected == 0) {
						m_first_connected = now;
					} else {
						m_last_reconnected = now;
						m_regain_count++;
					}
				} else {
					m_last_disconnected = now;
				}
			}
			
			m_metrics.setConnected(connected);
			m_metrics.setArchving(connected);
			m_metrics.setConnectionFirstEstablishedEpochSeconds(m_first_connected);
			m_metrics.setConnectionLastRestablishedEpochSeconds(m_last_reconnected);
			m_metrics.setConnectionLastLostEpochSeconds(m_last_disconnected);
			m_metrics.setConnectionLossRegainCount(m_regain_count);
			m_metrics.setLastConnectionEventState(old_connected);
		}
	}
	
	@Override
	public ConcurrentHashMap<String, String> getMetadataMap()
	{
		return m_metadata;
	}
	
	@Override
	public GenericChannelRequester.BulkStore startBulkStore(int buffer_size) throws GenericChannelRequester.BulkStoreException
	{
		synchronized (m_data_lock) {
			if (m_destroyed) {
				throw new GenericChannelRequester.BulkStoreException("Channel has been destroyed.");
			}
			
			if (m_buffer != null) {
				throw new GenericChannelRequester.BulkStoreException("Cannot bulk store if sample buffer was initialized.");
			}
			
			if (m_bulk_store != null) {
				throw new GenericChannelRequester.BulkStoreException("A bulk store is already in progress.");
			}
			
			m_bulk_store = new BulkStoreImpl(buffer_size);
			
			return m_bulk_store;
		}
	}
	
	@Override
	public void logTrouble(String message)
	{
		m_mediator.m_trouble_logger.log(m_logger.buildMessage(message));
	}
	
	@Override
	public String getName()
	{
		return m_full_channel_name;
	}
	
	@Override
	public SampleBuffer getSampleBuffer()
	{
		return m_buffer;
	}
	
	@Override
	public Writer getWriter()
	{
		return m_writer;
	}
	
	@Override
	public void setlastRotateLogsEpochSeconds(long lastRotateLogsEpochSecond)
	{
		synchronized (m_data_lock) {
			m_metrics.setLastRotateLogsEpochSeconds(lastRotateLogsEpochSecond);
		}
	}
	
	private boolean isfutureorpastOrSame(final DBRTimeEvent timeevent)
	{
		Timestamp currentEventTimeStamp = timeevent.getEventTimeStamp();

		if(currentEventTimeStamp.before(PAST_CUTOFF_TIMESTAMP)) {
			logTrouble("timestamp is too far in the past " + TimeUtils.convertToHumanReadableString(currentEventTimeStamp));
			return true;
		}

		Timestamp futureCutOffTimeStamp = TimeUtils.convertFromEpochSeconds(TimeUtils.getCurrentEpochSeconds() + FUTURE_CUTOFF_SECONDS, 0);
		if(currentEventTimeStamp.after(futureCutOffTimeStamp)) {
			logTrouble("timestamp " + TimeUtils.convertToHumanReadableString(currentEventTimeStamp) + " is after the future cutoff " + TimeUtils.convertToHumanReadableString(futureCutOffTimeStamp));
			return true;
		}

		if (m_last_archived_timestamp != null) {
			Timestamp lastEventTimeStamp = m_last_archived_timestamp;
			if(currentEventTimeStamp.before(lastEventTimeStamp)) {
				logTrouble("timestamp " + TimeUtils.convertToHumanReadableString(currentEventTimeStamp) + " is before the previous event's timestamp " + TimeUtils.convertToHumanReadableString(lastEventTimeStamp));
				return true;
			} else if(currentEventTimeStamp.equals(lastEventTimeStamp)){
				logTrouble("timestamp " + TimeUtils.convertToHumanReadableString(currentEventTimeStamp) + " is the same as  the previous event's timestamp " + TimeUtils.convertToHumanReadableString(lastEventTimeStamp));
				return true;
			}
		}
		return false;
	}
	
	private boolean isSameTimeStamp(final DBRTimeEvent timeevent)
	{
		return (m_last_archived_timestamp != null && timeevent.getEventTimeStamp().equals(m_last_archived_timestamp));
	}
	
	private int calc_buffer_capacity(double expectedFrequency)
	{
		EngineContext engineContext = m_mediator.m_config_service.getEngineContext();
		double write_period = engineContext.getWritePeriod();
		int buffer_capacity = ((int) Math.round(Math.max((write_period*expectedFrequency)*engineContext.getSampleBufferCapacityAdjustment(), 1.0))) + 1;
		if (buffer_capacity < 2) {
			buffer_capacity = 2;
		}
		return buffer_capacity;
	}
	
	private class BulkStoreImpl implements GenericChannelRequester.BulkStore, Runnable
	{
		public BulkStoreImpl(int buffer_size)
		{
			m_buffer_size = buffer_size;
			m_flush_sem = new Semaphore(1);
			m_flush_completed = new AtomicBoolean(false);
			m_buffer = create_buffer();
			m_year = 0;
			m_write_error = false;
		}
		
		@Override
		public void addSample(DBRTimeEvent timeevent)
		{
			@SuppressWarnings("deprecation")
			short year = (short)(timeevent.getEventTimeStamp().getYear() + 1900);
			
			if (m_buffer.size() >= m_buffer_size || (m_year != 0 && m_year != year)) {
				if (!need_new_buffer()) {
					return;
				}
			}
			
			m_year = year;
			m_buffer.add(timeevent);
		}
		
		@Override
		public void complete()
		{
			if (m_buffer.size() > 0) {
				need_new_buffer();
			}
			
			m_flush_completed.set(true);
			
			if (m_flush_sem.tryAcquire()) {
				m_flush_sem.release();
				unregister_bulk_store();
			}
			// else the flush job will unregister_bulk_store
		}
		
		private void unregister_bulk_store()
		{
			synchronized (MediatorChannel.this.m_data_lock) {
				if (this == MediatorChannel.this.m_bulk_store) {
					MediatorChannel.this.m_bulk_store = null;
				}
			}
		}
		
		private ArrayListEventStream create_buffer()
		{
			RemotableEventStreamDesc desc = new RemotableEventStreamDesc(m_type, m_full_channel_name, (short)0);
			return new ArrayListEventStream(m_buffer_size, desc);
		}
		
		private boolean need_new_buffer()
		{
			ExecutorService executor = m_mediator.get_bulk_flush_executor();
			if (executor == null) {
				return false;
			}
			
			try {
				m_flush_sem.acquire();
			} catch (InterruptedException ex) {
				return false;
			}
			
			try {
				m_flush_buffer = m_buffer;
				m_flush_buffer.setYear(m_year);
				executor.submit(this);
			}
			catch (Throwable ex) {
				m_flush_sem.release();
				if (ex instanceof RejectedExecutionException) {
					return false;
				}
				throw ex;
			}
			
			m_buffer = create_buffer();
			m_year = 0;
			
			return true;
		}
		
		@Override
		public void run()
		{
			try {
				boolean channel_destroyed;
				synchronized (MediatorChannel.this.m_data_lock) {
					channel_destroyed = m_destroyed;
				}
				
				int count = m_flush_buffer.size();
				
				if (!channel_destroyed && count > 0 && !m_write_error) {
					DBRTimeEvent last_event = (DBRTimeEvent)m_flush_buffer.get(count-1);
					m_metrics.setElementCount(last_event.getSampleValue().getElementCount());
					m_metrics.setLastEventFromIOCTimeStamp(last_event.getEventTimeStamp());
					m_metrics.setSecondsOfLastEvent(System.currentTimeMillis() / 1000);
					
					for (Event event : m_flush_buffer) {
						m_metrics.addEventCounts();
						m_metrics.addStorageSize((DBRTimeEvent)event);
					}
					
					try (BasicContext basicContext = new BasicContext()) {
						m_writer.appendData(basicContext, m_full_channel_name, m_flush_buffer);
					} catch (Exception ex) {
						m_write_error = true;
						logTrouble(String.format("Error writing: %s", ex.getMessage()));
					}
				}
			} finally {
				m_flush_buffer = null;
				m_flush_sem.release();
				
				if (m_flush_completed.get()) {
					unregister_bulk_store();
				}
			}
		}
		
		private final int m_buffer_size;
		private final Semaphore m_flush_sem;
		private final AtomicBoolean m_flush_completed;
		private ArrayListEventStream m_buffer;
		private ArrayListEventStream m_flush_buffer;
		private short m_year;
		private boolean m_write_error;
	}
	
	private static final Timestamp PAST_CUTOFF_TIMESTAMP = TimeUtils.convertFromISO8601String("1991-01-01T00:00:00.000Z");
	private static final int FUTURE_CUTOFF_SECONDS = 30*60;
	
	private final GenericEngineMediator m_mediator;
	protected final String m_full_channel_name;
	private final Writer m_writer;
	protected final ArchDBRTypes m_type;
	protected Timestamp m_last_archived_timestamp;
	private final WriterRunnable m_writer_runnable;
	private boolean m_destroyed;
	protected final ScopedLogger m_logger;
	private final Object m_data_lock = new Object();
	protected final PVMetrics m_metrics;
	protected final ConcurrentHashMap<String, String> m_metadata = new ConcurrentHashMap();
	private volatile SampleBuffer m_buffer;
	protected GenericChannel m_channel;
	private boolean m_connected;
	private long m_regain_count;
	private long m_first_connected;
	private long m_last_reconnected;
	private long m_last_disconnected;
	private BulkStoreImpl m_bulk_store;
}

class MediatorMetaGet implements GenericMetaGetRequester
{
	protected MediatorMetaGet(GenericEngineMediator mediator, String channelName, MetaCompletedListener listener)
	{
		m_mediator = mediator;
		m_channel_name = channelName;
		m_logger = new ScopedLogger(ScopedLogger.parentScope(mediator.m_metaget_logger), channelName);
		m_listener = listener;
	}
	
	protected void complete(GenericMetaGet metaget)
	{
		m_metaget = metaget;
		
		m_logger.info("MetaGet created");
	}
	
	protected void destroy()
	{
		m_destroyed = true;
		
		m_metaget.destroy();
		
		m_logger.info("MetaGet destroyed");
		
		if (m_listener != null) {
			m_listener.completed(new MetaInfo());
		}
	}
	
	@Override
	public ScopedLogger getLogger()
	{
		return m_logger;
	}
	
	@Override
	public void metaGetCompleted(final MetaInfo metaInfo)
	{
		m_logger.info("MetaGet completed");
		
		m_mediator.m_config_service.getEngineContext().getScheduler().execute(new Runnable() { @Override public void run() {
			metaInfoCompletedFromExecutor(metaInfo);
		} });
	}
	
	private void metaInfoCompletedFromExecutor(MetaInfo metaInfo)
	{
		synchronized (m_mediator) {
			if (!m_destroyed) {
				MetaCompletedListener listener = m_listener;
				m_listener = null;
				
				m_mediator.m_metagets.remove(m_channel_name);
				destroy();
				
				listener.completed(metaInfo);
			}
		}
	}
	
	private final GenericEngineMediator m_mediator;
	private final String m_channel_name;
	protected final ScopedLogger m_logger;
	protected MetaCompletedListener m_listener;
	private GenericMetaGet m_metaget;
	private boolean m_destroyed;
}
