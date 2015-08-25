package org.epics.archiverappliance.engine.generic;

import java.lang.reflect.Constructor;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.config.TypeSystem;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.engine.generic.GenericEngine;
import org.epics.archiverappliance.engine.generic.GenericEngineDefinition;
import org.epics.archiverappliance.engine.generic.GenericEngineRequester;
import org.epics.archiverappliance.engine.generic.GenericChannelParams;
import org.epics.archiverappliance.engine.generic.GenericChannel;
import org.epics.archiverappliance.engine.generic.GenericChannelRequester;
import org.epics.archiverappliance.engine.generic.GenericMetaGet;
import org.epics.archiverappliance.engine.generic.GenericMetaGetRequester;
import org.epics.archiverappliance.engine.generic.ScopedLogger;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import gov.aps.jca.dbr.DBR_TIME_Double;
import gov.aps.jca.dbr.TimeStamp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.simple.JSONValue;

public class DummyGenericEngineDefinition implements GenericEngineDefinition
{
	@Override
	public String getName()
	{
		return "dummy_generic";
	}

	@Override
	public GenericEngine createEngine(GenericEngineRequester requester) throws Exception
	{
		return new DummyGenericEngine(requester);
	}
}

class DummyGenericEngine implements GenericEngine
{
	public DummyGenericEngine(GenericEngineRequester requester)
	{
		m_requester = requester;
	}

	@Override
	public void destroy()
	{
	}
	
	@Override
	public GenericChannel addChannel(String channelName, GenericChannelParams params, GenericChannelRequester requester) throws Exception
	{
		DummyGenericChannel channel = new DummyGenericChannel(m_requester.getTypeSystem(), channelName, params, requester);
		channel.complete();
		return channel;
	}

	@Override
	public GenericMetaGet addMetaGet(String channelName, String metadatafields[], GenericMetaGetRequester requester) throws Exception
	{
		return new DummyGenericMetaGet(channelName, requester);
	}
	
	@Override
	public boolean handleHttpRequest(HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		if (!req.getPathInfo().equals("/dummyGenericEngine")) {
			return false;
		}
		
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			infoValues.put("status", "ok");
			infoValues.put("desc", "Dummy request is working.");
			out.println(JSONValue.toJSONString(infoValues));
		}
		
		return true;
	}

	public final GenericEngineRequester m_requester;
}

class DummyGenericChannel implements GenericChannel
{
	private static final double PERIOD = 30.0;
	private static final double AMPLITUDE = 10.0;
	
	public DummyGenericChannel(TypeSystem type_system, String channelName, GenericChannelParams params, GenericChannelRequester requester) throws Exception
	{
		if (params.dbrType != ArchDBRTypes.DBR_SCALAR_DOUBLE) {
			throw new Exception("Invalid dbrType");
		}
		
		m_channel_name = channelName;
		m_params = params;
		m_requester = requester;
		m_logger = requester.getLogger();
		m_semaphore = new Semaphore(0);
		m_thread = new Thread() { public void run() { DummyGenericChannel.this.thread_func(); } };
		m_constructor = type_system.getJCADBRConstructor(params.dbrType);

		m_thread.start();
	}
	
	public void complete()
	{
		m_requester.initSampleBuffer(1.0 / m_params.samplingPeriod);
	}
	
	@Override
	public void destroy()
	{
		m_semaphore.release();
		try {
			m_thread.join();
		} catch (InterruptedException ex) {}
	}
	
	public void thread_func()
	{
		m_logger.info("Thread started");
		
		int counter = 0;
		boolean connected = false;
		
		while (true) {
			// Wait some time.
			long waitTime_ms = (long)(1000.0 * Math.max(0.0, Math.min(3600.0, m_params.samplingPeriod)));
			try {
				if (m_semaphore.tryAcquire(waitTime_ms, TimeUnit.MILLISECONDS)) {
					break;
				}
			} catch (InterruptedException ex) {
				return;
			}
			
			// Simulate some connection and disconnection.
			if (counter > 0) {
				counter--;
			} else {
				counter = 9;
				connected = !connected;
				m_requester.reportConnected(connected);
			}
			
			// Get a JCA timestamp for the current time.
			DateTime dt = new DateTime(DateTimeZone.UTC);
			long ms = dt.getMillis();
			TimeStamp ts = new TimeStamp((ms / 1000) - TimeUtils.EPICS_EPOCH_2_JAVA_EPOCH_OFFSET, (ms % 1000) * 1000000);
			
			// Make up a value.
			double value = AMPLITUDE * Math.sin(ms * ((2.0 * Math.PI) / (1000.0 * PERIOD)));
			
			m_logger.debug(String.format("Sample %s %s", dt.toString(), value));
			
			// Create the DBR.
			DBR_TIME_Double dbr = new DBR_TIME_Double(new double[]{value});
			dbr.setSeverity(0);
			dbr.setStatus(0);
			dbr.setTimeStamp(ts);
			
			// Create the DBRTimeEvent from the DBR.
			DBRTimeEvent sample;
			try {
				sample = m_constructor.newInstance(dbr);
			} catch (Exception ex) {
				m_logger.error("newInstance threw exception");
				continue;
			}
			
			m_requester.addSample(sample);
		}

		m_logger.info("Thread stopping");
	}

	public final String m_channel_name;
	public final GenericChannelParams m_params;
	public final GenericChannelRequester m_requester;
	public final ScopedLogger m_logger;
	public final Semaphore m_semaphore;
	public volatile Thread m_thread;
	public final Constructor<? extends DBRTimeEvent> m_constructor;
}

class DummyGenericMetaGet implements GenericMetaGet
{
	private static final double METAGET_TIME = 2.0;
	
	public DummyGenericMetaGet(String channelName, GenericMetaGetRequester requester)
	{
		m_requester = requester;
		m_logger = requester.getLogger();
		m_semaphore = new Semaphore(0);
		m_thread = new Thread() { public void run() { DummyGenericMetaGet.this.thread_func(); } };
		
		m_thread.start();
	}
	
	@Override
	public void destroy()
	{
		m_semaphore.release();
		try {
			m_thread.join();
		} catch (InterruptedException ex) {}
	}
	
	private void thread_func()
	{
		m_logger.info("Thread started");
		
		try {
			if (m_semaphore.tryAcquire((long)(METAGET_TIME * 1000.0), TimeUnit.MILLISECONDS)) {
				return;
			}
		} catch (InterruptedException ex) {
			return;
		}
		
		m_logger.info("Simulating completed MetaGet");
		
		MetaInfo meta = new MetaInfo();
		meta.setArchDBRTypes(ArchDBRTypes.DBR_SCALAR_DOUBLE);
		meta.setEventCount(1);
		
		m_requester.metaGetCompleted(meta);
		
		m_logger.info("Thread stopping");
	}
	
	public final GenericMetaGetRequester m_requester;
	public final ScopedLogger m_logger;
	public final Semaphore m_semaphore;
	public final Thread m_thread;
}
