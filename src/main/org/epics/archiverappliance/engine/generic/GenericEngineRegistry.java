package org.epics.archiverappliance.engine.generic;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.generic.GenericEngineDefinition;
import org.epics.archiverappliance.engine.generic.GenericEngineMediator;

public class GenericEngineRegistry
{
	private static final Logger logger = Logger.getLogger(GenericEngineRegistry.class.getName());
	
	public GenericEngineRegistry(ConfigService config_service)
	{
		m_config_service = config_service;
		m_engine_defs = new HashMap<String, GenericEngineDefinition>();
		m_engines = new ConcurrentHashMap<String, GenericEngineMediator>();
	}

	public void registerEngineDefinition(GenericEngineDefinition engine_def)
	{
		m_engine_defs.put(engine_def.getName(), engine_def);
	}
	
	public void autoloadEnginesAsNeeded()
	{
		for (String engine_type : m_engine_defs.keySet()) {
			String autoload_property = String.format("org.epics.archiverappliance.engine.%s.AutoLoad", engine_type);
			boolean autoload = Boolean.parseBoolean(m_config_service.getInstallationProperties().getProperty(autoload_property, "false"));
			if (autoload) {
				try {
					getEngine(engine_type);
				} catch (Exception ex) {
					logger.error(String.format("Failed to autoload engine '%s'.", engine_type), ex);
				}
			}
		}
	}

	public synchronized GenericEngineMediator getEngine(String engine_type) throws Exception
	{
		if (m_destroyed) {
			throw new Exception("GenericEngineRegistry is being destroyed");
		}
		
		GenericEngineMediator engine = m_engines.get(engine_type);
		
		if (engine == null) {
			GenericEngineDefinition engine_def = m_engine_defs.get(engine_type);
			if (engine_def == null) {
				throw new Exception(String.format("Unknown engine type '%s'", engine_type));
			}
			
			engine = new GenericEngineMediator(engine_def, m_config_service);
			engine.complete();
			
			m_engines.put(engine_type, engine);
		}
		
		return engine;
	}
	
	public static class ParsedUri {
		public ParsedUri(String engine_name, String path)
		{
			this.engine_name = engine_name;
			this.path = path;
		}
		
		public final String engine_name;
		public final String path;
	}

	public ParsedUri readRequestUri(String requestUri)
	{
		ParsedUri result = null;
		
		for (String this_engine_name : m_engine_defs.keySet()) {
			String prefix = this_engine_name + ":";
			if (requestUri.startsWith(prefix)) {
				result = new ParsedUri(this_engine_name, requestUri.substring(prefix.length()));
			}
		}
		
		return result;
	}
	
	public GenericEngineMediator getUriEngine(ParsedUri uri) throws Exception
	{
		return this.getEngine(uri.engine_name);
	}
	
	public Map<String, GenericEngineMediator> getEngines()
	{
		return Collections.unmodifiableMap(m_engines);
	}
	
	public void destroy()
	{
		synchronized (this) {
			if (m_destroyed) {
				return;
			}
			m_destroyed = true;
		}
		
		for (GenericEngineMediator engine : m_engines.values()) {
			engine.destroy();
		}
	}
	
	public boolean handleHttpRequest(HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		if (m_destroyed) {
			throw new IOException("GenericEngineRegistry is being destroyed");
		}
		
		for (GenericEngineMediator engine : m_engines.values()) {
			if (engine.m_engine.handleHttpRequest(req, resp)) {
				return true;
			}
		}
		
		return false;
	}
	
	private final ConfigService m_config_service;
	private final HashMap<String, GenericEngineDefinition> m_engine_defs;
	private final ConcurrentHashMap<String, GenericEngineMediator> m_engines;
	private boolean m_destroyed;
}
