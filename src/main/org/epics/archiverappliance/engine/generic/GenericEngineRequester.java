package org.epics.archiverappliance.engine.generic;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import gov.aps.jca.configuration.Configuration;
import org.epics.archiverappliance.engine.generic.ScopedLogger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.TypeSystem;

public interface GenericEngineRequester
{
	ScopedLogger getLogger();
	
	TypeSystem getTypeSystem();
	
	Configuration getJcaConfiguration() throws Exception;
	
	ScheduledThreadPoolExecutor getScheduler();
	
	ConfigService getConfigService();
	
	String getProperty(String name, String default_value);
}
