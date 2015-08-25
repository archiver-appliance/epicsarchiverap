package org.epics.archiverappliance.engine.generic;

import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.engine.generic.ScopedLogger;

public interface GenericMetaGetRequester
{
	ScopedLogger getLogger();
	
	void metaGetCompleted(MetaInfo metaInfo);
}
