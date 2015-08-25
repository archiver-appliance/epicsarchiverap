package org.epics.archiverappliance.engine.generic;

import org.epics.archiverappliance.engine.generic.GenericEngine;
import org.epics.archiverappliance.engine.generic.GenericEngineRequester;

public interface GenericEngineDefinition
{
	String getName();
	
	GenericEngine createEngine(GenericEngineRequester requester) throws Exception;
}
