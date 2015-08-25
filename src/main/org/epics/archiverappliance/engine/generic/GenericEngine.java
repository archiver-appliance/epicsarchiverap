package org.epics.archiverappliance.engine.generic;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.epics.archiverappliance.engine.generic.GenericChannel;
import org.epics.archiverappliance.engine.generic.GenericChannelParams;
import org.epics.archiverappliance.engine.generic.GenericChannelRequester;
import org.epics.archiverappliance.engine.generic.GenericMetaGet;
import org.epics.archiverappliance.engine.generic.GenericMetaGetRequester;

public interface GenericEngine
{
	void destroy();
	
	GenericChannel addChannel(String channelName, GenericChannelParams params, GenericChannelRequester requester) throws Exception;
	
	GenericMetaGet addMetaGet(String channelName, String metadatafields[], GenericMetaGetRequester requester) throws Exception;
	
	default boolean handleHttpRequest(HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		return false;
	}
}
