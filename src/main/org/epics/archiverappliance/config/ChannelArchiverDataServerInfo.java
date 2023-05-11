/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import java.io.Serializable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.retrieval.channelarchiver.ChannelArchiverReadOnlyPlugin;

/**
 * POJO that contains Channel Archiver Data Server information.
 * @author mshankar
 *
 */
public class ChannelArchiverDataServerInfo implements Serializable {
	private static Logger logger = LogManager.getLogger(ChannelArchiverDataServerInfo.class.getName());
	private static final long serialVersionUID = 2721786392131798533L;
	private String serverURL;
	private String index;
	
	public ChannelArchiverDataServerInfo(String serverURL, String index) {
		this.serverURL = serverURL;
		this.index = index;
	}
	public String getIndex() {
		return index;
	}
	public String getServerURL() {
		return serverURL;
	}

	@Override
	public boolean equals(Object obj) {
		ChannelArchiverDataServerInfo other = (ChannelArchiverDataServerInfo) obj;
		return this.serverURL.equals(other.serverURL) && this.index.equals(other.index);
	}
	@Override
	public int hashCode() {
		return this.serverURL.hashCode() + this.index.hashCode();
	}
	@Override
	public String toString() {
		return "ChannelArchiver server " + serverURL + " using index " + index;
	}
	
	public ChannelArchiverReadOnlyPlugin getPlugin() {
		return new ChannelArchiverReadOnlyPlugin(this.serverURL, this.index);  
	}
	
	public ChannelArchiverReadOnlyPlugin getPlugin(int count, String howStr) {
		logger.debug("Creating ca plugin for " + serverURL + " using index " + index + " asking for " + count + " values " + " and method " + howStr);
		return new ChannelArchiverReadOnlyPlugin(this.serverURL, this.index, count, howStr);  
	}
}
