/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.channelarchiver;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.ConversionFunction;
import org.epics.archiverappliance.retrieval.CallableEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.utils.ui.URIUtils;

/**
 * A storage plugin that can front a Channel Archiver Data Server.
 * Only reads are supported.
 * This has the ability to support reduced data sets like LCLS_SPARSE but this is optional. 
 * If a sparse key is not specified, we default to using the un-reduced archive key.
 * Integration test plan for this. Try to include
 * <ol>
 * <li>A PV that is archived in both the appliance and ChannelArchiver. Date ranges should be appliance only, overlap and ChannelArchiver only.</li>
 * <li>A PV that is archived in only the ChannelArchiver.</li>
 * </ol>
 * @author mshankar
 *
 */
public class ChannelArchiverReadOnlyPlugin implements StoragePlugin {
	private static Logger logger = Logger.getLogger(ChannelArchiverReadOnlyPlugin.class.getName());
	private String serverURL;
	private int archiveKey;
	private int reducedArchiveKey = -1;
	private String description;
	private String name;
	private int valuesRequested = Integer.MAX_VALUE;
	private String howStr = "0";
	
	public ChannelArchiverReadOnlyPlugin() {
		
	}
	
	public ChannelArchiverReadOnlyPlugin(String serverURL, String index) {
		this.serverURL = serverURL;
		this.archiveKey = Integer.parseInt(index);
		this.setDescription("ChannelArchiverReadOnlyPlugin plugin with serverURL " + serverURL + " and archiveKey " + archiveKey + ((reducedArchiveKey != -1) ? (" and a reducedArchiveKey of " + reducedArchiveKey) : (" and no reducedArchiveKey")));
	}
	
	public ChannelArchiverReadOnlyPlugin(String serverURL, String index, int valuesRequested, String howStr) {
		this.serverURL = serverURL;
		this.archiveKey = Integer.parseInt(index);
		this.valuesRequested = valuesRequested;
		this.howStr = howStr;
		this.setDescription("ChannelArchiverReadOnlyPlugin plugin with serverURL " + serverURL + " and archiveKey " + archiveKey + ((reducedArchiveKey != -1) ? (" and a reducedArchiveKey of " + reducedArchiveKey) : (" and no reducedArchiveKey")));
	}


	@Override
	public List<Callable<EventStream>> getDataForPV(BasicContext context, String pvName, Timestamp startTime, Timestamp endTime, PostProcessor postProcessor)  throws IOException {
		if(reducedArchiveKey != -1) {
			return getDataForPV(context, pvName, startTime, endTime, reducedArchiveKey, postProcessor);
		} else {
			return getDataForPV(context, pvName, startTime, endTime, archiveKey, postProcessor);
		}
	}
	
	private List<Callable<EventStream>> getDataForPV(BasicContext context, String pvName, Timestamp startTime, Timestamp endTime, int archiveKey, PostProcessor postProcessor) throws IOException {
		try {
			String pvNameForCall = pvName;
			if(context.getPvNameFromRequest() != null) { 
				logger.info("Using pvName from request " + context.getPvNameFromRequest() + " when making a call to the ChannelArchiver for pv " + pvName);
				pvNameForCall = context.getPvNameFromRequest();
			}
			
			
			String archiveValuesStr = new String("<?xml version=\"1.0\"?>\n"
							+ "<methodCall>\n"
							+ "<methodName>archiver.values</methodName>\n"
							+ "<params>\n"
							+ "<param><value><i4>" + archiveKey + "</i4></value></param>\n"
							+ "<param><value><array><data><value><string>" + pvNameForCall + "</string></value></data></array></value></param>\n"
							+ "<param><value><i4>" + TimeUtils.convertToEpochSeconds(startTime)+ "</i4></value></param>\n"
							+ "<param><value><i4>" + startTime.getNanos() + "</i4></value></param>\n"
							+ "<param><value><i4>" + TimeUtils.convertToEpochSeconds(endTime) + "</i4></value></param>\n"
							+ "<param><value><i4>" + endTime.getNanos() + "</i4></value></param>\n"
							+ "<param><value><i4>" + valuesRequested + "</i4></value></param>\n"
							+ "<param><value><i4>" + howStr + "</i4></value></param>\n"
							+ "</params>\n"
							+ "</methodCall>\n");
			URI serverURI = new URI(serverURL);
			if(serverURI.getScheme().equals("file")) {
				logger.info("Using a file provider for Channel Archiver data - this better be a unit test.");
				// We use the file scheme for unit testing... Yeah, the extensions are hardcoded...
				InputStream is = new BufferedInputStream(new FileInputStream(new File(serverURI.getPath() + File.separator + pvName + ".xml")));
				// ArchiverValuesHandler takes over the burden of closing the input stream.
				ArchiverValuesHandler handler = new ArchiverValuesHandler(pvName, is, serverURL.toString() + "\n" + archiveValuesStr, context.getRetrievalExpectedDBRType());
				if(postProcessor != null) { 
					return CallableEventStream.makeOneStreamCallableList(handler, postProcessor, true);
				} else { 
					return CallableEventStream.makeOneStreamCallableList(handler);
				}
			} else {
				StringEntity archiverValues = new StringEntity(archiveValuesStr, ContentType.APPLICATION_XML);
				if(logger.isDebugEnabled()) {
					logger.debug(getDescription() + " making call to channel archiver with " + archiveValuesStr);
				}
				
				CloseableHttpClient httpclient = HttpClients.createDefault();
				HttpPost postMethod = new HttpPost(serverURL);
				postMethod.addHeader("Content-Type", "text/xml");
				postMethod.setEntity(archiverValues);
				if(logger.isDebugEnabled()) {
					logger.debug("About to make a POST with " + archiveValuesStr);
				}
				HttpResponse response = httpclient.execute(postMethod);
				int statusCode = response.getStatusLine().getStatusCode();
				if(statusCode >= 200 && statusCode <= 206) { 
					HttpEntity entity = response.getEntity();
					if (entity != null) {
						logger.debug("Obtained a HTTP entity of length " + entity.getContentLength());
						// ArchiverValuesHandler takes over the burden of closing the input stream.
						InputStream is = entity.getContent();
						ArchiverValuesHandler handler = new ArchiverValuesHandler(pvName, is, serverURL.toString() + "\n" + archiveValuesStr, context.getRetrievalExpectedDBRType());
						if(postProcessor != null) { 
							return CallableEventStream.makeOneStreamCallableList(handler, postProcessor, true);
						} else { 
							return CallableEventStream.makeOneStreamCallableList(handler);
						}
					} else {
						throw new IOException("HTTP response did not have an entity associated with it");
					}
				} else {
					logger.error("Got an invalid status code " + statusCode + " from the server " + serverURL + " for PV " + pvName + " so returning null");
					return null;
				}
			}
		} catch(UnsupportedEncodingException ex) {
			throw new IOException("Exception making call to Channel Archiver", ex);
		} catch (URISyntaxException e) {
			throw new IOException("Invalid URL " + serverURL, e);
		}
	}

	@Override
	public boolean appendData(BasicContext context, String pvName, EventStream stream) throws IOException {
		throw new IOException("The ChannelArchiverReadOnlyPlugin does not support the Writer interface");
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public void initialize(String configURL, ConfigService configService) throws IOException {
		try {
			URI srcURI = new URI(configURL);
			HashMap<String, String> queryNVPairs = URIUtils.parseQueryString(srcURI);

			if(queryNVPairs.containsKey("serverURL")) {
				this.setServerURL(queryNVPairs.get("serverURL"));
			} else {
				throw new IOException("Cannot initialize the plugin; this needs the serverURL to be specified");
			}

			if(queryNVPairs.containsKey("archiveKey")) {
				this.setArchiveKey(Integer.parseInt(queryNVPairs.get("archiveKey")));
			} else {
				throw new IOException("Cannot initialize the plugin; this needs the archiver key to be specified");
			}
			
			if(queryNVPairs.containsKey("reducedArchiveKey")) {
				this.setReducedArchiveKey(Integer.parseInt(queryNVPairs.get("reducedArchiveKey")));
			}
			
			if(queryNVPairs.containsKey("name")) {
				name = queryNVPairs.get("name");
			} else {
				name = new URL(this.getServerURL()).getHost();
				logger.debug("Using the default name of " + name + " for this channel archiver engine");
			}

			this.setDescription("ChannelArchiverReadOnlyPlugin plugin with serverURL " + serverURL + " and archiveKey " + archiveKey + ((reducedArchiveKey != -1) ? (" and a reducedArchiveKey of " + reducedArchiveKey) : (" and no reducedArchiveKey")));
		} catch(URISyntaxException ex) {
			throw new IOException(ex);
		}
	}

	public String getServerURL() {
		return serverURL;
	}

	public void setServerURL(String serverURL) {
		this.serverURL = serverURL;
	}

	public int getArchiveKey() {
		return archiveKey;
	}

	public void setArchiveKey(int archiveKey) {
		this.archiveKey = archiveKey;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public int getReducedArchiveKey() {
		return reducedArchiveKey;
	}

	public void setReducedArchiveKey(int reducedArchiveKey) {
		this.reducedArchiveKey = reducedArchiveKey;
	}

	@Override
	public Event getLastKnownEvent(BasicContext context, String pvName) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public Event getFirstKnownEvent(BasicContext context, String pvName) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getName() {
		return name;
	}
	
	@Override
	public void renamePV(BasicContext context, String oldName, String newName) throws IOException {
		// Nothing to do here.
	}
	@Override
	public void convert(BasicContext context, String pvName, ConversionFunction conversionFuntion) throws IOException {
		// Nothing to do here.
	}	
}
