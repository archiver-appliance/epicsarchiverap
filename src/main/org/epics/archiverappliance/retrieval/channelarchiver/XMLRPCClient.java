/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.channelarchiver;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * A class that handles the XML-RPC portion of an existing channel archiver instance.
 * We pass in a SAXParser for all method calls; theoretically, this should be accommodate passing data out of the system in a streaming fashion  
 * 
 * @author mshankar
 *
 */
public class XMLRPCClient {
	private static Logger logger = Logger.getLogger(XMLRPCClient.class.getName());
	/**
	 * Internal method to make a XML_RPC post call and call the SAX handler on the returned document.
	 * @param serverURL The Server URL
	 * @param handler  DefaultHandler 
	 * @param postEntity StringEntity 
	 * @throws IOException  &emsp; 
	 * @throws SAXException  &emsp; 
	 */
	private static void doHTTPPostAndCallSAXHandler(String serverURL, DefaultHandler handler, StringEntity postEntity)  throws IOException, SAXException {
		logger.debug("Executing doHTTPPostAndCallSAXHandler with the server URL " + serverURL);
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpPost postMethod = new HttpPost(serverURL);
		postMethod.addHeader("Content-Type", "text/xml");
		postMethod.setEntity(postEntity);
		logger.debug("Executing the HTTP POST" + serverURL);
		HttpResponse response = httpclient.execute(postMethod);
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			logger.debug("Obtained a HTTP entity of length " + entity.getContentLength());
			try(InputStream is = entity.getContent()) {
				SAXParserFactory sfac = SAXParserFactory.newInstance();
				sfac.setNamespaceAware(false);
				sfac.setValidating(false);
				SAXParser parser = sfac.newSAXParser();
				parser.parse(is, handler);
			} catch(ParserConfigurationException pex) {
				throw new IOException(pex);
			}
		} else {
			throw new IOException("HTTP response did not have an entity associated with it");
		}
	}
	
	
	/**
	 * Call archiver.info on the Channel Archiver
	 * @param serverURL  The Server URL 
	 * @param handler  DefaultHandler  
	 * @throws IOException &emsp; 
	 * @throws SAXException &emsp; 
	 */
	public static void archiverInfo(String serverURL, DefaultHandler handler) throws IOException, SAXException {
		logger.debug("Getting channel archiver info with URL " + serverURL);
		StringEntity archiverInfo = new StringEntity(
				"<?xml version=\"1.0\"?>\n"
						+ "<methodCall>\n"
						+ "<methodName>archiver.info</methodName>\n"
						+ "<params></params>\n"
						+ "</methodCall>\n",
						ContentType.APPLICATION_XML);
		doHTTPPostAndCallSAXHandler(serverURL, handler, archiverInfo);
	}
	
	
	/**
	 * Call archiver.archives on the Channel Archiver
	 * @param serverURL The Server URL 
	 * @param handler  DefaultHandler  
	 * @throws IOException  &emsp; 
	 * @throws SAXException  &emsp; 
	 */
	public static void archiverArchives(String serverURL, DefaultHandler handler) throws IOException, SAXException {
		logger.debug("Getting channel archiver archives with URL " + serverURL);
		StringEntity archiverInfo = new StringEntity(
				"<?xml version=\"1.0\"?>\n"
						+ "<methodCall>\n"
						+ "<methodName>archiver.archives</methodName>\n"
						+ "<params></params>\n"
						+ "</methodCall>\n",
						ContentType.APPLICATION_XML);
		doHTTPPostAndCallSAXHandler(serverURL, handler, archiverInfo);
	}
	
	
	/**
	 * Call archiver.names on the Channel Archiver - note this call can take a loooong time.
	 * @param serverURL The Server URL 
	 * @param key  &emsp; 
	 * @param handler  DefaultHandler  
	 * @throws IOException  &emsp; 
	 * @throws SAXException  &emsp; 
	 */
	public static void archiverNames(String serverURL, String key, DefaultHandler handler) throws IOException, SAXException {
		logger.debug("Getting channel archiver archives with URL " + serverURL);
		StringEntity archiverInfo = new StringEntity(
				"<?xml version=\"1.0\"?>\n"
						+ "<methodCall>\n"
						+ "<methodName>archiver.names</methodName>\n"
						+ "<params>\n"
						+ "<param><value><i4>" + key + "</i4></value></param>\n"
						+ "<param><value><string></string></value></param>"
						+ "</params>\n"
						+ "</methodCall>\n",
						ContentType.APPLICATION_XML);
		doHTTPPostAndCallSAXHandler(serverURL, handler, archiverInfo);
	}
}
