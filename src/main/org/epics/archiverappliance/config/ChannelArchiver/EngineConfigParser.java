/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config.ChannelArchiver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.LinkedList;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Ability to import a ChannelArchiver engine config file.
 * Support for PVNames, periods and scan/monitor so far.
 * @author mshankar
 *
 */
public class EngineConfigParser extends DefaultHandler {
	
	/**
	 * Main import method. Pass in an input stream (perhaps from an HTTP POST) and get a list of PVConfigs.
	 * @param configFileName
	 * @return
	 * @throws Exception
	 */
	public static LinkedList<PVConfig> importEngineConfig(InputStream is) throws Exception {
		SAXParserFactory sfac = SAXParserFactory.newInstance();
		sfac.setNamespaceAware(false);
		sfac.setValidating(false);
		SAXParser parser = sfac.newSAXParser();
		EngineConfigParser econfig = new EngineConfigParser();
		parser.parse(is, econfig);
		return econfig.pvConfigs;
	}
	
	LinkedList<PVConfig> pvConfigs = new LinkedList<PVConfig>();
	
	

	boolean inChannel = false;
	boolean inElement = true;
	StringWriter getThresholdBuf = null;
	StringWriter nameBuf = null;
	StringWriter periodBuf = null;
	String name = null;
	String period = null;
	boolean monitor = true;
	// According to the channel archiver manual, this defaults to 20 seconds.
	int getThreshold = 20;
	
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		if(qName.equals("channel")) {
			inChannel = true;
		} else if (inChannel && qName.equals("name")) {
			nameBuf = new StringWriter();
		} else if (inChannel && qName.equals("period")) {
			periodBuf = new StringWriter();
		} else if (inChannel && qName.equals("monitor")) {
			monitor = true;
		} else if (inChannel && qName.equals("scan")) {
			monitor = false;
		} else if(qName.equals("get_threshold")) {
			getThresholdBuf = new StringWriter();
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if(qName.equals("channel")) {
			inChannel = false;
			float samplingperiod = Float.parseFloat(period);
			pvConfigs.add(new PVConfig(name, samplingperiod, monitor));
			name = null;
			period = null;
			monitor = false;
		} else if (inChannel && qName.equals("name") && nameBuf != null) {
			name = nameBuf.toString();
			nameBuf = null;
		} else if (inChannel && qName.equals("period") && periodBuf != null) {
			period = periodBuf.toString();
			periodBuf = null;
		} else if(qName.equals("get_threshold") && getThresholdBuf != null) {
			getThreshold = Integer.parseInt(getThresholdBuf.toString());
			getThresholdBuf = null;
		}
	}

	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		if(inChannel && nameBuf != null) {
			nameBuf.append(new String(ch, start, length));
		} else if(inChannel && periodBuf != null) {
			periodBuf.append(new String(ch, start, length));
		} else if(getThresholdBuf != null) {
			getThresholdBuf.append(new String(ch, start, length));
		}
	}

	@Override
	public InputSource resolveEntity(String publicId, String systemId) throws IOException, SAXException {
		if(systemId.endsWith("engineconfig.dtd")) {
			ByteArrayInputStream bis = new ByteArrayInputStream(channelArchiverEngineConfigDTD.getBytes());
			return new InputSource(bis);
		}
		throw new IOException("Cannot resolve " + systemId);
	}
	
	private static String channelArchiverEngineConfigDTD = 
	"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + 
	"<!-- DTD for the ArchiveEngine Configuration         -->\n" + 
	"<!-- Note that we do not allow empty configurations: -->\n" + 
	"<!-- Each config. must contain at least one group,   -->\n" + 
	"<!-- and each group must contain at least 1 channel. -->\n" + 
	"<!ELEMENT engineconfig ((write_period|get_threshold|\n" + 
	"                         file_size|ignored_future|\n" + 
	"                         buffer_reserve|\n" + 
	"                         max_repeat_count|disconnect)*,\n" + 
	"                         group+)>\n" + 
	"<!ELEMENT group (name,channel+)>\n" + 
	"<!ELEMENT channel (name,period,(scan|monitor),disable?)>\n" + 
	"<!ELEMENT write_period (#PCDATA)><!-- int seconds -->\n" + 
	"<!ELEMENT get_threshold (#PCDATA)><!-- int seconds -->\n" + 
	"<!ELEMENT file_size (#PCDATA)><!-- MB -->\n" + 
	"<!ELEMENT ignored_future (#PCDATA)><!-- double hours -->\n" + 
	"<!ELEMENT buffer_reserve (#PCDATA)><!-- int times -->\n" + 
	"<!ELEMENT max_repeat_count (#PCDATA)><!-- int times -->\n" + 
	"<!ELEMENT disconnect EMPTY>\n" + 
	"<!ELEMENT name (#PCDATA)>\n" + 
	"<!ELEMENT period (#PCDATA)><!-- double seconds -->\n" + 
	"<!ELEMENT scan EMPTY>\n" + 
	"<!ELEMENT monitor EMPTY>\n" + 
	"<!ELEMENT disable EMPTY>\n";

	
}
