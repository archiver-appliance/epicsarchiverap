/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.cahdlers;

import java.io.StringWriter;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * SAX2 for handling archiver.info
 * @author mshankar
 *
 */
public class InfoHandler extends DefaultHandler {
	private String desc = null;
	
	private static Logger logger = LogManager.getLogger(InfoHandler.class.getName());
	LinkedList<String> currentNodes = new LinkedList<String>();
	boolean descFound = false;

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		currentNodes.add(qName);
		valBuf = new StringWriter();
	}
	

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		String currentNodeTree = getCurrentNodeTree();
		logger.debug(currentNodeTree);
		if(descFound) {
			if(currentNodeTree.equals("methodResponse.params.param.value.struct.member.value.string")) {
				desc = valBuf.toString();
				logger.debug("Description from the remote channel access server is " + desc);
			}
		}
		
		if(currentNodeTree.equals("methodResponse.params.param.value.struct.member.name")) {
			String currentName = valBuf.toString();
			logger.debug("Found name " + currentName);
			if(currentName != null && currentName.equals("desc")) {
				// We found the desc filed. The next value.string is the description
				logger.debug("Found the desc field");
				descFound = true;
			}
		}
		String poppedElement = currentNodes.pollLast();
		assert(qName.equals(poppedElement));
	}

	
	StringWriter valBuf = new StringWriter();
	
	
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		valBuf.append(new String(ch, start, length));
	}
	
	
	private String getCurrentNodeTree() {
		StringWriter buf = new StringWriter();
		boolean first = true;
		for(String node : currentNodes) {
			if(first) { first = false; } else { buf.append("."); }
			buf.append(node);
		}
		return buf.toString();
	}


	public String getDesc() {
		return desc;
	}
}
