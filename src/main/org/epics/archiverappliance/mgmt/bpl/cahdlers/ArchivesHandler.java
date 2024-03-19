/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.cahdlers;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * SAX2 for handling archiver.archives.
 * @author mshankar
 *
 */
public class ArchivesHandler extends DefaultHandler {
	private static Logger logger = LogManager.getLogger(ArchivesHandler.class.getName());
	private boolean inStruct = false;
	private LinkedList<HashMap<String, String>> structs = new LinkedList<HashMap<String, String>>();
	private HashMap<String, String> currentStruct = null;
	private String currentKey = null;

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		if(qName.equals("struct")) {
			inStruct = true;
			currentStruct = new HashMap<String, String>();
			structs.add(currentStruct);
		}
		valBuf = new StringWriter();
	}
	

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if(qName.equals("struct")) {
			inStruct = false;
			currentStruct = null;
			logger.debug("Done with struct");
			return;
		}
		
		if(inStruct) {
			if(qName.equals("name")) {
				currentKey = valBuf.toString();
				valBuf = new StringWriter();
			} else if(qName.equals("i4") || qName.equals("string")) {
				String currentVal = valBuf.toString();
				valBuf = new StringWriter();
				logger.debug("Adding " + currentKey + "=" + currentVal + " to current struct");
				currentStruct.put(currentKey, currentVal);
				currentKey = null;
			}
		}
	}

	
	StringWriter valBuf = new StringWriter();
	
	
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		valBuf.append(new String(ch, start, length));
	}
	
	/**
	 * Returns the archives that supported by this Channel Archiver.
	 * Keys are 
	 * <ol>
	 * <li>key - The integer key we use to pass to the rest of the calls</li>
	 * <li>name - The name of the archives - for example, LCLS_SPARSE</li>
	 * <li>path - Some internal configuration</li>
	 * </ol>
	 * @return structs  &emsp;
	 */
	public LinkedList<HashMap<String, String>> getArchives() { 
		return structs;
	}
}
