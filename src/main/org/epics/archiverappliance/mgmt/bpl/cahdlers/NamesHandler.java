/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.cahdlers;

import java.util.LinkedList;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * SAX2 for handling archiver.names
 * @author mshankar
 *
 */
public class NamesHandler extends DefaultHandler {
	public static class ChannelDescription {
		String name;
		long startSec = -1;
		long endSec = -1;
		ChannelDescription(String name) { 
			this.name = name;
		}
		public String getName() {
			return name;
		}
		public long getStartSec() {
			return startSec;
		}
		public long getEndSec() {
			return endSec;
		}
	}

	private boolean inStruct = false;
	private LinkedList<ChannelDescription> channels = new LinkedList<ChannelDescription>();
	private String previousName = null;
	private StringBuilder valBuf = new StringBuilder();
	private ChannelDescription currentDesc = null;
	

	@Override	
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		if(qName.equals("struct")) {
			inStruct = true;
		}
		valBuf = new StringBuilder();
	}
	

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if(qName.equals("struct")) {
			inStruct = false;
			if (currentDesc != null) { 
				channels.add(currentDesc);
				currentDesc = null;
			}
			return;
		}
		
		if(inStruct) {
			// There is only one string element for these structs; the rest are i4's
			if(qName.equals("string")) {
				String currentVal = valBuf.toString();
				currentDesc = new ChannelDescription(currentVal);
				valBuf = new StringBuilder();
			} else if(qName.equals("name")) { 
				previousName = valBuf.toString();
				valBuf = new StringBuilder();
			} else if(qName.equals("i4")) { 
				if(previousName != null && currentDesc != null) { 
					switch(previousName) { 
					case "start_sec":
						currentDesc.startSec = Long.parseLong(valBuf.toString());
						break;
					case "end_sec":
						currentDesc.endSec = Long.parseLong(valBuf.toString());
						break;
					default:
						// Ignore the nanos for now. 
					}
					previousName = null;
				}
				valBuf = new StringBuilder();
			}
		}
	}

	
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		valBuf.append(new String(ch, start, length));
	}
	
	/**
	 * This contains the PVS in this archive.
	 * @return channels  &emsp;
	 */
	public LinkedList<ChannelDescription> getChannels() { 
		return channels;
	}
}
