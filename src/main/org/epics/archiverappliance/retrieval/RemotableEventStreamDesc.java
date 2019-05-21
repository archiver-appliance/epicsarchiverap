/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;


import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.FieldValue;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;

/**
 * Information about the whole stream that is often used in generating headers etc...
 * @author mshankar
 *
 */
public class RemotableEventStreamDesc extends EventStreamDesc {
	private static Logger logger = Logger.getLogger(RemotableEventStreamDesc.class.getName());
	private short year;
	private int elementCount = 1;
	private HashMap<String, String> headers = new HashMap<String, String>();

	
	public RemotableEventStreamDesc(ArchDBRTypes archDBRType, String pvName, short year) {
		super(archDBRType, pvName);
		this.archDBRType = archDBRType;
		this.year = year;
	}
	
	public RemotableEventStreamDesc(String pvName, PayloadInfo info) {
		super(ArchDBRTypes.valueOf(info.getType()), pvName);
		if(!pvName.equals(info.getPvname())) { 
			logger.warn("Returning data from PV " + info.getPvname() + " as the data for PV " + pvName);
		}
		this.year = (short) info.getYear();
		if(info.hasElementCount()) this.setElementCount(info.getElementCount());
		if(info.getHeadersCount() > 0 ) { 
			for(FieldValue f : info.getHeadersList()) { 
				this.headers.put(f.getName(), f.getVal());
			}
		}
	}
	
	public RemotableEventStreamDesc(RemotableEventStreamDesc other) { 
		super(other);
		this.year = other.year;
		this.elementCount = other.elementCount;
		this.headers = new HashMap<String, String>(other.headers);
	}
	
	public void mergeFrom(PVTypeInfo info, HashMap<String, String> engineMetadata) throws IOException {
		if(!PVNames.stripFieldNameFromPVName(this.pvName).equals(PVNames.stripFieldNameFromPVName(info.getPvName()))) throw new IOException("Mismatch in pv info's. Src is for " + this.pvName + ". Info from config db is for " + info.getPvName());
		this.elementCount = info.getElementCount();
		if(!this.headers.containsKey("EGU")) { 
			this.headers.put("EGU", info.getUnits());
		}
		if(!this.headers.containsKey("PREC")) { 
			this.headers.put("PREC", Integer.valueOf((int)info.getPrecision().intValue()).toString());
		}

		// There are cases when we use operators where the DBR type of the PVTypeInfo is not the same as the DBR type of the event stream
		// So instead of throwing an exception; for now, I am just logging a warning.
		// Will revisit if lets thru event streams of difference kinds.
		// if(!this.archDBRType.equals(info.getDBRType())) throw new MismatchedDBRTypeException(info.getPvName(), info.getDBRType(), this.source, this.archDBRType);
		
		if(!this.archDBRType.equals(info.getDBRType()))  { 
			logger.warn("For pv " + this.pvName + " dbr types do not match for stream " + this.source);
		}
		
		if(engineMetadata != null) { 
			this.headers.putAll(engineMetadata);
		}
	}
	
	
	public void mergeInto(PayloadInfo.Builder builder) {
		if(!this.headers.isEmpty()) { 
			LinkedList<FieldValue> fieldValuesList = new LinkedList<FieldValue>();
			for(String fieldName : this.headers.keySet()) {
				String fieldValue = headers.get(fieldName);
				if(fieldValue != null && !fieldValue.isEmpty()) { 
					fieldValuesList.add(EPICSEvent.FieldValue.newBuilder().setName(fieldName).setVal(fieldValue).build());
				}
			}
			builder.addAllHeaders(fieldValuesList);
		}
	}

	public ArchDBRTypes getArchDBRType() {
		return archDBRType;
	}

	public void setArchDBRType(ArchDBRTypes archDBRType) {
		this.archDBRType = archDBRType;
	}
	public String getPvName() {
		return pvName;
	}
	public void setPvName(String pvName) {
		this.pvName = pvName;
	}
	public short getYear() {
		return year;
	}
	public void setYear(short year) {
		this.year = year;
	}
	public int getElementCount() {
		return elementCount;
	}
	public void setElementCount(int elementCount) {
		this.elementCount = elementCount;
	}

	public HashMap<String, String> getHeaders() {
		return headers;
	}
	
	public void addHeaders(Map<String, String> vals) { 
		this.headers.putAll(vals);
	}
	
	public void addHeader(String name, String value) { 
		this.headers.put(name, value);
	}
}
