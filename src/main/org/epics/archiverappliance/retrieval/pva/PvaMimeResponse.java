/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.pva;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map.Entry;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.nt.NTScalar;
import org.epics.pvdata.property.Alarm;
import org.epics.pvdata.property.AlarmSeverity;
import org.epics.pvdata.property.AlarmStatus;
import org.epics.pvdata.property.PVAlarm;
import org.epics.pvdata.property.PVAlarmFactory;
import org.epics.pvdata.pv.PVDouble;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVStructureArray;
import org.epics.pvdata.pv.ScalarType;
import org.json.simple.JSONValue;

/**
 * @author mshankar
 * The response is a array of PV elements, each PV has a meta and data section.
 * The data section has timestamp in epoch seconds and the value
 */
public class PvaMimeResponse implements MimeResponse {
	boolean firstPV = true;
	boolean closePV = false;
	private PVStructureArray pvStruct;

	@Override
	public void consumeEvent(Event e) throws Exception {
		//System.out.println("consume event");
		DBRTimeEvent evnt = (DBRTimeEvent)e;
		
		NTScalar a1 = NTScalar.createBuilder().value(ScalarType.pvDouble).addAlarm().create();
		
//		System.out.println(a1);		
		a1.getValue(PVDouble.class).put(evnt.getSampleValue().getValue().doubleValue());
		Alarm alarm = new Alarm();
		alarm.setSeverity(AlarmSeverity.getSeverity(evnt.getSeverity()));
		alarm.setStatus(AlarmStatus.getStatus(evnt.getStatus()));
		PVAlarm p = PVAlarmFactory.create();
		p.attach(a1.getAlarm());
		p.set(alarm);
		this.pvStruct.put(this.pvStruct.getLength(), 1, new PVStructure[] {a1.getPVStructure()}, 0);
//		System.out.println(a1);
	}

	private static String consumeMetadata(DBRTimeEvent evnt) { 
		if(evnt.hasFieldValues()) { 
			StringBuilder buf = new StringBuilder();
			buf.append(", \"fields\": { ");
			boolean metaComma = false;
			for(Entry<String, String> keyValue : evnt.getFields().entrySet()) { 
				if(!metaComma) { metaComma = true; } else { buf.append(","); }
				buf.append("\"");
				buf.append(keyValue.getKey());
				buf.append("\": \"");
				buf.append(JSONValue.escape(keyValue.getValue()));
				buf.append("\"");
			}
			buf.append("}");
			return buf.toString();
		} else { 
			return "";
		}
	}

	@Override
	public void setOutputStream(OutputStream os) {
		//TODO ??? 
	}
	
	public void setOutput(PVStructureArray pvStruct) {
		this.pvStruct = pvStruct;
	}

	public void close() {
		
	}

	@Override
	public void processingPV(String pv, Timestamp start, Timestamp end, EventStreamDesc streamDesc) {

		System.out.println(".....processingPV...." +pv);
		if(firstPV) {
			firstPV = false;
		}
		RemotableEventStreamDesc remoteDesc = (RemotableEventStreamDesc) streamDesc;
		StringWriter buf = new StringWriter();
		buf.append("{ \"name\": \"").append(pv).append("\" ");
		if(streamDesc != null) {
			HashMap<String, String> headers = remoteDesc.getHeaders();
			if(!headers.isEmpty()) { 
				for(String fieldName : headers.keySet()) {
					String fieldValue = headers.get(fieldName);
					if(fieldValue != null && !fieldValue.isEmpty()) { 
						buf.append(", \"" + fieldName + "\": \"").append(fieldValue).append("\" ");
					}
				}
			}
		}
		buf.append("}");
		NTScalar meta = NTScalar.createBuilder().value(ScalarType.pvString).create();
		meta.getValue(PVString.class).put(buf.toString());
		System.out.println(meta);
		closePV = true;
	}
	
	public void swicthingToStream(EventStream strm) {
		// Not much to do here for now.
	}

	@Override
	public HashMap<String, String> getExtraHeaders() {
		HashMap<String, String> ret = new HashMap<String, String>();
		// Allow applications served from other URL's to access the JSON data from this server.
		ret.put(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return ret;
	}
}
