/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.pva;

import java.io.OutputStream;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.HashMap;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.nt.NTScalar;
import org.epics.pvdata.property.Alarm;
import org.epics.pvdata.property.AlarmSeverity;
import org.epics.pvdata.property.AlarmStatus;
import org.epics.pvdata.property.PVAlarm;
import org.epics.pvdata.property.PVAlarmFactory;
import org.epics.pvdata.property.PVTimeStamp;
import org.epics.pvdata.property.PVTimeStampFactory;
import org.epics.pvdata.property.TimeStamp;
import org.epics.pvdata.property.TimeStampFactory;
import org.epics.pvdata.pv.PVDouble;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVStructureArray;
import org.epics.pvdata.pv.ScalarType;

/**
 * @author mshankar The response is a array of PV elements, each PV has a meta
 *         and data section. The data section has timestamp in epoch seconds and
 *         the value
 */
public class PvaMimeResponse implements MimeResponse {
	boolean firstPV = true;
	boolean closePV = false;
	private PVStructureArray pvStruct;

	@Override
	public void consumeEvent(Event e) throws Exception {
		DBRTimeEvent evnt = (DBRTimeEvent) e;
		
		NTScalar struct = NTScalar.createBuilder().value(ScalarType.pvDouble).addAlarm().addTimeStamp().create();

		// Put the value
		
		struct.getValue(PVDouble.class).put(evnt.getSampleValue().getValue().doubleValue());

		// Put the alarm info
		Alarm alarm = new Alarm();
		alarm.setSeverity(AlarmSeverity.getSeverity(evnt.getSeverity()));
		alarm.setStatus(AlarmStatus.getStatus(evnt.getStatus()));
		
		PVAlarm pvAlarm = PVAlarmFactory.create();
		pvAlarm.attach(struct.getAlarm());
		pvAlarm.set(alarm);

		// Put time info
		TimeStamp ts = TimeStampFactory.create();
		ts.put(TimeUtils.convertToEpochSeconds(evnt.getEventTimeStamp()), evnt.getEventTimeStamp().getNanos());
		
		PVTimeStamp pvTimeStamp = PVTimeStampFactory.create();
		pvTimeStamp.attach(struct.getTimeStamp());
		pvTimeStamp.set(ts);

		this.pvStruct.put(this.pvStruct.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
	}

	@Override
	public void setOutputStream(OutputStream os) {
		// TODO ???
	}

	public void setOutput(PVStructureArray pvStruct) {
		this.pvStruct = pvStruct;
	}

	public void close() {

	}

	@Override
	public void processingPV(String pv, Timestamp start, Timestamp end, EventStreamDesc streamDesc) {
		if (firstPV) {
			firstPV = false;
		}
		RemotableEventStreamDesc remoteDesc = (RemotableEventStreamDesc) streamDesc;
		StringWriter buf = new StringWriter();
		buf.append("{ \"name\": \"").append(pv).append("\" ");
		if (streamDesc != null) {
			HashMap<String, String> headers = remoteDesc.getHeaders();
			if (!headers.isEmpty()) {
				for (String fieldName : headers.keySet()) {
					String fieldValue = headers.get(fieldName);
					if (fieldValue != null && !fieldValue.isEmpty()) {
						buf.append(", \"" + fieldName + "\": \"").append(fieldValue).append("\" ");
					}
				}
			}
		}
		buf.append("}");
		NTScalar meta = NTScalar.createBuilder().value(ScalarType.pvString).create();
		meta.getValue(PVString.class).put(buf.toString());
		// TODO this could populate the label
		System.out.println(meta);
		closePV = true;
	}

	public void swicthingToStream(EventStream strm) {
		// Not much to do here for now.
	}

	@Override
	public HashMap<String, String> getExtraHeaders() {
		HashMap<String, String> ret = new HashMap<String, String>();
		// Allow applications served from other URL's to access the JSON data from this
		// server.
		ret.put(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return ret;
	}
}
