/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.mgmt.bpl.PVsMatchingParameter;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * PV for getting the data for multiple PV's from the engine's buffers at a particular time.
 * We loop thru all the values and find the latest value in the engine that is older or equal to the specified time.
 * If no such sample is present, we do not add the PV to the JSON response. 
 * @author mshankar
 *
 */
public class GetDataAtTimeEngine implements BPLAction {
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		List<String> pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req, configService);
		
		String timeStr = req.getParameter("at");
		Timestamp atTime = TimeUtils.now();
		if(timeStr != null) { 
			atTime = TimeUtils.convertFromISO8601String(timeStr);
		}

		EngineContext engineContext = configService.getEngineContext();
		HashMap<String, HashMap<String, Object>> values = new HashMap<String, HashMap<String, Object>>();
		for(String pvName : pvNames) {
			if(engineContext.getChannelList().containsKey(pvName)){
				ArchiveChannel archiveChannel = engineContext.getChannelList().get(pvName);
				ArrayListEventStream st = archiveChannel.getPVData();
				DBRTimeEvent potentialEvent = null, dEv = null;
				for(Event ev : st) {
					dEv = (DBRTimeEvent) ev;
					if(dEv.getEventTimeStamp().before(atTime) || dEv.getEventTimeStamp().equals(atTime)) {
						if(potentialEvent != null) {
							if(dEv.getEventTimeStamp().after(potentialEvent.getEventTimeStamp())) {
								potentialEvent = dEv;								
							}
						} else {
							potentialEvent = dEv;
						}
					}
				}
				dEv = archiveChannel.getLastArchivedValue();
				if(dEv != null && (dEv.getEventTimeStamp().before(atTime) || dEv.getEventTimeStamp().equals(atTime))) {
					if(potentialEvent != null) {
						if(dEv.getEventTimeStamp().after(potentialEvent.getEventTimeStamp())) {
							potentialEvent = dEv;								
						}
					} else {
						potentialEvent = dEv;
					}
				}
				
				if(potentialEvent != null) {
					HashMap<String, Object> evnt = new HashMap<String, Object>();
					evnt.put("secs", potentialEvent.getEpochSeconds());
					evnt.put("nanos", potentialEvent.getEventTimeStamp().getNanos());
					evnt.put("severity", potentialEvent.getSeverity());
					evnt.put("status", potentialEvent.getStatus());
					evnt.put("val", JSONValue.parse(potentialEvent.getSampleValue().toJSONString()));
					values.put(pvName, evnt);
				}
			}
		}
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		try(PrintWriter out = resp.getWriter()) {
			JSONObject.writeJSONString(values, out);
		}
	}	
}
