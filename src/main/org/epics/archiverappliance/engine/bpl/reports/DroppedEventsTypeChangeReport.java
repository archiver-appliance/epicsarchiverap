package org.epics.archiverappliance.engine.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

public class DroppedEventsTypeChangeReport implements BPLAction {
	private static Logger logger = Logger.getLogger(DroppedEventsTypeChangeReport.class.getName());
	private static class PVDroppedEvents {
		String pvName;
		long droppedEvents;
		
		PVDroppedEvents(String pvName, long droppedEvents) {
			this.pvName = pvName;
			this.droppedEvents = droppedEvents;
		}
	}
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		String limit = req.getParameter("limit");
		logger.info("Report for PVs that have dropped events because of type changes for " + (limit == null ? "default limit " : ("limit " + limit)));
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		List<PVDroppedEvents> eventRates = getDroppedEventsTypeChange(configService, limit);
		LinkedList<HashMap<String, String>> result = new LinkedList<HashMap<String, String>>();
		try (PrintWriter out = resp.getWriter()) {
			for(PVDroppedEvents eventRate : eventRates) {
				HashMap<String, String> pvStatus = new HashMap<String, String>();
				result.add(pvStatus);
				pvStatus.put("pvName", eventRate.pvName);
				pvStatus.put("eventsDropped", Long.toString(eventRate.droppedEvents));
			}
			out.println(JSONValue.toJSONString(result));
		}
	}
	
	private static List<PVDroppedEvents> getDroppedEventsTypeChange(ConfigService configService, String limit) {
		ArrayList<PVDroppedEvents> eventRates = new ArrayList<PVDroppedEvents>(); 
		EngineContext engineContext = configService.getEngineContext();
		for(ArchiveChannel channel : engineContext.getChannelList().values()) {
			PVMetrics pvMetrics = channel.getPVMetrics();
			if(pvMetrics.getInvalidTypeLostEventCount() > 0) {
				PVTypeInfo typeInfo = configService.getTypeInfoForPV(channel.getName());
				if(typeInfo != null && typeInfo.isPaused()) { 
					continue;
				}
				eventRates.add(new PVDroppedEvents(channel.getName(), pvMetrics.getInvalidTypeLostEventCount()));
			}
		}
		
		Collections.sort(eventRates, new Comparator<PVDroppedEvents>() {
			@Override
			public int compare(PVDroppedEvents o1, PVDroppedEvents o2) {
				if(o1.droppedEvents == o2.droppedEvents) return o1.pvName.compareTo(o2.pvName);
				return (o1.droppedEvents < o2.droppedEvents) ? 1 : -1; // We want a descending sort
			}
		});
		
		if(limit == null) {
			return eventRates;
		}
		
		int limitNum = Integer.parseInt(limit);
		return eventRates.subList(0, Math.min(limitNum, eventRates.size()));
	}

}
