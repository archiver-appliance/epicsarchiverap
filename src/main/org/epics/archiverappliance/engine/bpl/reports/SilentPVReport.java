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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

public class SilentPVReport implements BPLAction {
	private static Logger logger = LogManager.getLogger(SilentPVReport.class.getName());
	
	private static class SilentPV {
		String pvName;
		long epochSecondsOflastKnownEvent;
		
		SilentPV(String pvName, long epochSecondsOflastKnownEvent) {
			this.pvName = pvName;
			this.epochSecondsOflastKnownEvent = epochSecondsOflastKnownEvent;
		}
	}
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String limit = req.getParameter("limit");
		logger.info("Silent PV report for " + (limit == null ? "default limit " : ("limit " + limit)));
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		List<SilentPV> silentPVs = getSilentPVs(configService, limit);
		LinkedList<HashMap<String, String>> result = new LinkedList<HashMap<String, String>>();
		String identity = configService.getMyApplianceInfo().getIdentity();
		try (PrintWriter out = resp.getWriter()) {
			for(SilentPV silentPV : silentPVs) {
				HashMap<String, String> pvStatus = new HashMap<String, String>();
				result.add(pvStatus);
				pvStatus.put("pvName", silentPV.pvName);
				pvStatus.put("instance", identity);
				pvStatus.put("lastKnownEvent", TimeUtils.convertToHumanReadableString(silentPV.epochSecondsOflastKnownEvent));
			}
			out.println(JSONValue.toJSONString(result));
		}
	}

	private static List<SilentPV> getSilentPVs(ConfigService configService, String limit) {
		ArrayList<SilentPV> silentPVs = new ArrayList<SilentPV>(); 
		EngineContext engineContext = configService.getEngineContext();
		for(ArchiveChannel channel : engineContext.getChannelList().values()) {
			PVMetrics pvMetrics = channel.getPVMetrics();
			silentPVs.add(new SilentPV(pvMetrics.getPvName(), pvMetrics.getSecondsOfLastEvent()));
		}

		Collections.sort(silentPVs, new Comparator<SilentPV>() {
			@Override
			public int compare(SilentPV o1, SilentPV o2) {
				if(o1.epochSecondsOflastKnownEvent == o2.epochSecondsOflastKnownEvent) return 0;
				return (o1.epochSecondsOflastKnownEvent < o2.epochSecondsOflastKnownEvent) ? -1 : 1;
			}
		});

		if(limit == null) {
			return silentPVs;
		}

		int limitNum = Integer.parseInt(limit);
		return silentPVs.subList(0, Math.min(limitNum, silentPVs.size()));
	}
}