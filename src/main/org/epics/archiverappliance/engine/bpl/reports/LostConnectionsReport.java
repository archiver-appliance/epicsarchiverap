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
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

public class LostConnectionsReport implements BPLAction {
	private static Logger logger = LogManager.getLogger(LostConnectionsReport.class.getName());
	
	private static class PVLostConnections {
		String pvName;
		long connectionLossRegainCount;
		boolean currentlyConnected;
		
		PVLostConnections(String pvName, long connectionDropCount, boolean currentlyConnected) {
			this.pvName = pvName;
			this.connectionLossRegainCount = connectionDropCount;
			this.currentlyConnected = currentlyConnected;
		}
	}
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String limit = req.getParameter("limit");
		logger.info("Lost connections rate report for " + (limit == null ? "default limit " : ("limit " + limit)));
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		List<PVLostConnections> lostConnections = getLostConnections(configService, limit);
		LinkedList<HashMap<String, String>> result = new LinkedList<HashMap<String, String>>();
		String identity = configService.getMyApplianceInfo().getIdentity();
		try (PrintWriter out = resp.getWriter()) {
			for(PVLostConnections lostConnection : lostConnections) {
				HashMap<String, String> pvStatus = new HashMap<String, String>();
				result.add(pvStatus);
				pvStatus.put("pvName", lostConnection.pvName);
				pvStatus.put("instance", identity);
				pvStatus.put("lostConnections", Long.toString(lostConnection.connectionLossRegainCount));
				pvStatus.put("currentlyConnected", (lostConnection.currentlyConnected ? "Yes" : "No"));
			}
			out.println(JSONValue.toJSONString(result));
		}
	}

	private static List<PVLostConnections> getLostConnections(ConfigService configService, String limit) {
		ArrayList<PVLostConnections> lostConnections = new ArrayList<PVLostConnections>(); 
		EngineContext engineContext = configService.getEngineContext();
		for(ArchiveChannel channel : engineContext.getChannelList().values()) {
			PVMetrics pvMetrics = channel.getPVMetrics();
			lostConnections.add(new PVLostConnections(pvMetrics.getPvName(), pvMetrics.getConnectionLossRegainCount(), channel.isConnected()));
		}

		Collections.sort(lostConnections, new Comparator<PVLostConnections>() {
			@Override
			public int compare(PVLostConnections o1, PVLostConnections o2) {
				if(o1.connectionLossRegainCount == o2.connectionLossRegainCount) return 0;
				return (o1.connectionLossRegainCount < o2.connectionLossRegainCount) ? 1 : -1; // We want a descending sort
			}
		});

		if(limit == null) {
			return lostConnections;
		}

		int limitNum = Integer.parseInt(limit);
		return lostConnections.subList(0, Math.min(limitNum, lostConnections.size()));
	}
}