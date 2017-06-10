package org.epics.archiverappliance.engine.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

public class CurrentlyDisconnectedPVsAction implements BPLAction {
	private static Logger logger = Logger.getLogger(CurrentlyDisconnectedPVsAction.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String identity = configService.getMyApplianceInfo().getIdentity();
		logger.info("Currently disconnected PVs for appliance " + configService.getMyApplianceInfo().getIdentity());
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		LinkedList<HashMap<String, String>> result = new LinkedList<HashMap<String, String>>();
		Set<String> pausedPVs = configService.getPausedPVsInThisAppliance();
		try (PrintWriter out = resp.getWriter()) {
			for(ArchiveChannel channel : configService.getEngineContext().getChannelList().values()) {
				PVMetrics pvMetrics = channel.getPVMetrics();
				if(!pvMetrics.isConnected()) { 
					String pvName = pvMetrics.getPvName();
					if(pausedPVs.contains(pvName)) { 
						logger.debug("Skipping a paused PV " + pvName);
						continue;
					}
					HashMap<String, String> pvStatus = new HashMap<String, String>();
					result.add(pvStatus);
					pvStatus.put("pvName", pvName);
					pvStatus.put("instance", identity);
					pvStatus.put("lastKnownEvent", TimeUtils.convertToHumanReadableString(pvMetrics.getSecondsOfLastEvent()));
					long connectionLastLostEpochSeconds = pvMetrics.getConnectionLastLostEpochSeconds();
					pvStatus.put("connectionLostAt", connectionLastLostEpochSeconds > 0 ? TimeUtils.convertToHumanReadableString(connectionLastLostEpochSeconds) : TimeUtils.convertToHumanReadableString(configService.getTimeOfAppserverStartup()));
					pvStatus.put("noConnectionAsOfEpochSecs", Long.toString(connectionLastLostEpochSeconds > 0 ? connectionLastLostEpochSeconds : configService.getTimeOfAppserverStartup()));
					String hostName = channel.getHostName();
					pvStatus.put("hostName", hostName != null ? hostName : "N/A");
					pvStatus.put("commandThreadID", Integer.toString(channel.getJCACommandThreadID()));
					String internalState = channel.getInternalState();
					pvStatus.put("internalState", internalState != null ? internalState : "N/A");
				}
			}
			out.println(JSONValue.toJSONString(result));
		}
	}
}
