package org.epics.archiverappliance.engine.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.regex.Pattern;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;

/**
 * Get the last known timestamp for all PV's.
 * This does not sort/filter the results in any way intentionally. 
 * This is meant to be consumed by other reports in mgmt.
 * @author mshankar
 *
 */
public class LastKnownTimeStampReport implements BPLAction {
	private static Logger logger = LogManager.getLogger(LastKnownTimeStampReport.class.getName());
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Generating a last known timestamp report");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		Pattern pattern = null;
		if(req.getParameter("regex") != null) { 
			String nameToMatch = req.getParameter("regex");
			logger.debug("Finding PV's for regex " + nameToMatch);
			pattern = Pattern.compile(nameToMatch);
		}

		
		try (PrintWriter out = resp.getWriter()) {
			out.println("[");
			boolean first = true;
			for(ArchiveChannel channel : configService.getEngineContext().getChannelList().values()) {
				if(pattern != null && !pattern.matcher(channel.getName()).matches()) continue;
				
				PVMetrics pvMetrics = channel.getPVMetrics();
				if(first) { first = false; } else { out.println(","); }
				HashMap<String, String> ret = new HashMap<String, String>();
				ret.put("pvName", channel.getName());
				ret.put("lastEvent", Long.toString(pvMetrics.getLastEventFromIOCTimeStamp()));
				JSONObject.writeJSONString(ret, out);
			}
			out.println("]");
		}
	}
}