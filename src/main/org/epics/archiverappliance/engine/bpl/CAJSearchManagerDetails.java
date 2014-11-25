package org.epics.archiverappliance.engine.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Given a pv, determine the context id and a timer id, use reflection to print the details of the ChannelSearchManager.
 * Currently using this to debug reconnectivity issues.
 * @author mshankar
 *
 */
public class CAJSearchManagerDetails implements BPLAction {
	private static Logger logger = Logger.getLogger(CAJSearchManagerDetails.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		ArchiveChannel channel = configService.getEngineContext().getChannelList().get(pvName);
		if(channel == null) { 
			logger.error("PV " + pvName + " does not have a channel.");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			configService.getEngineContext().getJCACommandThread(channel.getJCACommandThreadID()).getCAJSearchManagerDetails(pvName, infoValues, channel.getCurrentSearchTimer());
			out.println(JSONValue.toJSONString(infoValues));
		} catch(Exception ex) { 
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
	}
}
