package org.epics.archiverappliance.mgmt.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

/**
 * 
 * @epics.BPLAction - Get a list of PVs that are currently disconnected. 
 * @epics.BPLActionEnd
 * 
 * 
 * @author mshankar
 *
 */
public class CurrentlyDisconnectedPVs implements BPLAction {
	private static Logger logger = LogManager.getLogger(CurrentlyDisconnectedPVs.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Getting the list of pvs that are currently disconnected.");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		LinkedList<String> neverConnUrls = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			neverConnUrls.add(info.getEngineURL() + "/getCurrentlyDisconnectedPVsForThisAppliance");
		}		

		try (PrintWriter out = resp.getWriter()) {
			JSONArray neverConnPVs = GetUrlContent.combineJSONArrays(neverConnUrls);
			out.println(JSONValue.toJSONString(neverConnPVs));
		}
	}

}
