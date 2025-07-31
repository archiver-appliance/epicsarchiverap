package org.epics.archiverappliance.mgmt.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

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
 * A report based on the number of events dropped based on incorrect timestamps from the IOC.
 * 
 * @epics.BPLAction - Return a list of PVs sorted by number of times we've lost events because of incorrect timestamps. 
 * @epics.BPLActionParam limit - Limit this report to this many PVs per appliance in the cluster. Optional, if unspecified, there are no limits enforced.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class DroppedEventsTimestampReport implements BPLAction {
	private static Logger logger = LogManager.getLogger(DroppedEventsTimestampReport.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String limit = req.getParameter("limit");
		logger.info("Report for PVs that have dropped events because of incorrect timestamps for " + (limit == null ? "default limit " : ("limit " + limit)));
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		LinkedList<String> storageRateURLs = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			storageRateURLs.add(info.getEngineURL() + "/getPVsByDroppedEventsTimestamp" + (limit == null ? "" : ("?limit=" + limit)));
		}		
		try (PrintWriter out = resp.getWriter()) {
			JSONArray neverConnPVs = GetUrlContent.combineJSONArrays(storageRateURLs);
			out.println(JSONValue.toJSONString(neverConnPVs));
		}
	}

}
