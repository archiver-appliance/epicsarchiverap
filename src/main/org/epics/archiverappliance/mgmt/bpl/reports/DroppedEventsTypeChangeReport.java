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
 * A report based on the number of events dropped based on changes in PV type.
 * 
 * 
 * @epics.BPLAction - Return a list of PVs sorted by number of times we've lost events because of changes in type of the underlying PV. This does not include PVs that have been paused.   
 * @epics.BPLActionParam limit - Limit this report to this many PVs per appliance in the cluster. Optional, if unspecified, there are no limits enforced.
 * @epics.BPLActionEnd
 * 
 * 
 * @author mshankar
 *
 */
public class DroppedEventsTypeChangeReport implements BPLAction {
	private static Logger logger = LogManager.getLogger(DroppedEventsTypeChangeReport.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String limit = req.getParameter("limit");
		logger.info("Report for PVs that have dropped events because of type changes for " + (limit == null ? "default limit " : ("limit " + limit)));
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		LinkedList<String> storageRateURLs = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			storageRateURLs.add(info.getEngineURL() + "/getPVsByDroppedEventsTypeChange" + (limit == null ? "" : ("?limit=" + limit)));
		}		
		try (PrintWriter out = resp.getWriter()) {
			JSONArray neverConnPVs = GetUrlContent.combineJSONArrays(storageRateURLs);
			out.println(JSONValue.toJSONString(neverConnPVs));
		}
	}

}
