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
 * 
 * @epics.BPLAction - Return a list of PVs sorted by number of times we've lost/reestablished connections (descending) to the IOC hosting the PV. Can be used to determine if the IOC is being overloaded. 
 * @epics.BPLActionParam limit - Limit this report to this many PVs per appliance in the cluster. Optional, if unspecified, there are no limits enforced.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class LostConnectionsReport implements BPLAction {
	private static Logger logger = LogManager.getLogger(LostConnectionsReport.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String limit = req.getParameter("limit");
		logger.info("Storage rate report for " + (limit == null ? "default limit " : ("limit " + limit)));
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		LinkedList<String> storageRateURLs = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			storageRateURLs.add(info.getEngineURL() + "/getLostConnectionsReport" + (limit == null ? "" : ("?limit=" + limit)));
		}		
		try (PrintWriter out = resp.getWriter()) {
			JSONArray neverConnPVs = GetUrlContent.combineJSONArrays(storageRateURLs);
			out.println(JSONValue.toJSONString(neverConnPVs));
		}
	}
}
