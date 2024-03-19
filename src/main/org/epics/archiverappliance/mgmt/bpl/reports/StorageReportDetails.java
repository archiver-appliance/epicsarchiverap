package org.epics.archiverappliance.mgmt.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;

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

public class StorageReportDetails implements BPLAction {
	private static Logger logger = LogManager.getLogger(StorageReportDetails.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String applianceIdentity = req.getParameter("appliance");
		logger.info("Getting the storage details for the appliance " + applianceIdentity);
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		String applianceDetailsURLSnippet = "/getStorageDetailsForAppliance?appliance=" + URLEncoder.encode(applianceIdentity, "UTF-8");
		ApplianceInfo info = configService.getAppliance(applianceIdentity);
		try (PrintWriter out = resp.getWriter()) {
			JSONArray etlStatusVars = GetUrlContent.getURLContentAsJSONArray(info.getEtlURL() + applianceDetailsURLSnippet );
			if(etlStatusVars == null) {
				logger.warn("No status vars from ETL using URL " + info.getEtlURL() + applianceDetailsURLSnippet);
				out.println("[]");
			} else {
				out.println(JSONValue.toJSONString(etlStatusVars));
			}

		}
	}
}