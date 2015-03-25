package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Add interval to keep data for with respect to ETL gating.
 * Forward the request to the ETL application of all appliances.
 */
public class AddETLGatingInterval implements BPLAction {
	private static final Logger logger = Logger.getLogger(AddETLGatingInterval.class);
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String gatingScope = req.getParameter("gatingScope");
		if(gatingScope == null || gatingScope.equals("")) {
			logger.warn("Missing gatingScope parameter");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		long startMillis;
		try {
			startMillis = readTimeParameter(req, "from");
		} catch (Exception ex) {
			logger.warn("Bad 'from' parameter", ex);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		long endMillis;
		try {
			endMillis = readTimeParameter(req, "to");
		} catch (Exception ex) {
			logger.warn("Bad 'to' parameter", ex);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		logger.info(String.format("Forwarding ETL gating interval request, gatingScope=%s from=%s to=%s",
			gatingScope, new Timestamp(startMillis), new Timestamp(endMillis)));
		
		LinkedList<String> etlReqUrls = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			etlReqUrls.add(String.format("%s/addGatingInterval?gatingScope=%s&startMillis=%d&endMillis=%d",
				info.getEtlURL(), URLEncoder.encode(gatingScope, "UTF-8"), startMillis, endMillis));
		}
		
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			boolean successful = true;
			for (String etlReqUrl : etlReqUrls) {
				JSONObject reqResp = GetUrlContent.getURLContentAsJSONObject(etlReqUrl);
				if (!(reqResp != null && reqResp.containsKey("status") && reqResp.get("status").equals("ok"))) {
					successful = false;
				}
			}
			infoValues.put("status", successful ? "ok" : "no");
			infoValues.put("desc", "forwarded ETL gating request");
			out.println(JSONValue.toJSONString(infoValues));
		}
	}
	
	private long readTimeParameter(HttpServletRequest req, String parameterName) throws Exception {
		String strValue = req.getParameter(parameterName);
		if (strValue == null) {
			throw new Exception("missing parameter");
		}
		
		long millis;
		try {
			millis = Long.parseLong(strValue);
		} catch (NumberFormatException ex) {
			try {
				Timestamp ts = TimeUtils.convertFromISO8601String(strValue);
				millis = ts.getTime();
			} catch (IllegalArgumentException ex1) {
				throw new Exception("incorrect value (neither milliseconds integer nor ISO8601)");
			}
		}
		
		return millis;
	}
}
