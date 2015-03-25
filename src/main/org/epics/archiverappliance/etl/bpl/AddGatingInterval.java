package org.epics.archiverappliance.etl.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Add interval to keep data for with respect to ETL gating.
 */
public class AddGatingInterval implements BPLAction {
	private static final Logger logger = Logger.getLogger(AddGatingInterval.class);
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String gatingScope = req.getParameter("gatingScope");
		if(gatingScope == null || gatingScope.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String startStr = req.getParameter("startMillis");
		String endStr = req.getParameter("endMillis");
		if (startStr == null || endStr == null) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		long startMillis;
		long endMillis;
		try {
			startMillis = Long.parseLong(startStr);
			endMillis = Long.parseLong(endStr);
		} catch (NumberFormatException ex) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		configService.getETLLookup().getGatingState().keepTimeInterval(gatingScope, startMillis, endMillis);
		
		try (PrintWriter out = resp.getWriter()) {
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			HashMap<String, Object> infoValues = new HashMap<String, Object>();
			infoValues.put("status", "ok");
			infoValues.put("desc", "successfully merged gating interval");
			out.println(JSONValue.toJSONString(infoValues));
		}
	}
}
