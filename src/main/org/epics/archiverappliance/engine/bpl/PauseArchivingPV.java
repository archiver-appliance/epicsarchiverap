package org.epics.archiverappliance.engine.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

public class PauseArchivingPV implements BPLAction {
	private static Logger logger = LogManager.getLogger(PauseArchivingPV.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvNamesStr = req.getParameter("pv");
		if(pvNamesStr == null || pvNamesStr.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String[] pvNames = pvNamesStr.split(",");

		LinkedList<HashMap<String, Object>> statuses = new LinkedList<HashMap<String, Object>>();
		for(String pvName : pvNames) { 
			HashMap<String, Object> infoValues = new HashMap<String, Object>();
			infoValues.put("pvName", pvName);
			statuses.add(infoValues);
			try {
				logger.debug("Calling the engine to pause PV " + pvName);
				ArchiveEngine.pauseArchivingPV(pvName, configService);
				infoValues.put("status", "ok");
				infoValues.put("desc", "Successfully paused the archiving of PV " + pvName);
			} catch(Exception ex) {
				logger.error("Exception pausing PV " + pvName, ex);
				infoValues.put("status", "failed");
				infoValues.put("validation", ex.getMessage());
			}
		}
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(statuses));
		}
	}
}
