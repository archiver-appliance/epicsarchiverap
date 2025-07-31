package org.epics.archiverappliance.engine.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

public class DeletePV implements BPLAction {
	private static Logger logger = LogManager.getLogger(DeletePV.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		try {
			logger.debug("Calling the engine to stop archiving PV " + pvName);
			ArchiveEngine.destoryPv(pvName, configService);
			HashMap<String, Object> infoValues = new HashMap<String, Object>();
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("status", "ok");
				infoValues.put("desc", "Successfully stopped the archiving of PV " + pvName);
				out.println(JSONValue.toJSONString(infoValues));
			}
		} catch(Exception ex) {
			logger.error("Exception stopping archiving of PV " + pvName, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}
		
	}
}
