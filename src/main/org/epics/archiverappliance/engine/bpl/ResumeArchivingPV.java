package org.epics.archiverappliance.engine.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

public class ResumeArchivingPV implements BPLAction {
	private static Logger logger = Logger.getLogger(ResumeArchivingPV.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		try {
			logger.debug("Calling the engine to resume PV " + pvName);
			ArchiveEngine.resumeArchivingPV(pvName, configService);
			HashMap<String, Object> infoValues = new HashMap<String, Object>();
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("status", "ok");
				infoValues.put("desc", "Successfully resumed the archiving of PV " + pvName);
				out.println(JSONValue.toJSONString(infoValues));
			}
		} catch(Exception ex) {
			logger.error("Exception resuming PV " + pvName, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}
		
	}
}
