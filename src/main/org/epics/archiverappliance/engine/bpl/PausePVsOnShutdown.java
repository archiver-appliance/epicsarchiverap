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
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Pause all PVs; this does not update PVTypeinfo.
 * This is an internal call that is typically to be used only on shutdown.
 * @author mshankar
 *
 */
public class PausePVsOnShutdown implements BPLAction {
	private static Logger configlogger = Logger.getLogger("config." + PausePVsOnShutdown.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		configlogger.info("Pausing PVs on potential shutdown");
		int pvCount = ArchiveEngine.pauseAllPvs(configlogger, configService);
		
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			infoValues.put("status", "ok");
			infoValues.put("desc", "Successfully paused " + pvCount + " pvs");
			out.println(JSONValue.toJSONString(infoValues));
		}
	}
}
