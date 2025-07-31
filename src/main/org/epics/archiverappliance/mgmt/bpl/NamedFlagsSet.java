package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;

/**
 * 
 * @epics.BPLAction - Set the value of the named flag specified by name 
 * @epics.BPLActionParam name - the name of the named flag.
 * @epics.BPLActionParam value - Either true of false; something that Boolean.parse can understand.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class NamedFlagsSet implements BPLAction {
	private static Logger logger = LogManager.getLogger(NamedFlagsSet.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String name = req.getParameter("name");
		if(name == null || name.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String value = req.getParameter("value");
		if(value == null || value.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			configService.setNamedFlag(name, Boolean.parseBoolean(value));
			HashMap<String, String> ret = new HashMap<String, String>();
			ret.put("status", "ok");
			out.println(JSONObject.toJSONString(ret));
		} catch(Exception ex) {
			logger.error("Exception getting named value for name " + name, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
		
	}
}
