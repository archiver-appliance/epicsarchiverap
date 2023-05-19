package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;

/**
 * 
 * @epics.BPLAction - Get the value of the named flag specified by name 
 * @epics.BPLActionParam name - the name of the named flag.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class NamedFlagsGet implements BPLAction {
	private static Logger logger = LogManager.getLogger(NamedFlagsGet.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String name = req.getParameter("name");
		if(name == null || name.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		boolean namedValue = configService.getNamedFlag(name);
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			HashMap<String, Object> ret = new HashMap<String, Object>();
			ret.put(name, Boolean.valueOf(namedValue));
			out.println(JSONObject.toJSONString(ret));
		} catch(Exception ex) {
			logger.error("Exception getting named value for name " + name, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
		
	}
}
