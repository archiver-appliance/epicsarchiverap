package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;
import org.json.simple.JSONValue;

/**
 *
 * @epics.BPLAction - For PVs that are still in the archive PV workflow, skip the alias check where we examine the .NAME field to determine the real name. This is useful if you have pCAS servers that overload the .NAME field for something else. 
 * @epics.BPLActionParam pv - The name of the pv as it is in the archive PV workflow.
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class SkipAliasCheckAction implements BPLAction {
	private static Logger logger = Logger.getLogger(SkipAliasCheckAction.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		// If we have a typeinfo for this PV already, this comes too late.
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo != null) {
			logger.error("When skipping the alias check for " + pvName + ", the PV already has a typeinfo.");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		// Check if we have the archivePV request
		if(configService.getMgmtRuntimeState().isPVInWorkflow(pvName)) { 
			UserSpecifiedSamplingParams params = configService.getUserSpecifiedSamplingParams(pvName);
			if(params == null) { 
				logger.error("When skipping the alias check for " + pvName + ", the archive PV workflow finished before changing the alias check.");
				resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
				return;
			} else { 
				params.setSkipAliasCheck(true);
				configService.updateArchiveRequest(pvName, params);
				logger.debug("Done setting the alias check for pv " + pvName);
			}
		} else {
			// We don't have the request, someone else has it. 
			// Send it to everyone else; only if we are the originator
			if(req.getParameter("doNotProxy") != null) { 
				StringWriter pathAndQuery = new StringWriter();
				pathAndQuery.append("/skipAliasCheck?pv=");
				pathAndQuery.append(URLEncoder.encode(pvName, "UTF-8"));
				pathAndQuery.append("&doNotProxy=true");
				ProxyUtils.routeURLToOtherAppliances(configService, pathAndQuery.toString());
			} else { 
				logger.debug("We have a do not proxy. so not proxying.");
			}
		}

		HashMap<String, String> status = new HashMap<String, String>();
		status.put("status", "ok");
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(status));
		}

	}
}
