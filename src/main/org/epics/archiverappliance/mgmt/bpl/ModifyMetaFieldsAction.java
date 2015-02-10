package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @epics.BPLAction - Modify the fields (HIHI, LOLO etc) being archived as part of the PV. PV needs to be paused first.
 * @epics.BPLActionParam pv - The real name of the pv.
 * @epics.BPLActionParam command - A command is a verb followed by a list of fields, all of them comma separated. Possible verbs are <code>add</code>, <code>remove</code> and <code>clear</code>. For example <code>add,ADEL,MDEL</code> will add the fields ADEL and MDEL if they are not already present. <code>clear</code> clears all the fields. You can have any number of commands.  
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class ModifyMetaFieldsAction implements BPLAction {
	private static Logger logger = Logger.getLogger(ModifyMetaFieldsAction.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		String[] commands = req.getParameterValues("command");
		if(commands == null || commands.length <= 0) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			logger.error("Cannot find typeinfo for pv " + pvName);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}
		
		if(!typeInfo.isPaused()) { 
			String msg = "Please pause the pv " + pvName + " first";
			logger.error(msg);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
			return;
		}
		
		ApplianceInfo infoForPV = configService.getApplianceForPV(pvName);
		if(!infoForPV.equals(configService.getMyApplianceInfo())) {
			// Route to the appliance that hosts the PVTypeInfo
			StringWriter buf = new StringWriter();
			buf.append(infoForPV.getMgmtURL());
			buf.append("/modifyMetaFields?"); 
			buf.append(req.getQueryString());
			String redirectURL = buf.toString();
			logger.info("Redirecting modifyMetaFields request for PV to " + infoForPV.getIdentity() + " using URL " + redirectURL);
			JSONObject status = GetUrlContent.getURLContentAsJSONObject(redirectURL);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(status));
			}
			return;
		}
		
		String[] originalArchiveFields = typeInfo.getArchiveFields();
		HashSet<String> archiveFields = new HashSet<String>();
		if(originalArchiveFields.length > 0) { 
			for(String originalArchiveField : originalArchiveFields) { 
				archiveFields.add(originalArchiveField);
			}
		}
		
		for(String command : commands) {
			String[] parts = command.split(",");
			if(parts.length <= 0) { 
				logger.debug("Cannot determine verb for " + command);
				continue;
			}
			String verb = parts[0];
			switch(verb) { 
			case "add":
				for(int i = 1; i < parts.length; i++) { 
					logger.debug("Adding meta field " + parts[i]);
					archiveFields.add(parts[i]);
				}
				continue;
			case "remove":
				for(int i = 1; i < parts.length; i++) { 
					logger.debug("Removing meta field " + parts[i]);
					archiveFields.remove(parts[i]);
				}
				continue;
			case "clear":
				logger.debug("Clearing meta fields");
				archiveFields.clear();
				continue;
			default:
				logger.debug("Can't understand command " + command);
				continue;	
			}
		}
		
		typeInfo.setArchiveFields(archiveFields.toArray(new String[0]));
		configService.updateTypeInfoForPV(pvName, typeInfo);
		logger.info("Final set of archive fields " + typeInfo.obtainArchiveFieldsAsString());
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			HashMap<String, String> ret = new HashMap<String, String>();
			ret.put("pvName", pvName);
			ret.put("fields", typeInfo.obtainArchiveFieldsAsString());
			JSONValue.writeJSONString(ret, out);
		}		
	}
}
