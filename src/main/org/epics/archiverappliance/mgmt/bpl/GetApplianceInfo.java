package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;

/**
 * 
 * @epics.BPLAction - Get the appliance information for the specified appliance. 
 * @epics.BPLActionParam id - The identity of the appliance for which we are requesting information. This is the same string as the <code>identity</code> element in the <code>appliances.xml</code> that identifies this appliance.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class GetApplianceInfo implements BPLAction {
	private static Logger logger = Logger.getLogger(GetApplianceInfo.class.getName());
	@SuppressWarnings("unchecked")
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String id = req.getParameter("id");
		ApplianceInfo applianceInfo = null;
		if(id == null || id.equals("")) {
			applianceInfo = configService.getMyApplianceInfo();
			logger.debug("No id specified, returning the id of this appliance " + applianceInfo.getIdentity());
		} else {
			logger.debug("Getting Appliance info for appliance with identity " + id);
			applianceInfo = configService.getAppliance(id);			
		}
		
		if(applianceInfo == null) {
			logger.warn("Cannot find appliance info for " + id);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}

		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			JSONEncoder<ApplianceInfo> jsonEncoder = JSONEncoder.getEncoder(ApplianceInfo.class);
			JSONObject output = jsonEncoder.encode(applianceInfo);
			Path versionPath = Paths.get(req.getServletContext().getRealPath("ui/comm/version.txt"));
			String versionString = Files.readAllLines(versionPath, Charset.defaultCharset()).get(0);
			output.put("version", versionString);
			out.println(output);
		} catch(Exception ex) {
			logger.error("Exception marshalling typeinfo for appliance " + id, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}
}
