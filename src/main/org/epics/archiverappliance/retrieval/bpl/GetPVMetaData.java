package org.epics.archiverappliance.retrieval.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.HashMap;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Gets information about the PV from PVTypeInfo, the engine and other places
 * 
 * Gets information about the PV from PVTypeInfo, the engine and other places
 * <ol> 
 * <li>pv - The name of the pv.</li>
 * </ol>
 * @author mshankar
 *
 */
public class GetPVMetaData implements BPLAction {
	private static Logger logger = LogManager.getLogger(GetPVMetaData.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		logger.debug("Getting metadata for PV " + pvName);
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}

		// String pvNameFromRequest = pvName;
		String realName = configService.getRealNameForAlias(pvName);
		if(realName != null) pvName = realName;

		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			logger.warn("Cannot find typeinfo for " + pvName);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		try (PrintWriter out = resp.getWriter()) {
			HashMap<String, String> retVal = new HashMap<String, String>();
			JSONEncoder<PVTypeInfo> jsonEncoder = JSONEncoder.getEncoder(PVTypeInfo.class);
			JSONObject typeInfoJSON = jsonEncoder.encode(typeInfo);
			GetUrlContent.combineJSONObjects(retVal, typeInfoJSON);
			String engineURL = configService.getAppliance(typeInfo.getApplianceIdentity()).getEngineURL() + "/getMetadata?pv=" + URLEncoder.encode(pvName, "UTF-8");
			JSONObject engineMetaData = GetUrlContent.getURLContentAsJSONObject(engineURL);
			if(engineMetaData != null) { 
				GetUrlContent.combineJSONObjects(retVal, engineMetaData);
				out.println(JSONValue.toJSONString(retVal));
			}
		} catch(Exception ex) {
			logger.error("Exception getting metadata typeinfo for pv " + pvName, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}
}
