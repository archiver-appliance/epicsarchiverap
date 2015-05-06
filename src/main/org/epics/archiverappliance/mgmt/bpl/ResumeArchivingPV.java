package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * 
 * @epics.BPLAction - Resume archiving the specified PV. 
 * @epics.BPLActionParam pv - The name of the pv.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class ResumeArchivingPV implements BPLAction {
	private static Logger logger = Logger.getLogger(ResumeArchivingPV.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		// String pvNameFromRequest = pvName;
		String realName = configService.getRealNameForAlias(pvName);
		if(realName != null) pvName = realName;


		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		ApplianceInfo info = configService.getApplianceForPV(pvName);
		if(info == null) {
			infoValues.put("validation", "Trying to resume PV " + pvName + " that is not currently being archived.");
			logger.error(infoValues.get("validation"));
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("validation", "Unable to resume PV " + pvName);
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		} else {
			PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
			if(!typeInfo.isPaused()) {
				infoValues.put("validation", "Trying to resume PV " + pvName + " that is not paused.");
				logger.error(infoValues.get("validation"));
				try(PrintWriter out = resp.getWriter()) {
					infoValues.put("validation", "PV " + pvName + " is not paused");
					out.println(JSONValue.toJSONString(infoValues));
				}
				return;
			}
			typeInfo.setPaused(false);
			typeInfo.setModificationTime(TimeUtils.now());
			configService.updateTypeInfoForPV(pvName, typeInfo);

			logger.debug("Asking engine to start archiving PV. This should create a channel object if it does not exist");
			String engineResumeURL = info.getEngineURL() + "/resumeArchivingPV" + "?pv=" + URLEncoder.encode(pvName, "UTF-8"); 
			JSONObject engineStatus = GetUrlContent.getURLContentAsJSONObject(engineResumeURL);
			if(engineStatus != null && !engineStatus.equals("")) {
				try(PrintWriter out = resp.getWriter()) {
					out.println(JSONValue.toJSONString(engineStatus));
				}
			} else {
				try(PrintWriter out = resp.getWriter()) {
					infoValues.put("validation", "Unable to resume PV " + pvName);
					out.println(JSONValue.toJSONString(infoValues));
				}
			}
		}
	}
}
