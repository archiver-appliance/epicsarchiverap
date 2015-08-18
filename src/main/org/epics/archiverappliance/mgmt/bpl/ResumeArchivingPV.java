package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.LinkedList;

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
 * @epics.BPLActionParam pv - The name of the pv. You can also pass in GLOB wildcards here and multiple PVs as a comma separated list. If you have more PVs that can fit in a GET, send the pv's as a CSV <code>pv=pv1,pv2,pv3</code> as the body of a POST.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class ResumeArchivingPV implements BPLAction {
	private static Logger logger = Logger.getLogger(ResumeArchivingPV.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		if(req.getMethod().equals("POST")) { 
			resumeMultiplePVs(req, resp, configService);
			return;
		}

		
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		if(pvName.contains(",") || pvName.contains("*") || pvName.contains("?")) { 
			resumeMultiplePVs(req, resp, configService);
		} else { 
			// We only have one PV in the request
			resumeSinglePV(req, resp, configService);
		}

	}

	private void resumeSinglePV(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException, UnsupportedEncodingException {
		String pvName = req.getParameter("pv");
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

	private void resumeMultiplePVs(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException, UnsupportedEncodingException {
		LinkedList<String> pvNames = null;
		if(req.getMethod().equals("POST")) { 
			pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req, configService);
		} else { 
			pvNames = PVsMatchingParameter.getMatchingPVs(req, configService, false, -1);
		}

		// String pvNameFromRequest = pvName;
		try(PrintWriter out = resp.getWriter()) {
			boolean printComma = false;
			out.println("[");
			for(String pvName : pvNames) { 
				if(printComma) { 
					out.println(",");
				} else { 
					printComma = true;
				}

				String realName = configService.getRealNameForAlias(pvName);
				if(realName != null) pvName = realName;


				HashMap<String, Object> infoValues = new HashMap<String, Object>();
				resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

				ApplianceInfo info = configService.getApplianceForPV(pvName);
				if(info == null) {
					infoValues.put("validation", "Trying to resume PV " + pvName + " that is not currently being archived.");
					logger.error(infoValues.get("validation"));
					infoValues.put("validation", "Unable to resume PV " + pvName);
					out.print(JSONValue.toJSONString(infoValues));
				} else {
					PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
					if(!typeInfo.isPaused()) {
						infoValues.put("validation", "Trying to resume PV " + pvName + " that is not paused.");
						logger.error(infoValues.get("validation"));
						infoValues.put("validation", "PV " + pvName + " is not paused");
						out.print(JSONValue.toJSONString(infoValues));
					} else { 
						typeInfo.setPaused(false);
						typeInfo.setModificationTime(TimeUtils.now());
						configService.updateTypeInfoForPV(pvName, typeInfo);

						logger.debug("Asking engine to resume archiving PV. This should create a channel object if it does not exist");
						String engineResumeURL = info.getEngineURL() + "/resumeArchivingPV" + "?pv=" + URLEncoder.encode(pvName, "UTF-8"); 
						JSONObject engineStatus = GetUrlContent.getURLContentAsJSONObject(engineResumeURL);
						if(engineStatus != null && !engineStatus.equals("")) {
								out.print(JSONValue.toJSONString(engineStatus));
						} else {
								infoValues.put("validation", "Unable to resume PV " + pvName);
								out.print(JSONValue.toJSONString(infoValues));
						}
					}
				}
			}
			out.println("]");
		}
	}
}
