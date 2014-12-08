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
 * @epics.BPLAction - Pause archiving the specified PV. This also tears down the CA channel for this PV. 
 * @epics.BPLActionParam pv - The name of the pv. You can also pass in GLOB wildcards here and multiple PVs as a comma separated list. If you have more PVs that can fit in a GET, send the pv's as a CSV <code>pv=pv1,pv2,pv3</code> as the body of a POST.
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class PauseArchivingPV implements BPLAction {
	private static Logger logger = Logger.getLogger(PauseArchivingPV.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		if(pvName.contains(",") || pvName.contains("*") || pvName.contains("?")) { 
			pauseMultiplePVs(req, resp, configService);
		} else { 
			// We only have one PV in the request
			pauseSinglePV(req, resp, configService);
		}
	}

	private void pauseSinglePV(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException, UnsupportedEncodingException {
		// String pvNameFromRequest = pvName;
		String pvName = req.getParameter("pv");
		String realName = configService.getRealNameForAlias(pvName);
		if(realName != null) pvName = realName;


		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		ApplianceInfo info = configService.getApplianceForPV(pvName);
		if(info == null) {
			infoValues.put("validation", "Trying to pause PV " + pvName + " that is not currently being archived.");
			logger.error(infoValues.get("validation"));
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("validation", "Unable to pause PV " + pvName);
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		} else {
			PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
			if(typeInfo.isPaused()) {
				infoValues.put("validation", "Trying to pause PV " + pvName + " that is already paused.");
				logger.error(infoValues.get("validation"));
				try(PrintWriter out = resp.getWriter()) {
					infoValues.put("validation", "PV " + pvName + " is already paused");
					out.println(JSONValue.toJSONString(infoValues));
				}
				return;
			}
			typeInfo.setPaused(true);
			typeInfo.setModificationTime(TimeUtils.now());
			configService.updateTypeInfoForPV(pvName, typeInfo);
			
			
			String ETLPauseURL = info.getEtlURL() + "/pauseArchivingPV" + "?pv=" + URLEncoder.encode(pvName, "UTF-8"); 
			logger.info("Pausing ETL using URL " + ETLPauseURL);
			GetUrlContent.getURLContentAsJSONObject(ETLPauseURL);
			
			String enginePauseURL = info.getEngineURL() + "/pauseArchivingPV" + "?pv=" + URLEncoder.encode(pvName, "UTF-8"); 
			logger.info("Pausing engine using URL " + enginePauseURL);

			JSONObject engineStatus = GetUrlContent.getURLContentAsJSONObject(enginePauseURL);
			if(engineStatus != null && !engineStatus.equals("")) {
				try(PrintWriter out = resp.getWriter()) {
					out.println(JSONValue.toJSONString(engineStatus));
				}
			} else {
				try(PrintWriter out = resp.getWriter()) {
					infoValues.put("validation", "Unable to pause PV " + pvName);
					out.println(JSONValue.toJSONString(infoValues));
				}
			}
		}
	}
	
	private void pauseMultiplePVs(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException, UnsupportedEncodingException {
		// String pvNameFromRequest = pvName;
		LinkedList<String> pvNames = PVsMatchingParameter.getMatchingPVs(req, configService, false);
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
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

				ApplianceInfo info = configService.getApplianceForPV(pvName);
				if(info == null) {
					infoValues.put("validation", "Trying to pause PV " + pvName + " that is not currently being archived.");
					logger.error(infoValues.get("validation"));
					infoValues.put("validation", "Unable to pause PV " + pvName);
					out.print(JSONValue.toJSONString(infoValues));
				} else {
					PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
					if(typeInfo.isPaused()) {
						infoValues.put("validation", "Trying to pause PV " + pvName + " that is already paused.");
						logger.error(infoValues.get("validation"));
						infoValues.put("validation", "PV " + pvName + " is already paused");
						out.print(JSONValue.toJSONString(infoValues));
					} else { 
						typeInfo.setPaused(true);
						typeInfo.setModificationTime(TimeUtils.now());
						configService.updateTypeInfoForPV(pvName, typeInfo);


						String ETLPauseURL = info.getEtlURL() + "/pauseArchivingPV" + "?pv=" + URLEncoder.encode(pvName, "UTF-8"); 
						logger.info("Pausing ETL using URL " + ETLPauseURL);
						GetUrlContent.getURLContentAsJSONObject(ETLPauseURL);

						String enginePauseURL = info.getEngineURL() + "/pauseArchivingPV" + "?pv=" + URLEncoder.encode(pvName, "UTF-8"); 
						logger.info("Pausing engine using URL " + enginePauseURL);

						JSONObject engineStatus = GetUrlContent.getURLContentAsJSONObject(enginePauseURL);
						if(engineStatus != null && !engineStatus.equals("")) {
							out.print(JSONValue.toJSONString(engineStatus));
						} else {
							infoValues.put("validation", "Unable to pause PV " + pvName);
							out.print(JSONValue.toJSONString(infoValues));
						}
					}
				}
			}
			out.println("]");
		}
	}
}
