package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
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
	private static Logger logger = LogManager.getLogger(PauseArchivingPV.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		
		if(req.getMethod().equals("POST")) { 
			pauseMultiplePVs(req, resp, configService);
			return;
		}
		
		
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
			JSONArray etlStatus = GetUrlContent.getURLContentAsJSONArray(ETLPauseURL);
			
			String enginePauseURL = info.getEngineURL() + "/pauseArchivingPV" + "?pv=" + URLEncoder.encode(pvName, "UTF-8"); 
			logger.info("Pausing engine using URL " + enginePauseURL);

			JSONArray engineStatus = GetUrlContent.getURLContentAsJSONArray(enginePauseURL);
			
			HashMap<String, String> retVal = new HashMap<String, String>();
			if(etlStatus != null) {
				for(Object statusObj : etlStatus) {
					@SuppressWarnings("unchecked")
					HashMap<String, String> status = (HashMap<String, String>) statusObj;
					for(String key : status.keySet()) {
						retVal.put("etl_" + key, status.get(key));
					}
				}
			}
			
			if(engineStatus != null && !engineStatus.equals("")) {
				for(Object statusObj : engineStatus) {
					@SuppressWarnings("unchecked")
					HashMap<String, String> status = (HashMap<String, String>) statusObj;
					for(String key : status.keySet()) {
						retVal.put("engine_" + key, status.get(key));
					}
				}
			}		
			
			if(retVal.containsKey("engine_status")
					&& retVal.containsKey("etl_status")
					&& retVal.get("engine_status").equals("ok")
					&& retVal.get("etl_status").equals("ok")
					) {
				retVal.put("status", "ok");
			} else { 
				infoValues.put("validation", "Either the engine or ETL did not return a valid status for " + pvName);
			}				
			
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(retVal));
			}			
		}
	}
	
	private void pauseMultiplePVs(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException, UnsupportedEncodingException {
		// String pvNameFromRequest = pvName;
		LinkedList<String> pvNames = BulkPauseResumeUtils.getPVNames(req, configService);
		boolean askingToPausePV = true; 
		List<HashMap<String, String>> response = BulkPauseResumeUtils.pauseResumeByAppliance(pvNames, configService, askingToPausePV);
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(response));
		}
	}
}
