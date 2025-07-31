package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.HashMap;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @epics.BPLAction - Add an alias for the specified PV. 
 * @epics.BPLActionParam pv - The real name of the pv.
 * @epics.BPLActionParam aliasname - The alias name of the pv. Note, we cannot have a PVTypeInfo mapped to the alias name.
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class AddAliasAction implements BPLAction {
	private static Logger logger = LogManager.getLogger(AddAliasAction.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		String aliasName = req.getParameter("aliasname");
		if(aliasName == null || aliasName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		logger.info("Adding alias " + aliasName + " for pv " + pvName);
		
		boolean useThisAppliance = false;
		if(req.getParameter("useThisAppliance") != null && Boolean.parseBoolean(req.getParameter("useThisAppliance")) == true) {
			logger.info("Skipping adding alias in the same appliance as the pv typeinfo. This is mostly for unit tests.");
			useThisAppliance = true;
		}
		
		// Make sure we do not have a pvTypeInfo in the alias name
		PVTypeInfo aliasTypeInfo = configService.getTypeInfoForPV(aliasName);
		if(aliasTypeInfo != null) { 
			logger.error("We seem to have a PVTypeInfo for alias " + aliasName + ". We do not support adding aliases that have typeinfo's associated with them.");
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}
		
		// Make sure we have a pvTypeInfo in the real name.
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			logger.debug("Type info is null. Check to see if we have the PV in the archivePV workflow");
			if(configService.getMgmtRuntimeState().isPVInWorkflow(pvName)) {
				logger.debug("PV " + pvName + " is in workflow. Updating the UserSpecifiedSamplingParams and adding alias " + aliasName);
				UserSpecifiedSamplingParams samplingParams = configService.getUserSpecifiedSamplingParams(pvName);
				samplingParams.addAlias(aliasName);
				configService.updateArchiveRequest(pvName, samplingParams);
				logger.debug("Done updating PV " + pvName + " UserSpecifiedSamplingParams and adding alias " + aliasName);				
			} else { 
				// This is expected sometimes and we do not want to unnecessarily log this as an error.
				logger.debug("We do not seem to have a PVTypeInfo for pv " + pvName + " and it is not in the archive PV workflow");
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				return;
			}
		}
		
		ApplianceInfo infoForPV = configService.getApplianceForPV(pvName);
		if(useThisAppliance || (infoForPV != null && infoForPV.equals(configService.getMyApplianceInfo()))) {
			configService.addAlias(aliasName, pvName);
			HashMap<String, Object> infoValues = new HashMap<String, Object>();
			infoValues.put("status", "ok");
			infoValues.put("desc", "Added an alias " + aliasName + " for PV " + pvName);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		} else { 
			if(infoForPV == null) { 
				logger.error("We seem to have a PVTypeInfo for pv " + pvName + " but it is not yet assigned?");
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				return;				
			}
			// Route to the appliance that hosts the PVTypeInfo
			String redirectURL = infoForPV.getMgmtURL()
					+ "/addAlias?pv=" 
					+ URLEncoder.encode(pvName, "UTF-8") 
					+ "&aliasname=" 
					+ URLEncoder.encode(aliasName, "UTF-8"); 

			logger.info("Redirecting addAlias request for PV to " + infoForPV.getIdentity() + " using URL " + redirectURL);
			JSONObject status = GetUrlContent.getURLContentAsJSONObject(redirectURL);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(status));
			}
			return;
		}		
	}
}
