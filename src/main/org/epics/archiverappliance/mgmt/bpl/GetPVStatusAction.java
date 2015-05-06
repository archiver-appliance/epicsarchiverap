/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Get the status of a PV.
 * 
 * @epics.BPLAction - Get the status of a PV.
 * @epics.BPLActionParam pv - The name(s) of the pv for which status is to be determined. If a pv is not being archived, you should get back a simple JSON object with a status string of "Not being archived." You can also pass in GLOB wildcards here and multiple PVs as a comma separated list. If you have more PVs that can fit in a GET, send the pv's as a CSV <code>pv=pv1,pv2,pv3</code> as the body of a POST.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class GetPVStatusAction implements BPLAction {
	private static final Logger logger = Logger.getLogger(GetPVStatusAction.class);

	@SuppressWarnings("unchecked")
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		logger.info("Getting the status of pv(s) " + req.getParameter("pv"));
		LinkedList<String> pvNames = PVsMatchingParameter.getMatchingPVs(req, configService, true);
		
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			out.println("[");
			boolean isFirst = true;
			for(String pvName : pvNames) {
				if(isFirst) { isFirst = false; } else { out.println(","); }
				pvName = PVNames.normalizePVName(pvName);
				String pvNameFromRequest = pvName;
				String realName = configService.getRealNameForAlias(pvName);
				if(realName != null) pvName = realName;
				logger.debug("Checking for status for " + pvName);
				
				ApplianceInfo info = configService.getApplianceForPV(pvName);
				PVTypeInfo typeInfoForPV = null;
				if(info == null) {
					String fieldName = PVNames.getFieldName(pvName);
					if(fieldName != null && !fieldName.equals("")) {
						String pvNameOnly = PVNames.stripFieldNameFromPVName(pvName);
						logger.debug("Looking for appliance for " + pvNameOnly + " to determine field " + fieldName);
						info = configService.getApplianceForPV(pvNameOnly);
						if(info != null) {
							typeInfoForPV = configService.getTypeInfoForPV(pvNameOnly);
							if(typeInfoForPV != null) {
								if(typeInfoForPV.checkIfFieldAlreadySepcified(fieldName)) {
									logger.debug("Standard field, returning status of pv instead " + pvName);
									pvName = pvNameOnly;
								} else { 
									logger.debug("Field " + fieldName + " is not a standard field");
									info = null;
								}
							}
						}
					}
				} else { 
					typeInfoForPV = configService.getTypeInfoForPV(pvName);
				}
				
				if(info == null) {
					if(configService.doesPVHaveArchiveRequestInWorkflow(pvName)) {
						out.println("{ \"pvName\": \"" + pvNameFromRequest + "\", \"status\": \"Initial sampling\" }");
					} else {
						out.println("{ \"pvName\": \"" + pvNameFromRequest + "\", \"status\": \"Not being archived\" }");
					}
				} else {
					String pvStatusURLStr = info.getEngineURL() + "/status?pv=" + URLEncoder.encode(pvName, "UTF-8");
					JSONObject pvStatus = GetUrlContent.getURLContentAsJSONObject(pvStatusURLStr, false);
					if(pvStatus != null && !pvStatus.isEmpty()) {
						pvStatus.put("appliance", info.getIdentity());
						pvStatus.put("pvName", pvNameFromRequest);
						pvStatus.put("pvNameOnly", pvNameFromRequest);
						out.println(pvStatus);
					} else if(typeInfoForPV != null) { 
						HashMap<String, String> tempStatus = new HashMap<String, String>();
						tempStatus.put("appliance", info.getIdentity());
						tempStatus.put("pvName", pvNameFromRequest);
						tempStatus.put("pvNameOnly", pvName);
						tempStatus.put("status", "Paused");
						JSONValue.writeJSONString(tempStatus, out);
					} 
					else {
						out.println("{ \"pvName\": \"" + pvNameFromRequest + "\", \"status\": \"Appliance assigned\" }");
					}
				}
			}
			out.println("]");
		}
	}
}
