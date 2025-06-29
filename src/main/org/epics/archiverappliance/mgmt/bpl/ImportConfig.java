package org.epics.archiverappliance.mgmt.bpl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Import configuration from an exported file
 * Send the exported file as the body of the POST.
 * @author mshankar
 *
 */
public class ImportConfig implements BPLAction {
	private static Logger logger = LogManager.getLogger(ImportConfig.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Importing configuration using POST");

		HashMap<String, LinkedList<JSONObject>> pvsForAppliances = new HashMap<String, LinkedList<JSONObject>>();
		try(InputStream is = new BufferedInputStream(req.getInputStream())) {
			JSONDecoder<PVTypeInfo> typeInfoDecoder = JSONDecoder.getDecoder(PVTypeInfo.class);
			JSONArray unmarshalledConfiguration = (JSONArray) JSONValue.parse(new InputStreamReader(is));
			for(Object configObj : unmarshalledConfiguration) {
				JSONObject configJSON = (JSONObject) configObj;
				PVTypeInfo unmarshalledTypeInfo = new PVTypeInfo();
				typeInfoDecoder.decode(configJSON, unmarshalledTypeInfo);
				String pvName = unmarshalledTypeInfo.getPvName();
				logger.debug("Importing configuration for " + pvName);
				String applianceIdentity = unmarshalledTypeInfo.getApplianceIdentity();
				ApplianceInfo applianceInfo = configService.getAppliance(applianceIdentity);
				if(applianceInfo == null) {
					logger.error("Unable to determine appliance information for appliance " + applianceIdentity + " when importing configuration for " + pvName);
					continue;
				}
				LinkedList<JSONObject> pvsForAppliance = pvsForAppliances.get(applianceIdentity);
				if(pvsForAppliance == null) {
					pvsForAppliance = new LinkedList<JSONObject>();
					pvsForAppliances.put(applianceIdentity, pvsForAppliance);
				}
				pvsForAppliance.add(configJSON);
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}
		
		
		LinkedList<JSONObject> responses = new LinkedList<JSONObject>();
		for(String applianceIdentity : pvsForAppliances.keySet()) {
			LinkedList<JSONObject> pvsForAppliance = pvsForAppliances.get(applianceIdentity);
			if(pvsForAppliance.size() > 1) {
				ApplianceInfo applianceInfo = configService.getAppliance(applianceIdentity);
				String importConfigURL = applianceInfo.getMgmtURL() + "/importConfigForAppliance";
				JSONObject response = GetUrlContent.postDataAndGetContentAsJSONObject(importConfigURL, GetUrlContent.from(pvsForAppliance));
				responses.add(response);
			} else {
				logger.warn("No pvs when importing configuration for appliance " + applianceIdentity);
			}
		}
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(responses));
		}
	}
}
