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
import org.epics.archiverappliance.mgmt.archivepv.ArchivePVState;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Import configuration from an exported file for this appliance...
 * Send the exported file as the body of the POST.
 * @author mshankar
 *
 */
public class ImportConfigForAppliance implements BPLAction {
	private static Logger logger = LogManager.getLogger(ImportConfigForAppliance.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		ApplianceInfo myApplianceInfo = configService.getMyApplianceInfo();
		String myIdentity = myApplianceInfo.getIdentity();
		logger.info("Importing configuration for appliance " + myIdentity + " using POST");

		LinkedList<String> errorPVs = new LinkedList<String>();
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
				if(!myIdentity.equals(applianceIdentity)) {
					logger.error("Trying to import PV belonging to another appliance " + pvName);
					errorPVs.add(pvName);
					continue;
				}
				// We now have a valid PVTypeInfo for this PV.
				PVTypeInfo existingTypeInfo = configService.getTypeInfoForPV(pvName);
				if(existingTypeInfo != null) {
					logger.error("PV is already being archived " + pvName);
					errorPVs.add(pvName);
					continue;
				}
				
				configService.registerPVToAppliance(pvName, myApplianceInfo);
				configService.updateTypeInfoForPV(pvName, unmarshalledTypeInfo);
				ArchivePVState.startArchivingPV(pvName, configService, myApplianceInfo);
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}
		
		HashMap<String, Object> status = new HashMap<String, Object>();
		status.put("appliance", myIdentity);
		status.put("errorPVs", errorPVs);
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(status));
		}
	}
}
