package org.epics.archiverappliance.mgmt.bpl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.net.URLEncoder;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * Updates the type info for a PV. The typeinfo should be sent in the POST body as JSON.
 * 
 * @epics.BPLAction - Updates the typeinfo for the specified PV. Note this merely updates the typeInfo. It does not have any logic to react to changes in the typeinfo. That is, don't assume that the PV is automatically paused just because you changed the isPaused to true. This is meant to be used in conjuction with other BPL to implement site-specific BPL in external code (for example, python). This can also be used to add PVTypeInfo's into the system; support for this is experimental. The new PVTypeInfo's are automatically paused before adding into the system. Logically, you have to specify at least one of override or createnew.
 * @epics.BPLActionParam pv - The name of the pv.
 * @epics.BPLActionParam override - If the PVTypeInfo for this PV already exists, do you want to update it or return an error? By default, this is false.
 * @epics.BPLActionParam createnew - If the PVTypeInfo for this PV does not exist, do you want to create a new one or return an error? By default, this is false.
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class PutPVTypeInfo implements BPLAction {
	private static Logger logger = LogManager.getLogger(PutPVTypeInfo.class.getName());
	
	class TypeInfoAndJsonObject { 
		PVTypeInfo typeInfo;
		JSONObject jsonObject;
		public TypeInfoAndJsonObject(PVTypeInfo typeInfo, JSONObject jsonObject) {
			this.typeInfo = typeInfo;
			this.jsonObject = jsonObject;
		}
	}

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		logger.debug("Updating typeinfo for PV " + pvName);
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		String overridestr = req.getParameter("override");
		boolean override = false;
		try { 
			override = Boolean.parseBoolean(overridestr);
		} catch(Exception ex) { 
			logger.error("Exception processing override flag for pv " + pvName, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}
		
		String createnewstr = req.getParameter("createnew");
		boolean createnew = false;
		try { 
			createnew = Boolean.parseBoolean(createnewstr);
		} catch(Exception ex) { 
			logger.error("Exception processing createnew flag for pv " + pvName, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}
		
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		TypeInfoAndJsonObject updatedTypeInfo = this.readInNewTypeInfo(req, resp, (typeInfo == null ? true : false)); // If we are creating a new typeInfo, we should automatically pause it.
		
		// Data consistency checks. 
		// Make sure the pv name in the PVTypeInfo matches the pvName specified in the call.
		String pvNameFromTypeInfo = updatedTypeInfo.typeInfo.getPvName();
		if(!pvName.equals(pvNameFromTypeInfo)) { 
			String msg = "The PV name in the call " + pvName + " does not match the PV name in the typeinfo " + pvNameFromTypeInfo;
			logger.error(msg);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
			return;
		}
		
		String applianceIdentity = null;
		boolean newPVTypeInfo = false;
		if(typeInfo == null) {
			if(!createnew) { 
				String msg = "We do not have a typeinfo for PV " + pvName + " but the createnew flag has not been set";
				logger.info(msg);
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
				return;
			}
			applianceIdentity = updatedTypeInfo.typeInfo.getApplianceIdentity();
			newPVTypeInfo = true;
		} else { 
			if(!override) { 
				String msg = "We already have a typeinfo for PV " + pvName + " but the override flag has not been set";
				logger.info(msg);
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
				return;
			}
			
			applianceIdentity = typeInfo.getApplianceIdentity();

			// Another data consistency check
			// Make sure the appliances are the same
			String applianceIdentityFromUpdatedTypeInfo = updatedTypeInfo.typeInfo.getApplianceIdentity();
			if(!applianceIdentity.equals(applianceIdentityFromUpdatedTypeInfo)) { 
				String msg = "The appliance identity for an existing PVTypeInfo " + applianceIdentity + " does not match the appliance identity in the typeinfo " + applianceIdentityFromUpdatedTypeInfo + " for pv " + pvName;
				logger.error(msg);
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
				return;
			}
		}
		
		if(applianceIdentity == null) { 
			String msg = "Cannot determine appliance when updating typeinfo for PV " + pvName + " with " + (typeInfo== null ? "new" : "existing") + " typeinfo";
			logger.info(msg);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
			return;
		}
		
		// Route this to the mgmt server
		if(!applianceIdentity.equals(configService.getMyApplianceInfo().getIdentity())) {
			String mgmtURL = configService.getAppliance(applianceIdentity).getMgmtURL();
			String updateTypeInfoURL = mgmtURL + "/putPVTypeInfo?pv=" 
					+ URLEncoder.encode(pvName, "UTF-8") 
					+ "&override=" + Boolean.toString(override)
					+ "&createnew=" + Boolean.toString(createnew);
			GetUrlContent.postDataAndGetContentAsJSONObject(updateTypeInfoURL, updatedTypeInfo.jsonObject);
		} else { 
			logger.info("Updating typeInfo for PV " + pvName);
			if(newPVTypeInfo) {
				try { 
					configService.updateTypeInfoForPV(pvName, updatedTypeInfo.typeInfo);
					configService.registerPVToAppliance(pvName, configService.getAppliance(applianceIdentity));
				} catch(AlreadyRegisteredException ex) { 
					String msg = "Exception registering new PV " + pvName + " to appliance " + applianceIdentity;
					logger.info(msg);
					resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
					return;
				}
			} else { 
				configService.updateTypeInfoForPV(pvName, updatedTypeInfo.typeInfo);			
			}
		}

		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try { 
			PVTypeInfo typeInfoAfterConfigServiceUpdate = configService.getTypeInfoForPV(pvName);
			JSONEncoder<PVTypeInfo> jsonEncoder = JSONEncoder.getEncoder(PVTypeInfo.class);
			try (PrintWriter out = resp.getWriter()) {
				out.println(jsonEncoder.encode(typeInfoAfterConfigServiceUpdate));
			}
		} catch(Exception ex) {
			logger.error("Exception sending typeinfo after updating", ex);
		}
	}
	
	private TypeInfoAndJsonObject readInNewTypeInfo(HttpServletRequest req, HttpServletResponse resp, boolean pauseGeneratedTypeInfo) throws IOException { 
		try (LineNumberReader lineReader = new LineNumberReader(new InputStreamReader(new BufferedInputStream(req.getInputStream())))) {
			JSONParser parser=new JSONParser();
			PVTypeInfo updatedInfo = new PVTypeInfo();
			JSONObject updatedTypeInfoJSON = (JSONObject) parser.parse(lineReader);
			JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
			decoder.decode(updatedTypeInfoJSON, updatedInfo);
			if(pauseGeneratedTypeInfo) updatedInfo.setPaused(true);
			return new TypeInfoAndJsonObject(updatedInfo, updatedTypeInfoJSON);
		} catch(Exception ex) { 
			if(ex instanceof IOException) { 
				throw ((IOException)ex);
			} else { 
				throw new IOException(ex);
			}
		}
	}
	
}
