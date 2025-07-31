package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.HashMap;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.conversion.ThruNumberAndStringConversion;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * 
 * @epics.BPLAction - Change the type of the PV to the specified type. The PV needs to be paused first. For best results, consolidate all the data to one store. Note, this is actually changing the data so you should make a backup just in case. There is every chance that this may leave the data for this PV in an inconsistent state. It is also possible that the plugin may do nothing in which case you may have to rename the existing PV to a new PV; delete this PV and issue a fresh archival request. 
 * @epics.BPLActionParam pv - The name of the pv.
 * @epics.BPLActionParam newtype - The new type - one of the ArchDBRTypes. For example, DBR_SCALAR_DOUBLE.
 * @epics.BPLActionEnd
 * 
 * Note, this is actually a dangerous function in that it can leave the PV in all kinds of inconsistent states.
 * Use with caution. 
 * @author mshankar
 *
 */
public class ChangeTypeForPV implements BPLAction {
	private static Logger logger = LogManager.getLogger(ChangeTypeForPV.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String newTypeStr = req.getParameter("newtype");
		if(newTypeStr==null || newTypeStr.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		ArchDBRTypes newDBRType = ArchDBRTypes.valueOf(newTypeStr);
		if(newDBRType == null) { 
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		// String pvNameFromRequest = pvName;
		String realName = configService.getRealNameForAlias(pvName);
		if(realName != null) pvName = realName;

		ApplianceInfo info = configService.getApplianceForPV(pvName);
		if(info == null) {
			logger.debug("Unable to find appliance for PV " + pvName);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		if(!info.getIdentity().equals(configService.getMyApplianceInfo().getIdentity())) { 
			// We should proxy this call to the actual appliance hosting the PV.
			String redirectURL = info.getMgmtURL() + "/changeTypeForPV?pv=" + URLEncoder.encode(pvName, "UTF-8") + "&newtype=" + URLEncoder.encode(newTypeStr, "UTF-8"); 
			logger.debug("Routing request to the appliance hosting the PV " + pvName + " using URL " + redirectURL);
			JSONObject status = GetUrlContent.getURLContentAsJSONObject(redirectURL);
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(status));
			}
			return;
			
		}

		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			logger.debug("Unable to find typeinfo for PV...");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		if(!typeInfo.isPaused()) { 
			String msg = "Need to pause PV before changing type for " + pvName;
			logger.error(msg);
			infoValues.put("validation", msg);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		}

		for(String store : typeInfo.getDataStores()) {
			StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
			try(BasicContext context = new BasicContext()) {
				logger.info("Converting data in plugin " + plugin.getName() + " for PV " + pvName + " from " + typeInfo.getDBRType().toString() + " to " + newDBRType.toString());
				plugin.convert(context, pvName, new ThruNumberAndStringConversion(newDBRType));
				logger.info("Done converting data in plugin " + plugin.getName() + " for PV " + pvName + " from " + typeInfo.getDBRType().toString() + " to " + newDBRType.toString());
			}
		}
		
		// Update the type info in the database.
		typeInfo.setDBRType(newDBRType);
		configService.updateTypeInfoForPV(pvName, typeInfo);
		infoValues.put("status", "ok");
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(infoValues));
		}
	}
}
