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
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * 
 * @epics.BPLAction - Rename this pv to a new name. The PV needs to be paused first.
 * @epics.BPLActionParam pv - The current name of the PV.
 * @epics.BPLActionParam newname - The new name of the PV.
 * @epics.BPLActionEnd
 * 
 * Note - Renames are not transactional; to accommodate a safer process for renaming PV's, this action functions more like a "copyPV" action.
 * When you rename PV <code>old</code> to <code>new</code>, the PVTypeInfo and data for <code>old</code> are copied into <code>new</code>.
 * Both <code>old</code> and <code>new</code> are still in a paused state.
 * After making sure that the data transfer has succeeded, you can resume <code>new</code> and delete <code>old</code>.
 * 
 * 
 * @author mshankar
 *
 */
public class RenamePVAction implements BPLAction {
	private static Logger logger = LogManager.getLogger(RenamePVAction.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		if(!configService.hasClusterFinishedInitialization()) {
			// If you have defined spare appliances in the appliances.xml that will never come up; you should remove them
			// This seems to be one of the few ways we can prevent split brain clusters from messing up the pv <-> appliance mapping.
			throw new IOException("Waiting for all the appliances listed in appliances.xml to finish loading up their PVs into the cluster");
		}

		String currentPVName = req.getParameter("pv");
		if(currentPVName == null || currentPVName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String newPVName=req.getParameter("newname");
		if(newPVName==null || newPVName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		// String pvNameFromRequest = pvName;
		String realName = configService.getRealNameForAlias(currentPVName);
		if(realName != null) currentPVName = realName;

		
		ApplianceInfo info = configService.getApplianceForPV(currentPVName);
		if(info == null) {
			logger.debug("Unable to find appliance for PV " + currentPVName);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(currentPVName);
		if(typeInfo == null) {
			logger.debug("Unable to find typeinfo for PV...");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		
		if(!typeInfo.isPaused()) { 
			String msg = "Need to pause PV before renaming " + currentPVName + " to " + newPVName;
			logger.error(msg);
			infoValues.put("validation", msg);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		}


		if(!info.getIdentity().equals(configService.getMyApplianceInfo().getIdentity())) { 
			String redirectURL = info.getMgmtURL() + "/renamePV?pv=" + URLEncoder.encode(currentPVName, "UTF-8") + 
					"&newname=" + URLEncoder.encode(newPVName, "UTF-8"); 
			logger.info("Redirecting rename request for PV to " + info.getIdentity() + " using URL " + redirectURL);
			JSONObject status = GetUrlContent.getURLContentAsJSONObject(redirectURL);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(status));
			}
			return;
		}

		
		if(configService.getApplianceForPV(newPVName) != null) {
			String msg = "While renaming PV  " + currentPVName + " to " + newPVName + ", the new name  already exists ";
			logger.error(msg);
			infoValues.put("validation", msg);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		}

		if(configService.getTypeInfoForPV(newPVName) != null) {
			String msg = "While renaming PV  " + currentPVName + " to " + newPVName + ", the typeinfo for the new name  already exists ";
			logger.error(msg);
			infoValues.put("validation", msg);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		}
		
		try(PrintWriter out = resp.getWriter()) {
			PVTypeInfo newPVTypeInfo = new PVTypeInfo(newPVName, typeInfo);
			newPVTypeInfo.setCreationTime(typeInfo.getCreationTime());
			newPVTypeInfo.setModificationTime(TimeUtils.now());
			newPVTypeInfo.setApplianceIdentity(info.getIdentity());
			configService.updateTypeInfoForPV(newPVName, newPVTypeInfo);
			configService.registerPVToAppliance(newPVName, info);
			logger.debug("Done registering typeinfo when renaming PV  " + currentPVName + " to " + newPVName);

			for(String store : newPVTypeInfo.getDataStores()) {
				StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
				try(BasicContext context = new BasicContext()) { 
					plugin.renamePV(context, currentPVName, newPVName);
					logger.debug("Done renaming data when renaming PV  " + currentPVName + " to " + newPVName + " for plugin " + plugin.getName());
				}
			}
			infoValues.put("status", "ok");
			infoValues.put("desc", "Successfully renamed PV " + currentPVName + " to " + newPVName);
			out.println(JSONValue.toJSONString(infoValues));
		} catch(Exception ex) { 
			String msg = "Exception while renaming PV  " + currentPVName + " to " + newPVName + " " + ex.getMessage();
			logger.error(msg, ex);
			infoValues.put("validation", msg);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		}
	}
}
