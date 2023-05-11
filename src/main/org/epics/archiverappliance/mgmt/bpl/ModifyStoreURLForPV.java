package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

/**
 * Modifies the specified store for this PV using the new definition. The PV needs to be paused first.
 * 
 * @epics.BPLAction - Changes the store for this particular PV. Note this only changes the PVTypeInfo; it does not change any data/files so one could lose data using this call. 
 * @epics.BPLActionParam pv - The name of the pv
 * @epics.BPLActionParam storage - The name of the store to change ( for example, MTS )
 * @epics.BPLActionParam plugin_url - The new URL specification for this store; this is what you would have used in the policy file and is something that can be understood by <a href="../api/org/epics/archiverappliance/config/StoragePluginURLParser.html">StoragePluginURLParser</a>.
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class ModifyStoreURLForPV implements BPLAction {
	private static Logger logger = LogManager.getLogger(ModifyStoreURLForPV.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Please specify the PV name");
			return;
		}
		
		String storage = req.getParameter("storage");
		if(storage == null || storage.equals("")) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Please specify the name of the store to alter");
			return;
		}

		String plugin_url = req.getParameter("plugin_url");
		if(plugin_url == null || plugin_url.equals("")) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Please specify the new storage specification as the plugin_url argument");
			return;
		}
		
		PVTypeInfo typeInfo = PVNames.determineAppropriatePVTypeInfo(pvName, configService);
		if(typeInfo == null) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Cannot find typeinfo for " + pvName);
			return;
		}

		if(!typeInfo.isPaused()) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND, "PV is not paused " + pvName);
			return;
		}
		
		HashMap<String, String> stores = new HashMap<String, String>();
		for(String store : typeInfo.getDataStores()) {
			StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
			stores.put(plugin.getName(), store);
		}
		
		if(!stores.containsKey(storage)) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Cannot find store with name " + storage);
			return;
		}

		try {
			StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(plugin_url, configService);
			if(plugin == null) {
				resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Cannot parse URL " + plugin_url);
				return;
			}
		}catch(Exception ex) {			
			logger.error("Cannot parse URL " + plugin_url, ex);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Cannot parse URL " + plugin_url);
			return;
		}
		
		logger.info("Replacing plugin URL for " + storage + " for PV " + pvName + " with store definition " + plugin_url);
		
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			LinkedList<String> newStores = new LinkedList<String>(); 
			for(String store : typeInfo.getDataStores()) {
				StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
				if(plugin.getName().equals(storage)) {
					newStores.add(plugin_url);
				} else {
					newStores.add(store);
				}
			}
			typeInfo.setDataStores(newStores.toArray(new String[0]));
			typeInfo.setModificationTime(TimeUtils.now());
			configService.updateTypeInfoForPV(pvName, typeInfo);
		} catch(Exception ex) {
			logger.error("Exception updating typeinfo for pv " + pvName, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Exception updating typeinfo for pv " + pvName);
		}
	}
}
