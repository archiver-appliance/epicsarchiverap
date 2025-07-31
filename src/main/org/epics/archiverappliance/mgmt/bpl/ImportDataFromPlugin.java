package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.HashMap;

/**
 * Import data using another plugin given the plugin definition URL for a PV and store in the specified store.
 * @author mshankar
 *
 */
public class ImportDataFromPlugin implements BPLAction {
	private static Logger logger = LogManager.getLogger(ImportDataFromPlugin.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			logger.warn("Cannot find typeinfo for " + pvName);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		String startTimeStr = req.getParameter("from"); 
		String endTimeStr = req.getParameter("to");
		if(startTimeStr == null || startTimeStr.equals("") || endTimeStr == null || endTimeStr.equals("")) {
			logger.warn("No from and to (start and end times) specified " + pvName);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}

        Instant startTime = TimeUtils.convertFromISO8601String(startTimeStr);
        Instant endTime = TimeUtils.convertFromISO8601String(endTimeStr);
		
		String srcStoragePluginURL = req.getParameter("src");
		if(srcStoragePluginURL == null || srcStoragePluginURL.equals("")) {
			logger.warn("Need to specify src plugin using src for " + pvName);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		String destStoragePluginName = req.getParameter("dest");
		if(destStoragePluginName == null || destStoragePluginName.equals("")) {
			logger.warn("Need to specify dest plugin name using dest for " + pvName);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		StoragePlugin srcPlugin = StoragePluginURLParser.parseStoragePlugin(srcStoragePluginURL, configService);
		
		StoragePlugin destPlugin = null;
		for(String dataStore : typeInfo.getDataStores()) {
			StoragePlugin dataStorePlugin = StoragePluginURLParser.parseStoragePlugin(dataStore, configService);
			if(dataStorePlugin.getName().equals(destStoragePluginName)) {
				destPlugin = dataStorePlugin;
				break;
			}
		}
		
		if(destPlugin == null) {
			logger.warn("Unable to find storage plugin with name " + destStoragePluginName + " for pv " + pvName);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		try(BasicContext context = new BasicContext();
				EventStream strm = new CurrentThreadWorkerEventStream(pvName, srcPlugin.getDataForPV(context, pvName, startTime, endTime, new DefaultRawPostProcessor()))) {
			destPlugin.appendData(context, pvName, strm);
		}
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			HashMap<String, String> ret = new HashMap<String, String>();
			ret.put("status", "ok");
			ret.put("pvName", pvName);
			out.println(JSONValue.toJSONString(ret));
		} catch(Exception ex) {
			logger.error("Exception marshalling typeinfo for pv " + pvName, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}
}
