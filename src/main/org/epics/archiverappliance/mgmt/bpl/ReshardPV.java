package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * <div>Reshards the PV to another appliance. This is a complex BPL whose implementation uses functionality provided by existing BPL.</div>
 * 
 * <div>
 * All of these steps are driven by the destination appliance. The sequence of steps are
 * <ol>
 * <li>Make sure the PV is paused.</li>
 * <li>Consolidate the data for the PV to the specified store in the src appliance using the {@link org.epics.archiverappliance.mgmt.bpl.ConsolidatePBFilesForOnePV consolidate BPL}.</li>
 * <li>Clone the source PV's {@link org.epics.archiverappliance.config.PVTypeInfo typeinfo} and register under a new temporary name.</li>
 * <li>Assign the destination PV to the destination appliance (and as all steps are happening on the destination appliance, this is myself).</li>
 * <li>Get all the events as an {@link org.epics.archiverappliance.EventStream eventstream} for the source PV as getting data for the source PV between the source PV's typeInfo <code>creationTime</code> and sometime into the future.</li>
 * <li>Append into store identified by the <code>storage</code> parameter on the destination appliance (again, myself)</li>
 * <li>Delete the source PV along with it's data by calling the {@link org.epics.archiverappliance.mgmt.bpl.DeletePV delete BPL}</li>
 * <li>Rename the dest PV by calling the {@link org.epics.archiverappliance.mgmt.bpl.RenamePVAction rename BPL}</li>
 * </ol>
 * </div>
 * 
 * @epics.BPLAction - This BPL reassigns the PV to another appliance. This is a complex BPL that also moves data over to the other appliance. For more details on the sequence of steps, see the Javadoc for the BPL.
 * @epics.BPLActionParam pv - The name of the pv. The PV needs to be paused first and will remain in a paused state after the resharding is complete.
 * @epics.BPLActionParam appliance - The new appliance to assign the PV to. This is the same string as the <code>identity</code> element in the <code>appliances.xml</code> that identifies this appliance.
 * @epics.BPLActionParam storage - The name of the store until which we'll consolidate data before resharding. The data is moved over to the store with the same name on the new appliance. This is typically a string like LTS.
 * @epics.BPLActionEnd
 *
 * 
 * @author mshankar
 */
public class ReshardPV implements BPLAction {
	private static Logger logger = Logger.getLogger(ReshardPV.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String srcPVName = req.getParameter("pv");
		if(srcPVName == null || srcPVName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String storageName=req.getParameter("storage");
		if(storageName==null || storageName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String destApplianceIdentity = req.getParameter("appliance");
		if(destApplianceIdentity==null || destApplianceIdentity.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		// String pvNameFromRequest = pvName;
		String realName = configService.getRealNameForAlias(srcPVName);
		if(realName != null) srcPVName = realName;

		ApplianceInfo srcApplianceInfo = configService.getApplianceForPV(srcPVName);
		if(srcApplianceInfo == null) {
			logger.error("Unable to find appliance for PV " + srcPVName);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		ApplianceInfo destApplianceInfo = configService.getAppliance(destApplianceIdentity);
		if(destApplianceInfo == null) {
			logger.error("Unable to find appliance with identity " + destApplianceIdentity);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		
		if (srcApplianceInfo.getIdentity().equals(destApplianceInfo.getIdentity())) { 
			logger.error("Attempting to reshard onto same appliance - " + srcApplianceInfo.getIdentity() + "/" + destApplianceInfo.getIdentity());
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		
		PVTypeInfo srcTypeInfo = configService.getTypeInfoForPV(srcPVName);
		if(srcTypeInfo == null) {
			logger.debug("Unable to find typeinfo for PV...");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		if(!destApplianceIdentity.equals(configService.getMyApplianceInfo().getIdentity())) { 
			String redirectURL = destApplianceInfo.getMgmtURL()
					+ "/reshardPV?pv=" 
					+ URLEncoder.encode(srcPVName, "UTF-8") 
					+ "&storage=" 
					+ URLEncoder.encode(storageName, "UTF-8")
					+ "&appliance=" 
					+ URLEncoder.encode(destApplianceIdentity, "UTF-8"); 

			logger.info("Redirecting resharding request for PV to " + destApplianceIdentity + " using URL " + redirectURL);
			JSONObject status = GetUrlContent.getURLContentAsJSONObject(redirectURL);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(status));
			}
			return;
		}



		boolean foundSrcPlugin = false;
		for(String store : srcTypeInfo.getDataStores()) {
			StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
			if(plugin.getName().equals(storageName)) {
				logger.debug("Found the storage plugin identifier by " + storageName);
				foundSrcPlugin = true;
				break;
			}
		}
		
		if(!foundSrcPlugin) {
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("validation", "Cannot find storage with name " + storageName + " for pv " + srcPVName);
				out.println(JSONValue.toJSONString(infoValues));
				return;
			}
		}

		if(!srcTypeInfo.isPaused()) {
			infoValues.put("validation", "Cannot consolidate unless PV " + srcPVName + " is paused.");
			logger.error(infoValues.get("validation"));
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		}
		

		
		String ETLConsolidateURL = srcApplianceInfo.getEtlURL() + "/consolidateDataForPV" 
				+ "?pv=" + URLEncoder.encode(srcPVName, "UTF-8")
				+ "&storage=" + URLEncoder.encode(storageName, "UTF-8"); 
		logger.info("Consolidating data for PV using URL " + ETLConsolidateURL);

		JSONObject consolidateStatus = GetUrlContent.getURLContentAsJSONObject(ETLConsolidateURL);
		if(consolidateStatus == null || consolidateStatus.equals("") || !consolidateStatus.containsKey("status") || !((String)consolidateStatus.get("status")).equalsIgnoreCase("ok")) {
			try(PrintWriter out = resp.getWriter()) {
				String errorMsg = "Unable to consolidate data for PV " + srcPVName;
				logger.error(errorMsg);
				infoValues.put("validation", errorMsg);
				out.println(JSONValue.toJSONString(infoValues));
				return;
			}
		}

		logger.info("Done consolidating data for PV using URL " + ETLConsolidateURL + ". Now cloning the type info");
		
		
		
		Random rand = new Random();
		String destPVName = srcPVName + "_reshard_" + rand.nextLong();
		int nameTries = 0;
		while(configService.getApplianceForPV(destPVName) != null) {
			logger.error(destPVName + " seems to exist. This is highly improbable. Trying again.");
			if(nameTries++ > 100) { 
				try(PrintWriter out = resp.getWriter()) {
					String errorMsg = "Unable to get a temporary name even after 100 tries. Giving up on resharding " + srcPVName;
					logger.error(errorMsg);
					infoValues.put("validation", errorMsg);
					out.println(JSONValue.toJSONString(infoValues));
					return;
				}
			}
			destPVName = srcPVName + "_reshard_" + rand.nextLong();
		}

		PVTypeInfo destTypeInfo = new PVTypeInfo(destPVName, srcTypeInfo);
		destTypeInfo.setCreationTime(srcTypeInfo.getCreationTime());
		destTypeInfo.setModificationTime(TimeUtils.now());
		try { 
			configService.registerPVToAppliance(destPVName, configService.getMyApplianceInfo());
			destTypeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
			configService.updateTypeInfoForPV(destPVName, destTypeInfo);
		} catch(AlreadyRegisteredException ex) { 
			try(PrintWriter out = resp.getWriter()) {
				String errorMsg = "Temporary PV name is already registered " + destPVName + ". Giving up on resharding " + srcPVName;
				logger.error(errorMsg);
				infoValues.put("validation", errorMsg);
				out.println(JSONValue.toJSONString(infoValues));
				return;
			}
		}
		
		logger.info("Registered temp PV " + destPVName + " to appliance " + configService.getMyApplianceInfo().getIdentity());
		
		StoragePlugin destPlugin = null;
		for(String store : destTypeInfo.getDataStores()) {
			StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
			if(plugin.getName().equals(storageName)) {
				logger.debug("Found the storage plugin identifier by " + storageName);
				destPlugin = plugin;
				break;
			}
		}
		
		if(destPlugin == null) {
			try(PrintWriter out = resp.getWriter()) {
				String errorMsg = "Cannot find storage with name " + storageName + " for pv " + destPVName;
				logger.error(errorMsg);
				infoValues.put("validation", errorMsg);
				out.println(JSONValue.toJSONString(infoValues));
				cleanupTemporaryPV(configService, destPVName);
				return;
			}
		}

		long distantFutureEpochSeconds = System.currentTimeMillis()/1000 + 2*365*24*60*60;
		Timestamp distantFuture = TimeUtils.convertFromEpochSeconds(distantFutureEpochSeconds, 0);
		long beforeEventCount = getEventCount(configService, srcApplianceInfo, srcPVName, srcTypeInfo.getCreationTime(), distantFuture);
		logger.info("Before data transfer, we have " + beforeEventCount + " events");
		
		String dataRetrievalURL = srcApplianceInfo.getRetrievalURL().replace("/bpl", "") + "/data/getData.raw"; 
		logger.info("Getting data for source PV using URL " + dataRetrievalURL);
		StoragePlugin srcStoragePlugin = StoragePluginURLParser.parseStoragePlugin("pbraw://localhost?rawURL=" + dataRetrievalURL, configService);
		try(BasicContext context = new BasicContext()) { 
			List<Callable<EventStream>> callables = srcStoragePlugin.getDataForPV(context, srcPVName, srcTypeInfo.getCreationTime(), distantFuture, new DefaultRawPostProcessor());
			if(callables != null) { 
				for(Callable<EventStream> callable : callables) { 
					try(EventStream st = callable.call()) { 
						logger.info("Appending data from " + st.getDescription().getSource());
						destPlugin.appendData(context, destPVName, st);
					}
				}
			} else { 
				// I think this is not necessarily an error condition? So we just warn and move on...
				logger.warn("No data for source PV " + srcPVName + " using retrieval URL " + dataRetrievalURL);
			}
		} catch (Exception ex) { 
			try(PrintWriter out = resp.getWriter()) {
				String errorMsg = "Exception appending data for " + destPVName + ". Giving up on resharding " + srcPVName;
				logger.error(errorMsg);
				infoValues.put("validation", errorMsg);
				out.println(JSONValue.toJSONString(infoValues));
				cleanupTemporaryPV(configService, destPVName);
				return;
			}
		}
		
		
		long afterEventCount = getEventCount(configService, configService.getMyApplianceInfo(), destPVName, destTypeInfo.getCreationTime(), distantFuture);
		logger.info("After data transfer, we have " + afterEventCount + " events");
		
		if(beforeEventCount != afterEventCount) { 
			try(PrintWriter out = resp.getWriter()) {
				String errorMsg = "When transferring data for pv " + srcPVName + " we are not transferring the correct number of events. The source has " 
						+ beforeEventCount 
						+ " and the dest has " 
						+ afterEventCount
						+ ". Giving up on resharding " + srcPVName;
				logger.error(errorMsg);
				infoValues.put("validation", errorMsg);
				out.println(JSONValue.toJSONString(infoValues));
				cleanupTemporaryPV(configService, destPVName);
				logger.error(errorMsg);
				return;
			}
		}
		
		infoValues.put("beforeEventCount", Long.toString(beforeEventCount));
		infoValues.put("afterEventCount", Long.toString(afterEventCount));

		
		String deleteURL = srcApplianceInfo.getMgmtURL() + "/deletePV?pv=" + URLEncoder.encode(srcPVName, "UTF-8") + "&deleteData=true";
		logger.info("Deleting original PV from system using " + deleteURL);
		JSONObject deleteStatus =  GetUrlContent.getURLContentAsJSONObject(deleteURL);
		if(deleteStatus == null || deleteStatus.equals("") || !deleteStatus.containsKey("status") || !((String)deleteStatus.get("status")).equalsIgnoreCase("ok")) {
			logger.error("Invalid status deleting original PV from system using " + deleteURL);
			infoValues.put("deleteOriginal", "failed");
		} else { 
			logger.info("Deleted original PV from system using " + deleteURL);
			infoValues.put("deleteOriginal", "ok");
		}
		
		String renameURL = configService.getMyApplianceInfo().getMgmtURL() + "/renamePV?pv=" 
				+ URLEncoder.encode(destPVName, "UTF-8") 
				+ "&newname=" 
				+ URLEncoder.encode(srcPVName, "UTF-8");
		logger.info("Renaming temporary PV from system using " + renameURL);
		JSONObject renameStatus =  GetUrlContent.getURLContentAsJSONObject(renameURL);
		if(renameStatus == null || renameStatus.equals("") || !renameStatus.containsKey("status") || !((String)renameStatus.get("status")).equalsIgnoreCase("ok")) {
			logger.error("Invalid status renaming temporary PV using " + renameURL);
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("status", "Rename failed");
				infoValues.put("desc", "Could not rename temporary PV " + destPVName + " to source PV " + srcPVName);
				out.println(JSONValue.toJSONString(infoValues));
				return;
			}
		} else { 
			logger.info("Renamed temporary PV from system using " + renameURL);
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("status", "ok");
				infoValues.put("desc", "Successfully assigned " + srcPVName + " to appliance " + destApplianceIdentity);
				infoValues.put("renameTemporary", "ok");
				out.println(JSONValue.toJSONString(infoValues));
				return;
			}
		}
	}

	private void cleanupTemporaryPV(ConfigService configService, String destPVName) {
		try { 
			String deleteURL = configService.getMyApplianceInfo().getMgmtURL() + "/deletePV?pv=" + URLEncoder.encode(destPVName, "UTF-8") + "&deleteData=true";
			logger.info("Deleting temporary PV from system using " + deleteURL);
			GetUrlContent.getURLContentAsJSONObject(deleteURL);
		} catch (Throwable t) {
			logger.error("Exception deleting temporary PV " + destPVName, t);
		}
	}
	
	/**
	 * Get's the number of events that must be transferred over...
	 * @param pvName
	 * @param from
	 * @param to
	 * @return
	 */
	private long getEventCount(ConfigService configService, ApplianceInfo info, String pvName, Timestamp from, Timestamp to) { 
		long eventCount = 0;
		String dataRetrievalURL = info.getRetrievalURL().replace("/bpl", "") + "/data/getData.raw";
		try { 
			StoragePlugin srcStoragePlugin = StoragePluginURLParser.parseStoragePlugin("pbraw://localhost?rawURL=" + dataRetrievalURL, configService);
			try(BasicContext context = new BasicContext()) { 
				List<Callable<EventStream>> callables = srcStoragePlugin.getDataForPV(context, pvName, from, to, new DefaultRawPostProcessor());
				if(callables == null) return eventCount;

				for(Callable<EventStream> callable : callables) { 
					try(EventStream st = callable.call()) { 
						for(@SuppressWarnings("unused") Event e : st) {
							eventCount++;
						}
					}
				}
			}
		} catch (Exception ex) {
			logger.error("Exception determing event count using URL " + dataRetrievalURL + " for pv " + pvName);
		}
		return eventCount;
	}
}




