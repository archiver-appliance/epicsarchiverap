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
 * <div>Appends the data for an older PV into a newer PV. The older PV is deleted and an alias is added mapping the older PV into the newer PV.</div>
 * 
 * <div>
 * All of these steps are driven by the destination appliance. The sequence of steps are
 * <ol>
 * <li>Make sure the DBR Type's for both PVs are the same.</li>
 * <li>Make sure the older PV and newer PV's are paused.</li>
 * <li>Consolidate data for both older and newer PV to the specified store (LTS).</li>
 * <li>Copy data from old PV until earliest timestamp of new PV as temporary PV</li>
 * <li>Copy data from new PV from earliest timestamp of new PV into temporary PV</li>
 * <li>Rename temporary PV to new PV</li>
 * <li>Delete old PV</li>
 * <li>Add alias for old PV pointing to new PV</li>
 * </ol>
 * </div>
 * 
 * @epics.BPLAction - This BPL appends the data for an older PV into a newer PV. The older PV is deleted and an alias mapping the older PV name to the new PV is added.
 * @epics.BPLActionParam olderpv - The name of the older pv. The data for this PV will be appended to the newer PV and then deleted.
 * @epics.BPLActionParam newerPV - The name of the newer pv. 
 * @epics.BPLActionParam storage - The name of the store until which we'll consolidate data before appending. This is typically a string like LTS.
 * @epics.BPLActionEnd
 *
 *
 * 
 * @author mshankar
 */
public class AppendAndAliasPV implements BPLAction {
	private static Logger logger = Logger.getLogger(AppendAndAliasPV.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String olderPVName = req.getParameter("olderpv");
		if(olderPVName == null || olderPVName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		String newerPVName = req.getParameter("newerpv");
		if(newerPVName == null || newerPVName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String storageName=req.getParameter("storage");
		if(storageName==null || storageName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String realOlderName = configService.getRealNameForAlias(olderPVName);
		if(realOlderName != null) olderPVName = realOlderName;
		String realNewerName = configService.getRealNameForAlias(newerPVName);
		if(realNewerName != null) newerPVName = realNewerName;
		
		PVTypeInfo olderTypeInfo = configService.getTypeInfoForPV(olderPVName);
		if(olderTypeInfo == null) {
			logger.debug("Unable to find typeinfo for PV " + olderPVName);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		PVTypeInfo newerTypeInfo = configService.getTypeInfoForPV(newerPVName);
		if(newerTypeInfo == null) {
			logger.debug("Unable to find typeinfo for PV " + newerPVName);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		
		if(!olderTypeInfo.isPaused()) {
			infoValues.put("validation", "Cannot append unless PV " + olderPVName + " is paused.");
			logger.error(infoValues.get("validation"));
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		}

		if(!newerTypeInfo.isPaused()) {
			infoValues.put("validation", "Cannot append unless PV " + newerPVName + " is paused.");
			logger.error(infoValues.get("validation"));
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		}
		
		if(olderTypeInfo.getDBRType() != newerTypeInfo.getDBRType()) {
			infoValues.put("validation", "The PVs " + olderPVName + " and " + newerPVName + " are of different DBR Types. Cannot consolidate.");
			logger.error(infoValues.get("validation"));
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}
			return;
		}
		
		boolean foundSrcPlugin = false;
		for(String store : newerTypeInfo.getDataStores()) {
			StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
			if(plugin.getName().equals(storageName)) {
				logger.debug("Found the storage plugin identifier by " + storageName);
				foundSrcPlugin = true;
				break;
			}
		}
		
		if(!foundSrcPlugin) {
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("validation", "Cannot find storage with name " + storageName + " for pv " + newerPVName);
				out.println(JSONValue.toJSONString(infoValues));
				return;
			}
		}
		
		ApplianceInfo newerApplianceInfo = configService.getApplianceForPV(newerPVName);
		if(newerApplianceInfo == null) {
			logger.error("Unable to find appliance for PV " + newerPVName);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		ApplianceInfo olderApplianceInfo = configService.getApplianceForPV(olderPVName);
		if(olderApplianceInfo == null) {
			logger.error("Unable to find appliance for PV " + olderPVName);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		if(!newerApplianceInfo.getIdentity().equals(configService.getMyApplianceInfo().getIdentity())) { 
			String redirectURL = newerApplianceInfo.getMgmtURL()
					+ "/appendAndAliasPV?olderpv=" 
					+ URLEncoder.encode(olderPVName, "UTF-8") 
					+ "&newerpv=" 
					+ URLEncoder.encode(newerPVName, "UTF-8")
					+ "&storage=" 
					+ URLEncoder.encode(storageName, "UTF-8"); 

			logger.info("Redirecting appendAndAliasPV request for older PV " + olderPVName + " and newer PV " + newerPVName +  " using URL " + redirectURL);
			JSONObject status = GetUrlContent.getURLContentAsJSONObject(redirectURL);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(status));
			}
			return;
		}
				
		// We are now on the same appliance as newerPV.
		logger.debug("Consolidating the data into " + storageName + " for newer PV " + newerPVName);
		
		String ETLConsolidateURL = newerApplianceInfo.getEtlURL() + "/consolidateDataForPV" 
				+ "?pv=" + URLEncoder.encode(newerPVName, "UTF-8")
				+ "&storage=" + URLEncoder.encode(storageName, "UTF-8"); 
		logger.info("Consolidating data for PV using URL " + ETLConsolidateURL);

		JSONObject consolidateStatus = GetUrlContent.getURLContentAsJSONObject(ETLConsolidateURL);
		if(consolidateStatus == null || consolidateStatus.equals("") || !consolidateStatus.containsKey("status") || !((String)consolidateStatus.get("status")).equalsIgnoreCase("ok")) {
			try(PrintWriter out = resp.getWriter()) {
				String errorMsg = "Unable to consolidate data for PV " + newerPVName;
				logger.error(errorMsg);
				infoValues.put("validation", errorMsg);
				out.println(JSONValue.toJSONString(infoValues));
				return;
			}
		}

		logger.info("Done consolidating data for PV using URL " + ETLConsolidateURL + ". Now cloning the type info");
		
		Random rand = new Random();
		String destPVName = newerPVName + "_appendAlias_" + rand.nextLong();
		int nameTries = 0;
		while(configService.getApplianceForPV(destPVName) != null) {
			logger.error(destPVName + " seems to exist. This is highly improbable. Trying again.");
			if(nameTries++ > 100) { 
				try(PrintWriter out = resp.getWriter()) {
					String errorMsg = "Unable to get a temporary name even after 100 tries. Giving up on appending " + newerPVName;
					logger.error(errorMsg);
					infoValues.put("validation", errorMsg);
					out.println(JSONValue.toJSONString(infoValues));
					return;
				}
			}
			destPVName = newerPVName + "_appendAlias_" + rand.nextLong();
		}

		PVTypeInfo destTypeInfo = new PVTypeInfo(destPVName, newerTypeInfo);
		destTypeInfo.setCreationTime(newerTypeInfo.getCreationTime());
		destTypeInfo.setModificationTime(TimeUtils.now());
		try { 
			configService.registerPVToAppliance(destPVName, configService.getMyApplianceInfo());
			destTypeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
			configService.updateTypeInfoForPV(destPVName, destTypeInfo);
		} catch(AlreadyRegisteredException ex) { 
			try(PrintWriter out = resp.getWriter()) {
				String errorMsg = "Temporary PV name is already registered " + destPVName + ". Giving up on appending " + newerPVName;
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
		
		// Get the first known timestamp for newer PV.
		long distantFutureEpochSeconds = System.currentTimeMillis()/1000 + 2*365*24*60*60;
		Timestamp distantFuture = TimeUtils.convertFromEpochSeconds(distantFutureEpochSeconds, 0);
		Timestamp distantPast = TimeUtils.minusDays(olderTypeInfo.getCreationTime(), 5*366);

		long epochForFirstEvent = distantFutureEpochSeconds;
		for(String store : newerTypeInfo.getDataStores()) {
			StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
			try(BasicContext context = new BasicContext(newerTypeInfo.getDBRType(), newerPVName)) {
				Event ev = plugin.getFirstKnownEvent(context, newerPVName);
				if(ev != null) {
					long fts = ev.getEpochSeconds();
					if(fts < epochForFirstEvent) {
						epochForFirstEvent = fts;
					}
				}
			}
		}
		
		if(epochForFirstEvent == distantFutureEpochSeconds) {
			String errorMsg = "We dont seem to have any data for " + newerPVName + ". This operation is equivalent to a rename";
			logger.warn(errorMsg);
		} else {
			logger.info("Copying over data from older PV " + olderPVName + " upto " + TimeUtils.convertToHumanReadableString(epochForFirstEvent));
		}
		
		Timestamp firstEventTS = TimeUtils.convertFromEpochSeconds(epochForFirstEvent, 0);
		
		long beforeEventCountOlder = getEventCount(configService, olderApplianceInfo, olderPVName, distantPast, firstEventTS);
		long beforeEventCountNewer = getEventCount(configService, newerApplianceInfo, olderPVName, firstEventTS, distantFuture);
		long beforeEventCountTotal = beforeEventCountOlder + beforeEventCountNewer;
 		logger.info("Before data transfer, we have " + beforeEventCountTotal + " events");
		
		{
			String olderPVRetrievalURL = olderApplianceInfo.getRetrievalURL().replace("/bpl", "") + "/data/getData.raw"; 
			logger.info("Getting data for older PV using URL " + olderPVRetrievalURL);
			StoragePlugin olderPVStoragePlugin = StoragePluginURLParser.parseStoragePlugin("pbraw://localhost?rawURL=" + olderPVRetrievalURL+"&skipExternalServers=true", configService);
			try(BasicContext context = new BasicContext()) { 
				List<Callable<EventStream>> callables = olderPVStoragePlugin.getDataForPV(context, olderPVName, distantPast, firstEventTS, new DefaultRawPostProcessor());
				if(callables != null) { 
					for(Callable<EventStream> callable : callables) { 
						try(EventStream st = callable.call()) { 
							logger.info("Appending data from " + st.getDescription().getSource());
							destPlugin.appendData(context, destPVName, st);
						}
					}
				} else { 
					try(PrintWriter out = resp.getWriter()) {
						String errorMsg = "There seems to be no data for " + olderPVName + ". Giving up on append and alias  " + olderPVName;
						infoValues.put("validation", errorMsg);
						out.println(JSONValue.toJSONString(infoValues));
						cleanupTemporaryPV(configService, destPVName);
						return;
					}
				}
			} catch (Exception ex) { 
				try(PrintWriter out = resp.getWriter()) {
					String errorMsg = "Exception appending data for " + destPVName + ". Giving up on append and alias  " + olderPVName;
					logger.error(errorMsg, ex);
					infoValues.put("validation", errorMsg);
					out.println(JSONValue.toJSONString(infoValues));
					cleanupTemporaryPV(configService, destPVName);
					return;
				}
			}			
		}

		{

			String newerPVRetrievalURL = olderApplianceInfo.getRetrievalURL().replace("/bpl", "") + "/data/getData.raw"; 
			logger.info("Getting data for newer PV using URL " + newerPVRetrievalURL);
			StoragePlugin newerPVStoragePlugin = StoragePluginURLParser.parseStoragePlugin("pbraw://localhost?rawURL=" + newerPVRetrievalURL+"&skipExternalServers=true", configService);
			try(BasicContext context = new BasicContext()) { 
				List<Callable<EventStream>> callables = newerPVStoragePlugin.getDataForPV(context, newerPVName, firstEventTS, distantFuture, new DefaultRawPostProcessor());
				if(callables != null) { 
					for(Callable<EventStream> callable : callables) { 
						try(EventStream st = callable.call()) { 
							logger.info("Appending data from " + st.getDescription().getSource());
							destPlugin.appendData(context, destPVName, st);
						}
					}
				} else {
					logger.warn("When appending and aliasing, there seems to no data for " + newerPVName);
				}
			} catch (Exception ex) { 
				try(PrintWriter out = resp.getWriter()) {
					String errorMsg = "Exception appending data for " + destPVName + ". Giving up on append and alias  " + olderPVName;
					logger.error(errorMsg, ex);
					infoValues.put("validation", errorMsg);
					out.println(JSONValue.toJSONString(infoValues));
					cleanupTemporaryPV(configService, destPVName);
					return;
				}
			}
		}
		
		long afterEventCountDest = getEventCount(configService, newerApplianceInfo, destPVName, distantPast, distantFuture);
 		logger.info("After data transfer, we have " + afterEventCountDest + " events with a difference of " + Math.abs(beforeEventCountTotal - afterEventCountDest) + " events");
 		
 		if(beforeEventCountTotal - afterEventCountDest > 10) {
 			// Use an arbitrary cutoff for data quality checks. We could lose an event or so on the boundaries.
			try(PrintWriter out = resp.getWriter()) {
				String errorMsg = "When copying data into a temporary PV, we seem to be missing  " + (beforeEventCountTotal - afterEventCountDest) + " events";
				infoValues.put("validation", errorMsg);
				out.println(JSONValue.toJSONString(infoValues));
				cleanupTemporaryPV(configService, destPVName);
				return;
			}
 		}
 		
 		{
			String deleteURL = newerApplianceInfo.getMgmtURL() + "/deletePV?pv=" + URLEncoder.encode(newerPVName, "UTF-8") + "&deleteData=true";
			logger.info("Deleting newer PV from system using " + deleteURL);
			JSONObject deleteStatus =  GetUrlContent.getURLContentAsJSONObject(deleteURL);
			if(deleteStatus == null || deleteStatus.equals("") || !deleteStatus.containsKey("status") || !((String)deleteStatus.get("status")).equalsIgnoreCase("ok")) {
				logger.error("Invalid status deleting newer PV from system using " + deleteURL);
				infoValues.put("deleteNewer", "failed");
			} else { 
				logger.info("Deleted newer PV from system using " + deleteURL);
				infoValues.put("deleteNewer", "ok");
			}
			
			String renameURL = newerApplianceInfo.getMgmtURL() + "/renamePV?pv=" 
					+ URLEncoder.encode(destPVName, "UTF-8") 
					+ "&newname=" 
					+ URLEncoder.encode(newerPVName, "UTF-8");
			logger.info("Renaming temporary PV from system using " + renameURL);
			JSONObject renameStatus =  GetUrlContent.getURLContentAsJSONObject(renameURL);
			if(renameStatus == null || renameStatus.equals("") || !renameStatus.containsKey("status") || !((String)renameStatus.get("status")).equalsIgnoreCase("ok")) {
				logger.error("Invalid status renaming temporary PV using " + renameURL);
				try(PrintWriter out = resp.getWriter()) {
					infoValues.put("status", "Rename failed");
					infoValues.put("desc", "Could not rename temporary PV " + destPVName + " to source PV " + newerPVName);
					out.println(JSONValue.toJSONString(infoValues));
					return;
				}
			} else { 
				logger.info("Renamed temporary PV from system using " + renameURL);
				// Cleanup the temporary PV after the rename is successful.
				cleanupTemporaryPV(configService, destPVName);
			}
 		}
 		
		// Now delete the older PV and add an alias mapping olderPVName to newerPVName		
 		{
			String deleteURL = olderApplianceInfo.getMgmtURL() + "/deletePV?pv=" + URLEncoder.encode(olderPVName, "UTF-8") + "&deleteData=true";
			logger.info("Deleting older PV from system using " + deleteURL);
			JSONObject deleteStatus =  GetUrlContent.getURLContentAsJSONObject(deleteURL);
			if(deleteStatus == null || deleteStatus.equals("") || !deleteStatus.containsKey("status") || !((String)deleteStatus.get("status")).equalsIgnoreCase("ok")) {
				logger.error("Invalid status deleting older PV from system using " + deleteURL);
				infoValues.put("deleteOlder", "failed");
			} else { 
				logger.info("Deleted older PV from system using " + deleteURL);
				infoValues.put("deleteOlder", "ok");
			} 	
 		}
 		{
			String addAliasURL = newerApplianceInfo.getMgmtURL() + "/addAlias?pv=" + URLEncoder.encode(newerPVName, "UTF-8") + "&aliasname=" + URLEncoder.encode(olderPVName, "UTF-8");
			logger.info("Deleting newer PV from system using " + addAliasURL);
			JSONObject allAliasStatus =  GetUrlContent.getURLContentAsJSONObject(addAliasURL);
			if(allAliasStatus == null || allAliasStatus.equals("") || !allAliasStatus.containsKey("status") || !((String)allAliasStatus.get("status")).equalsIgnoreCase("ok")) {
				logger.error("Invalid status adding alias for older PV from system using " + addAliasURL);
				infoValues.put("addAlias", "failed");
			} else { 
				logger.info("Added alias for older PV from system using " + addAliasURL);
				infoValues.put("addAlias", "ok");
			}
 		}
		
		
		try(PrintWriter out = resp.getWriter()) {
			infoValues.put("status", "ok");
			out.println(JSONValue.toJSONString(infoValues));
			return;
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
			StoragePlugin srcStoragePlugin = StoragePluginURLParser.parseStoragePlugin("pbraw://localhost?rawURL=" + dataRetrievalURL + "&skipExternalServers=true", configService);
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




