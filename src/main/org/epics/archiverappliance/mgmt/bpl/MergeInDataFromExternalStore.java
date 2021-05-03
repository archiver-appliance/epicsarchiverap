package org.epics.archiverappliance.mgmt.bpl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.mergededup.MergeDedupEventStream;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ConversionFunction;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.stanford.slac.archiverappliance.PBOverHTTP.InputStreamBackedEventStream;

/**
 * 
 * @epics.BPLAction - Manually merges in data from an external appliance that is used for failover.  
 * @epics.BPLActionParam pv - The name of the pv; the PV needs to be paused. It's probably also a good idea to consolidate the data to the store being used for merging.
 * @epics.BPLActionParam other - This is the <code>data_retrieval_url</code> of the other appliance.
 * @epics.BPLActionParam storage - The name of the data store (LTS etc) to merge the data into. Please consolidate the data into this store to make sure merging in later data from the remote store does not result in losing data in the other stores on this appliance.   
 * @epics.BPLActionParam from - The start time for the retrieval request to the other server - defaults to 2 months ago.
 * @epics.BPLActionParam to - The end time for the retrieval request to the other server - defaults to a month from now.
 * @epics.BPLActionEnd
 * 
 * Use this to merge in data that is outside any ETL windows. See {@link org.epics.archiverappliance.common.mergededup.MergeDedupStoragePlugin}
 * Note, this call actually changes the underlying data; so please do make a backup before making this call; use with caution.
 * 
 * @author mshankar
 *
 */
public class MergeInDataFromExternalStore implements BPLAction {
	private static Logger logger = Logger.getLogger(MergeInDataFromExternalStore.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String other = req.getParameter("other");
		if(other==null || other.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		String storagePluginName = req.getParameter("storage");
		if(storagePluginName==null || storagePluginName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		String endTimeStr = req.getParameter("to");
		Timestamp end = TimeUtils.plusDays(TimeUtils.now(), 31);
		if(endTimeStr != null) {
			try { 
				end = TimeUtils.convertFromISO8601String(endTimeStr);
			} catch(IllegalArgumentException ex) {
				try { 
					end = TimeUtils.convertFromDateTimeStringWithOffset(endTimeStr);
				} catch(IllegalArgumentException ex2) { 
					String msg = "Cannot parse time" + endTimeStr;
					logger.warn(msg, ex2);
					resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
					resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
					return;
				}
			}
		}
		
		String startTimeStr = req.getParameter("from"); 
		Timestamp start = TimeUtils.minusDays(TimeUtils.now(), 2*31);
		if(startTimeStr != null) {
			try { 
				start = TimeUtils.convertFromISO8601String(startTimeStr);
			} catch(IllegalArgumentException ex) {
				try { 
					start = TimeUtils.convertFromDateTimeStringWithOffset(startTimeStr);
				} catch(IllegalArgumentException ex2) { 
					String msg = "Cannot parse time " + startTimeStr;
					logger.warn(msg, ex2);
					resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
					resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
					return;
				}
			}
		}
		
		if(end.before(start)) {
			String msg = "For request, end " + end.toString() + " is before start " + start.toString() + " for pv " + pvName;
			logger.error(msg);
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		final Timestamp startTime = start, endTime = end;
		
		
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
			String redirectURL = info.getMgmtURL() + "/mergeInData?pv=" + URLEncoder.encode(pvName, "UTF-8") + "&other=" + URLEncoder.encode(other, "UTF-8"); 
			logger.debug("Routing request to the appliance hosting the PV " + pvName + " using URL " + redirectURL);
			JSONObject status = GetUrlContent.getURLContentAsJSONObject(redirectURL);
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(status));
			}
			return;
			
		}
		
		logger.info("Merging in data for PV " + pvName + " fetching data from " + other);

		PVTypeInfo typeInfo = PVNames.determineAppropriatePVTypeInfo(pvName, configService);
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

		class MergeInData implements ConversionFunction {
			@Override
			public EventStream convertStream(EventStream srcEventStream) throws IOException {
				if(srcEventStream.getDescription() instanceof RemotableEventStreamDesc) {
					RemotableEventStreamDesc desc = (RemotableEventStreamDesc) srcEventStream.getDescription();
					String serverURL = other + "/data/getData.raw" 
							+ "?pv=" + desc.getPvName() 
							+ "&from=" + TimeUtils.convertToISO8601String(startTime) 
							+ "&to=" + TimeUtils.convertToISO8601String(endTime)
							+ "&skipExternalServers=true";
					logger.info("Getting data from URL " + serverURL);
					InputStream is = GetUrlContent.getURLContentAsStream(serverURL);
					if(is != null) {
						InputStreamBackedEventStream eis = new InputStreamBackedEventStream(new BufferedInputStream(is), startTime);
						return new MergeDedupEventStream(srcEventStream, eis);
					} else {
						logger.error("Other stream is null for " + srcEventStream.getDescription());
					}
				}
				logger.error("Skipping merging in event stream " + srcEventStream.getDescription());
				return srcEventStream;
			}
		}

		for(String store : typeInfo.getDataStores()) {
			StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
			if(!plugin.getName().equals(storagePluginName)) {
				logger.info("Skippng merging in storage " + plugin.getName() + " as it is not " + storagePluginName);
				continue;
			}
			try(BasicContext context = new BasicContext()) {
				logger.info("Merging in data for plugin " + plugin.getName() + " for PV " + pvName);
				plugin.convert(context, pvName, new MergeInData());
				logger.info("Done merging in data for plugin " + plugin.getName() + " for PV " + pvName);
			}
		}
		
		
		infoValues.put("status", "ok");
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(infoValues));
		}
	}
}
