package org.epics.archiverappliance.retrieval.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * 
 * Get matching PV's for this appliance. Specify one of pv or regex. If both are specified, we only apply the pv wildcard. If neither is specified, we return an empty list.
 * <ol>
 * <li>pv - An optional argument that can contain a <a href="http://en.wikipedia.org/wiki/Glob_%28programming%29">GLOB</a> wildcard. We will return PVs that match this GLOB. For example, if <code>pv=KLYS*</code>, the server will return all PVs that start with the string <code>KLYS</code>.</li> 
 * <li>regex - An optional argument that can contain a <a href="http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">Java regex</a> wildcard. We will return PVs that match this regex. For example, if <code>pv=KLYS.*</code>, the server will return all PVs that start with the string <code>KLYS</code>.</li> 
 * <li>limit - An optional argument that specifies the number of matched PV's that are returned. If unspecified, we return 500 PV names. To get all the PV names, (potentially in the millions), set limit to -1.</li>
 * </ol> 
 * 
 * @author mshankar
 *
 */
public class GetMatchingPVs implements BPLAction {
	private static Logger logger = Logger.getLogger(GetMatchingPVs.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		
		resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		int limit = 500;
		String limitParam = req.getParameter("limit");
		if(limitParam != null) { 
			limit = Integer.parseInt(limitParam);
		}
		
		String nameToMatch = null;
		if(req.getParameter("regex") != null) { 
			nameToMatch = req.getParameter("regex");
			logger.debug("Finding PV's for regex " + nameToMatch);
		}
		
		if(req.getParameter("pv") != null) { 
			nameToMatch = req.getParameter("pv");
			nameToMatch = nameToMatch.replace("*", ".*");
			nameToMatch = nameToMatch.replace("?", ".");
			logger.debug("Finding PV's for glob (converted to regex)" + nameToMatch);			
		}
		
		if(nameToMatch == null) { 
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		String originatingAppliance = req.getParameter("originatingAppliance");
		
		try (PrintWriter out = resp.getWriter()) {
			List<String> matchingNames = getMatchingPVsInCluster(configService, limit, nameToMatch, originatingAppliance);
			if(limit > 0) { 
				Collections.sort(matchingNames);
				if(limit > 0 && matchingNames.size() >= limit) { 
					matchingNames = matchingNames.subList(0, limit);
				}
			}
			out.println(JSONValue.toJSONString(matchingNames));
		} catch(Exception ex) {
			logger.error("Exception getting all pvs on appliance " + configService.getMyApplianceInfo().getIdentity(), ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}


	/**
	 * Get a list of PV's being archived in this cluster
	 * @param configService ConfigService
	 * @param limit The numbers of PV's you want to limit the response to; 
	 * @param nameToMatch A regex specifying the PV name pattern; globs should be converted to regex's 
	 * @param originatingAppliance The appliance identity of the originating appliance; this is used to break circular proxies during searches.  
	 * @return mathcing PVs in the cluster
	 * @throws IOException  &emsp; 
	 */
	@SuppressWarnings("unchecked")
	public static List<String> getMatchingPVsInCluster(ConfigService configService, int limit, String nameToMatch, String originatingAppliance) throws IOException {
		List<JSONObject> applianceInfos = getAppliancesInCluster(configService);
		if(originatingAppliance != null) {
			for(JSONObject applianceInfo : applianceInfos) {
				if(applianceInfo.get("identity").equals(originatingAppliance)) {
					logger.debug("Breaking circular search request. Skipping request originating from one of the appliances in this cluster " + originatingAppliance);
					return new LinkedList<String>();
				}
			}
		}
		
		try { 
			LinkedList<String> mgmtURLs = getMgmtURLsInCluster(applianceInfos);
			
			List<String> pvNamesURLs = new LinkedList<String>();
			for(String mgmtURL : mgmtURLs) { 
				String pvNamesURL = mgmtURL + "/getMatchingPVsForThisAppliance"
						+ "?regex=" + URLEncoder.encode(nameToMatch, "UTF-8")
						+ "&limit=" + Integer.toString(limit);
				pvNamesURLs.add(pvNamesURL);
				logger.debug("Getting matching PV names using " + pvNamesURL);
			}
			
			Map<String, String> externalServers = configService.getExternalArchiverDataServers();
			if(externalServers != null) { 
				for(String serverUrl : externalServers.keySet()) { 
					String index = externalServers.get(serverUrl);
					if(index.equals("pbraw")) { 
						logger.debug("Asking external EPICS Archiver Appliance " + serverUrl + " for PV's matching " + nameToMatch);
						pvNamesURLs.add(serverUrl + "/bpl/getMatchingPVs?regex=" + URLEncoder.encode(nameToMatch, "UTF-8") + "&limit=" + Integer.toString(limit) + "&originatingAppliance=" + ((originatingAppliance == null) ? configService.getMyApplianceInfo().getIdentity() : originatingAppliance));
					}
				}
			}

			JSONArray matchingNames = GetUrlContent.combineJSONArrays(pvNamesURLs);
			return (List<String>) matchingNames;
		} catch(Exception ex) { 
			throw new IOException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	private static List<JSONObject> getAppliancesInCluster(ConfigService configService) {
		try { 
			JSONArray appliancesInCluster = GetUrlContent.getURLContentAsJSONArray(configService.getMyApplianceInfo().getMgmtURL() + "/getAppliancesInCluster");
			return (List<JSONObject>) appliancesInCluster;
		} catch(Throwable t) { 
			logger.error("Exception determining the appliances in the cluster", t);
			return null;
		}
	}

	
	private static LinkedList<String> getMgmtURLsInCluster(List<JSONObject> appliancesInCluster) {
		LinkedList<String> mgmtURLs = new LinkedList<String>();
		try { 
			for(Object object : appliancesInCluster) { 
				mgmtURLs.add((String)((JSONObject)object).get("mgmtURL"));
			}
			logger.debug("Returning " + mgmtURLs.size() + " mgmt URLs");
			return mgmtURLs;
		} catch(Throwable t) { 
			logger.error("Exception determining the appliances in the cluster", t);
			return null;
		}
	}
}
