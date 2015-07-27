package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * 
 * @epics.BPLAction - Get matching PV's for this appliance. Specify one of pv or regex. If both are specified, we only apply the pv wildcard. If neither is specified, we return an empty list.
 * @epics.BPLActionParam pv - An optional argument that can contain a <a href="http://en.wikipedia.org/wiki/Glob_%28programming%29">GLOB</a> wildcard. We will return PVs that match this GLOB. For example, if <code>pv=KLYS*</code>, the server will return all PVs that start with the string <code>KLYS</code>. 
 * @epics.BPLActionParam regex - An optional argument that can contain a <a href="http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">Java regex</a> wildcard. We will return PVs that match this regex. For example, if <code>pv=KLYS.*</code>, the server will return all PVs that start with the string <code>KLYS</code>. 
 * @epics.BPLActionParam limit - An optional argument that specifies the number of matched PV's that are returned. If unspecified, we return 500 PV names. To get all the PV names, (potentially in the millions), set limit to -1. 
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class GetMatchingPVsForAppliance implements BPLAction {
	private static Logger logger = Logger.getLogger(GetMatchingPVsForAppliance.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.debug("Getting PV's matching wildcard or regex for this appliance.");
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
		

		Set<String> pvNamesMatchingRegex = configService.getPVsForApplianceMatchingRegex(nameToMatch);
		
		LinkedList<String> pvNames = new LinkedList<String>();
		if(limit == -1) { 
			pvNames.addAll(pvNamesMatchingRegex);
		} else {
			int pvCount = 0;
			for(String matchedName : pvNamesMatchingRegex) { 
				pvNames.add(matchedName);
				pvCount++;
				if(pvCount >= limit) break;
			}
		}

		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(pvNames));
		} catch(Exception ex) {
			logger.error("Exception getting all pvs on appliance " + configService.getMyApplianceInfo().getIdentity(), ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}

}
