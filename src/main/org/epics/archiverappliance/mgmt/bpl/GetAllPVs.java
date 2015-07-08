package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * 
 * @epics.BPLAction - Get all the PVs in the cluster. Note this call can return millions of PVs
 * @epics.BPLActionParam pv - An optional argument that can contain a <a href="http://en.wikipedia.org/wiki/Glob_%28programming%29">GLOB</a> wildcard. We will return PVs that match this GLOB. For example, if <code>pv=KLYS*</code>, the server will return all PVs that start with the string <code>KLYS</code>. If both pv and regex are unspecified, we match against all PVs. 
 * @epics.BPLActionParam regex - An optional argument that can contain a <a href="http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">Java regex</a> wildcard. We will return PVs that match this regex. For example, if <code>pv=KLYS*</code>, the server will return all PVs that start with the string <code>KLYS</code>. 
 * @epics.BPLActionParam limit - An optional argument that specifies the number of matched PV's that are retured. If unspecified, we return 500 PV names. To get all the PV names, (potentially in the millions), set limit to -1. 
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class GetAllPVs implements BPLAction {
	private static Logger logger = Logger.getLogger(GetAllPVs.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.debug("Getting all pvs for cluster");
		int defaultLimit = 500;
		LinkedList<String> pvNames = PVsMatchingParameter.getMatchingPVs(req, configService, defaultLimit);

		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(pvNames));
		} catch(Exception ex) {
			logger.error("Exception getting all pvs on appliance " + configService.getMyApplianceInfo().getIdentity(), ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}

}
