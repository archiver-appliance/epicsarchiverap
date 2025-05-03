package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

/**
 * 
 * @epics.BPLAction - Resume archiving the specified PV. 
 * @epics.BPLActionParam pv - The name of the pv. You can also pass in GLOB wildcards here and multiple PVs as a comma separated list. If you have more PVs that can fit in a GET, send the pv's as a CSV <code>pv=pv1,pv2,pv3</code> as the body of a POST.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class ResumeArchivingPV implements BPLAction {
	private static Logger logger = LogManager.getLogger(ResumeArchivingPV.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		if(req.getMethod().equals("POST")) { 
			resumeMultiplePVs(req, resp, configService);
			return;
		}

		
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		if(pvName.contains(",") || pvName.contains("*") || pvName.contains("?")) { 
			resumeMultiplePVs(req, resp, configService);
		} else { 
			// We only have one PV in the request
			List<String> pvNames = new LinkedList<String>();
			pvNames.add(pvName);
			resumeMultiplePVs(pvNames, resp, configService);
		}
	}

	private void resumeMultiplePVs(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException, UnsupportedEncodingException {
		LinkedList<String> pvNames = BulkPauseResumeUtils.getPVNames(req, configService);
		resumeMultiplePVs(pvNames, resp, configService);
	}

	private void resumeMultiplePVs(List<String> pvNames, HttpServletResponse resp, ConfigService configService) throws IOException, UnsupportedEncodingException {
		boolean askingToPausePV = false; 
		List<HashMap<String, String>> response = BulkPauseResumeUtils.pauseResumeByAppliance(pvNames, configService, askingToPausePV);
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(response));
		}
	}
}
