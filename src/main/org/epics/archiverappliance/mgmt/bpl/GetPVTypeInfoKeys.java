package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;

/**
 * @epics.BPLAction - Get a list of all the PV names that have a PVTypeInfo. 
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class GetPVTypeInfoKeys implements BPLAction {
	private static Logger logger = LogManager.getLogger(GetPVTypeInfoKeys.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Getting PVTypeInfo keys");
		LinkedList<String> typeInfoURLs = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			String mgmtUrl = info.getMgmtURL();
			String typeInfoURL = mgmtUrl + "/getPVsForThisAppliance";
			typeInfoURLs.add(typeInfoURL);
		}

		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			JSONArray.writeJSONString(GetUrlContent.combineJSONArrays(typeInfoURLs), out);
		}
	}
}
