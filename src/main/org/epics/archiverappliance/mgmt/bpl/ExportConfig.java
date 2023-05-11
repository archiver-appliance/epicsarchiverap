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

/**
 * Export the archiving configuration (PVTypeInfo's) as a JSON file. 
 * Used for export and import of configuration.
 * @author mshankar
 *
 */
public class ExportConfig implements BPLAction {
	private static Logger logger = LogManager.getLogger(ExportConfig.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Exporting PV archiving configuration ");
		LinkedList<String> exportPVTypeInfoURLs = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			String mgmtUrl = info.getMgmtURL();
			String exportPVTypeInfoURL = mgmtUrl + "/exportConfigForAppliance";
			exportPVTypeInfoURLs.add(exportPVTypeInfoURL);
		}

		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			GetUrlContent.combineJSONArraysAndPrintln(exportPVTypeInfoURLs, out);
		}
	}
}
