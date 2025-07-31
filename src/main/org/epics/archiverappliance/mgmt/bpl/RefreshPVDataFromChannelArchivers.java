package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 *
 * Call the config service to refresh the PV data from ChannelArchivers.
 * This should help with proxying ChannelArchiver servers that are still active.
 * @author mshankar
 *
 */
public class RefreshPVDataFromChannelArchivers implements BPLAction {
	private static Logger logger = LogManager.getLogger(RefreshPVDataFromChannelArchivers.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Updating the PV data from all the ChannelArchivers");
		configService.refreshPVDataFromChannelArchiverDataServers();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		try(PrintWriter out = resp.getWriter()) {
			infoValues.put("status", "ok");
			out.println(JSONValue.toJSONString(infoValues));
		}
		
	}
}
