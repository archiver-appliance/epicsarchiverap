package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.function.Consumer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * 
 * @epics.BPLAction - Get all expanded PV names in the cluster. This is targeted at automation and should return the PV's being archived, the fields, .VAL's, aliases and PV's in the archive workflow. Note this call can return 10's of millions of names.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class GetAllExpandedPVNames implements BPLAction {
	private static Logger logger = LogManager.getLogger(GetAllExpandedPVNames.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.debug("Getting all expanded pv names for the cluster");
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			out.println("[");
			JSONValue.writeJSONString("EPICS:Archiver:Appliance", out); // Dummy value
			configService.getAllExpandedNames(new Consumer<String>() {
				@Override
				public void accept(String t) {
					try { 
						out.println(",");
						JSONValue.writeJSONString(t, out);
					} catch(Exception ex) { 
						logger.error("Exception writing all names out", ex);
					}
				} });
			out.println();
			out.println("]");
		} catch(Exception ex) {
			logger.error("Exception getting all pvs on appliance " + configService.getMyApplianceInfo().getIdentity(), ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}

}
