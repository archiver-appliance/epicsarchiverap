package org.epics.archiverappliance.retrieval.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Send a true/false if we are archiving the given PV or not.
 * <ol> 
 * <li>pv - The name of the pv.</li>
 * </ol>
 * @author mshankar
 *
 */
public class AreWeArchivingPV implements BPLAction {
	private static Logger logger = LogManager.getLogger(AreWeArchivingPV.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		logger.debug("Checking to see if we are archiving PV " + pvName);
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}

		// String pvNameFromRequest = pvName;
		String realName = configService.getRealNameForAlias(pvName);
		if(realName != null) pvName = realName;
		
		HashMap<String, String> retVal = new HashMap<String, String>();

		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			typeInfo = configService.getTypeInfoForPV(PVNames.stripFieldNameFromPVName(pvName));
			if(typeInfo == null) {
				retVal.put("status", Boolean.FALSE.toString());
			} else { 
				retVal.put("status", Boolean.TRUE.toString());
			}
		} else { 
			retVal.put("status", Boolean.TRUE.toString());
		}
		
		
		try (PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(retVal));
		} catch(Exception ex) {
			logger.error("Exception checking if we are archiving pv " + pvName, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}
}
