package org.epics.archiverappliance.mgmt.bpl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * Updates the type info for a PV. The typeinfo should be sent in the POST body as JSON.
 * 
 * @epics.BPLAction - Updates the typeinfo for the specified PV. Note this merely updates the typeInfo. It does not have any logic to react to changes in the typeinfo. That is, don't assume that the PV is automatically paused just because you changed the isPaused to true. This is meant to be used in conjuction with other BPL to implement site-specific BPL in external code (for example, python). 
 * @epics.BPLActionParam pv - The name of the pv.
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class PutPVTypeInfo implements BPLAction {
	private static Logger logger = Logger.getLogger(PutPVTypeInfo.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		logger.debug("Getting typeinfo for PV " + pvName);
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			logger.warn("Cannot find typeinfo for " + pvName);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		try (LineNumberReader lineReader = new LineNumberReader(new InputStreamReader(new BufferedInputStream(req.getInputStream())))) {
			JSONParser parser=new JSONParser();
			PVTypeInfo updatedInfo = new PVTypeInfo();
			JSONObject updatedTypeInfoJSON = (JSONObject) parser.parse(lineReader);
			JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
			decoder.decode(updatedTypeInfoJSON, updatedInfo);
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			try (PrintWriter out = resp.getWriter()) {
				logger.info("Updating typeInfo for PV " + pvName);
				configService.updateTypeInfoForPV(pvName, updatedInfo);
				PVTypeInfo typeInfoAfterConfigServiceUpdate = configService.getTypeInfoForPV(pvName);
				JSONEncoder<PVTypeInfo> jsonEncoder = JSONEncoder.getEncoder(PVTypeInfo.class);
				out.println(jsonEncoder.encode(typeInfoAfterConfigServiceUpdate));
			} catch(Exception ex) {
				logger.error("Exception updating typeinfo for pv " + pvName, ex);
				throw new IOException(ex);
			}
			
		} catch(Throwable t) { 
			logger.error("Exception parsing typeinfo for pv " + pvName, t);
			throw new IOException(t);
		}
		
	}
}
