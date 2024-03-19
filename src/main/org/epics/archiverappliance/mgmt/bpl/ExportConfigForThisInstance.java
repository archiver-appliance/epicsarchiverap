package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

/**
 * Export the archiving configuration (PVTypeInfo's) for this instance as a JSON file. 
 * Used for export and import of configuration.
 * @author mshankar
 *
 */
public class ExportConfigForThisInstance implements BPLAction {
	private static Logger logger = LogManager.getLogger(ExportConfigForThisInstance.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,ConfigService configService) throws IOException {
		String identity = configService.getMyApplianceInfo().getIdentity();
		logger.info("Exporting config for this instance" +  identity);
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			out.println("[");
			JSONEncoder<PVTypeInfo> typeInfoEncoder = JSONEncoder.getEncoder(PVTypeInfo.class);
			boolean first = true;
			for(String pvName : configService.getPVsForThisAppliance()) {
				PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
				if(typeInfo != null) {
					if(first) { first = false; } else { out.println(","); }
					typeInfoEncoder.encodeAndPrint(typeInfo, out);
				} else {
					logger.error("Not exporting configuration for pv " + pvName + " in appliance " + identity);
				}
			}
			out.println("]");
		} catch(Exception ex) {
			throw new IOException(ex);
		}
	}
}
