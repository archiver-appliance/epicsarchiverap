package org.epics.archiverappliance.engine.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

/**
 * Get the last known timestamp for all PV's.
 * This does not sort/filter the results in any way intentionally. 
 * This is meant to be consumed by other reports in mgmt.
 * @author mshankar
 *
 */
public class LastKnownTimeStampReport implements BPLAction {
	private static Logger logger = Logger.getLogger(LastKnownTimeStampReport.class.getName());
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Generating a last known timestamp report");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			out.println("[");
			boolean first = true;
			for(ArchiveChannel channel : configService.getEngineContext().getChannelList().values()) {
				PVMetrics pvMetrics = channel.getPVMetrics();
				if(first) { first = false; } else { out.println(","); }
				out.println("{");
				out.print("\"pvName\": \"");
				out.print(channel.getName());
				out.println("\",");
				out.print("\"lastEvent\": ");
				out.print(pvMetrics.getSecondsOfLastEvent());
				out.println();
				out.print("}");
			}
			out.println("]");
		}
	}
}