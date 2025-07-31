package org.epics.archiverappliance.engine.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

/**
 * Lists all the channels that are currently active
 * @author mshankar
 *
 */
public class ListAllChannels implements BPLAction {

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		EngineContext engineRuntime = configService.getEngineContext();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			out.println("[");
			boolean first = true;
			for(Map.Entry<String,ArchiveChannel> entry: engineRuntime.getChannelList().entrySet()) {
				if(first) { first = false; } else { out.println(","); }
				out.print("\"");
				out.print(entry.getKey());
				out.print("\"");
				for(String metaChannelName : entry.getValue().getMetaPVNames()) { 
					out.print("\"");
					out.print(metaChannelName);
					out.print("\"");
				}
			}
			out.println();
			out.println("]");
		}
	}
}
