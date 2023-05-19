package org.epics.archiverappliance.engine.bpl;

import gov.aps.jca.Channel;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.pv.EngineContext.CommandThreadChannel;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import com.cosylab.epics.caj.CAJChannel;

/**
 * This is a reasonably dangerous call. 
 * It appears that there is a bug somewhere in CAJ (or in the engine code that uses it) where we leave channels hanging around in an inconsistent state.
 * Specifically, we seem to have an entry in the ChannelSearchManager but the channel itself has not proceeded to the tcp/ip circuit.
 * This code forcibly closes these CAJ channels. 
 * Now; I'm unclear what that does to the engine layer above; it may leave the engine in a state where we need to restart it. 
 * However; I'm going to attempt this on one of the stuck PVs soon.
 * I'd rather fix the bug in CAJ but until I can get Matej a reproducible usecase....
 * @author mshankar
 *
 */
public class CleanUpAnyImmortalChannels implements BPLAction {
	private static Logger logger = LogManager.getLogger(CleanUpAnyImmortalChannels.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			logger.error("Cannot get typeinfo for PV " + pvName);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		if(!typeInfo.isPaused()) { 
			logger.error("PV " + pvName + " is not paused. At the very least, please pause the PV first.");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		List<CommandThreadChannel> immortalChannelsForPV = configService.getEngineContext().getAllChannelsForPV(pvName);
		for(final CommandThreadChannel immortalCommandThreadChannel : immortalChannelsForPV) { 
			immortalCommandThreadChannel.getCommandThread().addCommand(new Runnable() {
				@Override
				public void run() {
					try { 
						Channel immortalChannel = immortalCommandThreadChannel.getChannel();
						logger.error("Forcibly closing channel for " + immortalChannel.getName());
						if(immortalChannel instanceof CAJChannel) { 
							((CAJChannel)immortalChannel).destroy(true);
						} else { 
							immortalChannel.destroy();
						}
					} catch(Throwable t) { 
						logger.error("Exception forcibly closing channel", t);
					}
				}
			});
		}

		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			infoValues.put("status", "ok");
			infoValues.put("Channels destroyed", immortalChannelsForPV.size());
			out.println(JSONValue.toJSONString(infoValues));
		}
	}
}
