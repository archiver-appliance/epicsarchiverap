/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext.CommandThreadChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Details of a PV
 * @author mshankar
 *
 */
public class PVDetails implements BPLAction {
	private static final Logger logger = LogManager.getLogger(PVDetails.class);

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		try {
			logger.info("Getting the detailed status for PV " + pvName);
			PVTypeInfo typeInfoForPV = configService.getTypeInfoForPV(pvName);
			if(typeInfoForPV == null) {
				logger.error("Unable to find typeinfo for PV " + pvName);
				resp.sendError(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
			
			if(typeInfoForPV.isPaused()) { 
				LinkedList<Map<String, String>> statuses = new LinkedList<Map<String, String>>(); 
				List<CommandThreadChannel> immortalChannelsForPV = configService.getEngineContext().getAllChannelsForPV(pvName);
				if(immortalChannelsForPV.isEmpty()) { 
					addDetailedStatus(statuses, "Open channels", "0");					
				} else { 
					for(CommandThreadChannel immortalChanel : immortalChannelsForPV) { 
						addDetailedStatus(statuses, "Channel still hanging around", immortalChanel.getChannel().getName());
					}
				}
				resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
				try (PrintWriter out = resp.getWriter()) { 
					out.print(JSONValue.toJSONString(statuses));
				}
				return;
			}

			
			ArchDBRTypes dbrType = typeInfoForPV.getDBRType();
			PVMetrics metrics = ArchiveEngine.getMetricsforPV(pvName, configService);
			if(metrics != null){
				LinkedList<Map<String, String>> statuses = metrics.getDetailedStatus();
				ArchiveEngine.getLowLevelStateInfo(pvName, configService, statuses);
				
				if(dbrType.isV3Type()) { 
					ArchiveChannel channel = configService.getEngineContext().getChannelList().get(pvName);
					if(channel != null) { 
						int metaFieldCount = channel.getMetaChannelCount();
						int connectedMetaFieldCount = channel.getConnectedMetaChannelCount();
						addDetailedStatus(statuses, "Channels for the extra fields", "" + metaFieldCount);
						addDetailedStatus(statuses, "Connected channels for the extra fields", "" + connectedMetaFieldCount);
						addDetailedStatus(statuses, "Sample buffer capacity", "" + channel.getSampleBuffer().getCapacity());
						addDetailedStatus(statuses, "Time elapsed since search request (s)", "" + channel.getSecondsElapsedSinceSearchRequest());
					}
				}
				
				resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
				try (PrintWriter out = resp.getWriter()) { 
					out.print(JSONValue.toJSONString(statuses));
				}
			} else {
				logger.error("No status for PV " + pvName + " in this engine.");
				resp.sendError(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
		} catch(Exception ex) {
			logger.error("Exception getting details for PV " + pvName, ex);
			throw new IOException(ex);
		}
	}
	
	private static void addDetailedStatus(LinkedList<Map<String, String>> statuses, String name, String value) {
		Map<String, String> obj = new LinkedHashMap<String, String>();
		obj.put("name", name);
		obj.put("value", value);
		obj.put("source", "pv");
		statuses.add(obj);
	}
}
