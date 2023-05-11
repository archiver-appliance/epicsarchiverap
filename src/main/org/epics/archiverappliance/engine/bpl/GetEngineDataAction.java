/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.ui.StreamPBIntoOutput;

/**
 * PV for getting the data for a PV from the engine's buffers
 * @author mshankar
 *
 */
public class GetEngineDataAction implements BPLAction {
	private static final Logger logger = LogManager.getLogger(GetEngineDataAction.class);
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		String startTimeStr = req.getParameter("from"); 
		String endTimeStr = req.getParameter("to");
		// ISO datetimes are of the form "2011-02-02T08:00:00.000Z" 
		Timestamp start = null;
		if(startTimeStr != null) { 
			start = TimeUtils.convertFromISO8601String(startTimeStr);
		}
		Timestamp end = null;
		if(endTimeStr != null) { 
			end = TimeUtils.convertFromISO8601String(endTimeStr);
		}

		EngineContext engineContext = configService.getEngineContext();
		if(engineContext.getChannelList().containsKey(pvName)){
			ArchiveChannel archiveChannel = engineContext.getChannelList().get(pvName);
			ArrayListEventStream st = archiveChannel.getPVData();
			HashMap<String, String> metaFields = archiveChannel.getCurrentCopyOfMetaFields();
			if(st != null && metaFields != null) { 
				mergeMetaFieldsIntoStream(st, metaFields);
			}
			if(st != null && !st.isEmpty()) {
				OutputStream os = resp.getOutputStream();
				try {
					long s = System.currentTimeMillis();
					int totalEvents = StreamPBIntoOutput.streamPBIntoOutputStream(st, os, start, end);
					long e = System.currentTimeMillis();
					logger.info("Found a total of " + totalEvents + " in " + (e-s) + "(ms)");
				} finally {
					try { os.flush(); os.close(); } catch(Throwable t) {}
				}
				return;
			} else { 
				if(metaFields != null && archiveChannel.getPVMetrics() != null) { 
					logger.debug("Inserting empty header with latest meta fields from engine");
					OutputStream os = resp.getOutputStream();
					try {
						RemotableEventStreamDesc desc = new RemotableEventStreamDesc(archiveChannel.getPVMetrics().getArchDBRTypes(), pvName, TimeUtils.getCurrentYear());
						if(!archiveChannel.isConnected() && archiveChannel.getPVMetrics() != null) { 
							long connectionLastLostEpochSeconds = archiveChannel.getPVMetrics().getConnectionLastLostEpochSeconds();
							if(connectionLastLostEpochSeconds != 0) {
								logger.debug("Adding a cnxlostepsecs header");
								metaFields.put("cnxlostepsecs", Long.toString(connectionLastLostEpochSeconds));
							}
						}
						desc.addHeaders(metaFields);
						StreamPBIntoOutput.writeHeaderOnly(os, desc);
					} finally {
						try { os.flush(); os.close(); } catch(Throwable t) {}
					}
					return;
				}
			}
		}

		logger.debug("No data for PV " + pvName + " in this engine.");
		resp.sendError(HttpServletResponse.SC_NOT_FOUND);
		return;
	}
	
	private void mergeMetaFieldsIntoStream(EventStream st, HashMap<String, String> metaFields) { 
		logger.debug("Merging meta fields from channel into engine's stream");
		RemotableEventStreamDesc desc = (RemotableEventStreamDesc) st.getDescription();
		try { 
			desc.addHeaders(metaFields);
		} catch(Exception ex) { 
			logger.error("Exception merging meta fields into stream", ex);
		}
	}
}
