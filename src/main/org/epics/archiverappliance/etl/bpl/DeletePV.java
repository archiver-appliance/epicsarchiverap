/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/


package org.epics.archiverappliance.etl.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLInfo;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
/**
 * consolidate PB files for one pv before one storage
 * @author Luofeng  Li 
 *
 */
public class DeletePV implements BPLAction {
	private static final Logger logger = LogManager.getLogger(DeletePV.class);
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		boolean deleteData = false;
		String deleteDataStr=req.getParameter("deleteData");
		if(deleteDataStr!=null && !deleteDataStr.equals("")) {
			deleteData = Boolean.parseBoolean(deleteDataStr);
		}
		
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			logger.debug("Unable to find typeinfo for PV...");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		try (PrintWriter out = resp.getWriter()) {
			// Remove any ETL jobs from the runtime state. 
			configService.getETLLookup().deleteETLJobs(pvName);
			
			if(deleteData) {
				HashMap<String, String> timingValues = new HashMap<String, String>();
				infoValues.put("deletes_timing", timingValues);
				try(ETLContext context = new ETLContext()) {
					Instant tenYearsLaterTimeStamp = TimeUtils.convertFromEpochSeconds(TimeUtils.getCurrentEpochSeconds() + 10 * 365 * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(), 0);
					for(String dataSource : typeInfo.getDataStores()) {
						try {
							ETLSource etlSource = StoragePluginURLParser.parseETLSource(dataSource, configService);
							if(etlSource == null) continue;
							List<ETLInfo> infos = etlSource.getETLStreams(pvName, tenYearsLaterTimeStamp, context);
							if(infos == null) continue;
							for(ETLInfo info : infos) {
								timingValues.put(info.getKey() + ": Start", TimeUtils.convertToHumanReadableString(System.currentTimeMillis()/1000));
								logger.debug("Marking src " + info.getKey() + " for deletion when stopping archiving pv " + pvName);
								etlSource.markForDeletion(info, context);
								timingValues.put(info.getKey() + ": End", TimeUtils.convertToHumanReadableString(System.currentTimeMillis()/1000));
							}
						} catch(Exception ex) {
							logger.error("Exception deleting data for PV " + pvName, ex);
						}
					}
				}
			}
			
			infoValues.put("status", "ok");
			infoValues.put("desc", "Successfully removed PV " + pvName + " from the cluster");
			out.println(JSONValue.toJSONString(infoValues));
			return;
		}
		
	}
}
