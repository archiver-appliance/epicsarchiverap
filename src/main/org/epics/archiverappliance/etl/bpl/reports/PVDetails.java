/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.common.ETLPVLookupItems;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Gets the ETL details of a PV.
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

		logger.info("Getting the detailed status for PV " + pvName);
		String detailedStatus = getDetailedETLStatusForPV(configService, pvName);
		if(detailedStatus != null){
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			try (PrintWriter out = resp.getWriter()) { 
				out.print(detailedStatus);
			}
		} else {
			logger.debug("No status for PV " + pvName + " in this ETL.");
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
	}

	private static void addDetailedStatus(LinkedList<Map<String, String>> statuses, String name, String value) {
		Map<String, String> obj = new LinkedHashMap<String, String>();
		obj.put("name", name);
		obj.put("value", value);
		obj.put("source", "etl");
		statuses.add(obj);
	}

	private String getDetailedETLStatusForPV(ConfigService configService, String pvName) {
		DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
		LinkedList<Map<String, String>> statuses = new LinkedList<Map<String, String>>();
		addDetailedStatus(statuses, "Name (from ETL)", pvName);
		for(ETLPVLookupItems lookupItem : configService.getETLLookup().getLookupItemsForPV(pvName)) {
			addDetailedStatus(statuses, "ETL " + lookupItem.getLifetimeorder() + " partition granularity of source", lookupItem.getETLSource().getPartitionGranularity().toString());
			addDetailedStatus(statuses, "ETL " + lookupItem.getLifetimeorder() + " partition granularity of dest", lookupItem.getETLDest().getPartitionGranularity().toString());
			if(lookupItem.getLastETLCompleteEpochSeconds()!= 0) {
				addDetailedStatus(statuses, "ETL " + lookupItem.getLifetimeorder() + " last completed", TimeUtils.convertToHumanReadableString(lookupItem.getLastETLCompleteEpochSeconds()));
				addDetailedStatus(statuses, "ETL " + lookupItem.getLifetimeorder() + " last job took (ms)", Long.toString(lookupItem.getLastETLTimeWeSpentInETLInMilliSeconds()));
			}
            addDetailedStatus(statuses, "ETL " + lookupItem.getLifetimeorder() + " next job runs at", TimeUtils.convertToHumanReadableString(lookupItem.getCancellingFuture().getDelay(TimeUnit.SECONDS) + (TimeUtils.now().toEpochMilli() / 1000)));
			if(lookupItem.getNumberofTimesWeETLed() != 0) {
				addDetailedStatus(statuses, "ETL " + lookupItem.getLifetimeorder() + " total time performing ETL(ms)", Long.toString(lookupItem.getTotalTimeWeSpentInETLInMilliSeconds()));
				addDetailedStatus(statuses, "ETL " + lookupItem.getLifetimeorder() + " average time performing ETL(ms)", Long.toString(lookupItem.getTotalTimeWeSpentInETLInMilliSeconds()/lookupItem.getNumberofTimesWeETLed()));
				addDetailedStatus(statuses, "ETL " + lookupItem.getLifetimeorder() + " number of times we performed ETL", Integer.toString(lookupItem.getNumberofTimesWeETLed()));
				addDetailedStatus(statuses, "ETL " + lookupItem.getLifetimeorder() + " out of space chunks deleted", Long.toString(lookupItem.getOutOfSpaceChunksDeleted()));
				String bytesTransferedUnits = "";
				long bytesTransferred = lookupItem.getTotalSrcBytes();
				double bytesTransferredInUnits = bytesTransferred;
				if(bytesTransferred > 1024*10 && bytesTransferred <= 1024*1024) { 
					bytesTransferredInUnits =  bytesTransferred/1024.0;
					bytesTransferedUnits = "(KB)";
				} else if (bytesTransferred > 1024*1024) { 
					bytesTransferredInUnits =  bytesTransferred/(1024.0*1024.0);
					bytesTransferedUnits = "(MB)";
				}
				addDetailedStatus(statuses, "ETL " + lookupItem.getLifetimeorder() + " approx bytes transferred" + bytesTransferedUnits, twoSignificantDigits.format(bytesTransferredInUnits));
				
				addDetailedStatus(statuses, "ETL Total time spent by getETLStreams() in ETL("+lookupItem.getLifetimeorder()+") (ms)", Long.toString(lookupItem.getTime4getETLStreams()));
				addDetailedStatus(statuses, "ETL Total time spent by free space checks in ETL("+lookupItem.getLifetimeorder()+") (ms)", Long.toString(lookupItem.getTime4checkSizes()));
				addDetailedStatus(statuses, "ETL Total time spent by prepareForNewPartition() in ETL("+lookupItem.getLifetimeorder()+") (ms)", Long.toString(lookupItem.getTime4prepareForNewPartition()));
				addDetailedStatus(statuses, "ETL Total time spent by appendToETLAppendData() in ETL("+lookupItem.getLifetimeorder()+") (ms)", Long.toString(lookupItem.getTime4appendToETLAppendData()));
				addDetailedStatus(statuses, "ETL Total time spent by commitETLAppendData() in ETL("+lookupItem.getLifetimeorder()+") (ms)", Long.toString(lookupItem.getTime4commitETLAppendData()));
				addDetailedStatus(statuses, "ETL Total time spent by markForDeletion() in ETL("+lookupItem.getLifetimeorder()+") (ms)", Long.toString(lookupItem.getTime4markForDeletion()));
				addDetailedStatus(statuses, "ETL Total time spent by runPostProcessors() in ETL("+lookupItem.getLifetimeorder()+") (ms)", Long.toString(lookupItem.getTime4runPostProcessors()));
				addDetailedStatus(statuses, "ETL Total time spent by executePostETLTasks() in ETL("+lookupItem.getLifetimeorder()+") (ms)", Long.toString(lookupItem.getTime4runPostProcessors()));

				
			} else {
				addDetailedStatus(statuses, "ETL " + lookupItem.getLifetimeorder() + " number of times we performed ETL", "None so far");
			}
		}
		return JSONValue.toJSONString(statuses);
	}

}
