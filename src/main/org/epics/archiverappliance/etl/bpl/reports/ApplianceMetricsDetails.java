/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.common.ETLMetricsForLifetime;
import org.json.simple.JSONValue;

/**
 * Get the metrics details for an appliance for ETL. 
 * @author mshankar
 *
 */
public class ApplianceMetricsDetails implements BPLAction {

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		try (PrintWriter out = resp.getWriter()) {
			out.println(getETLMetricsDetails(configService));
		}
	}

	public static String getETLMetricsDetails(ConfigService configService) {
		DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
		LinkedList<Map<String, String>> details = new LinkedList<Map<String, String>>();
		ETLMetricsForLifetime[] metricsForLifetime = configService.getETLLookup().getApplianceMetrics();
		if(metricsForLifetime == null || metricsForLifetime.length < 1) {
			addDetailedStatus(details, "Startup", "In Progress");
		} else { 
			for(ETLMetricsForLifetime metricForLifetime : metricsForLifetime) {
				String lifetimeIdentifier = metricForLifetime.getLifeTimeId() + "&raquo;" + (metricForLifetime.getLifeTimeId()+1);
				long totalRunsNum = metricForLifetime.getTotalETLRuns();
				long timeForOverallETLInMillis=metricForLifetime.getTimeForOverallETLInMilliSeconds();
				addDetailedStatus(details, "Total number of ETL("+lifetimeIdentifier+") runs so far", Long.toString(totalRunsNum));
				if(totalRunsNum != 0){
					double avgETLTimeInSeconds = ((double)timeForOverallETLInMillis)/(totalRunsNum*1000.0);
					addDetailedStatus(details, "Average time spent in ETL("+lifetimeIdentifier+") (s/run)", twoSignificantDigits.format(avgETLTimeInSeconds));
					double timeSpentInETLPercent = (avgETLTimeInSeconds*100)/(TimeUtils.getCurrentEpochSeconds() - metricForLifetime.getStartOfMetricsMeasurementInEpochSeconds());
					addDetailedStatus(details, "Average percentage of time spent in ETL("+lifetimeIdentifier+")", twoSignificantDigits.format(timeSpentInETLPercent));
					addDetailedStatus(details, "Approximate time taken by last job in ETL("+lifetimeIdentifier+") (s)", twoSignificantDigits.format(metricForLifetime.getApproximateLastGlobalETLTimeInMillis()/1000));
					addDetailedStatus(details, "Estimated weekly usage in ETL("+lifetimeIdentifier+") (%)", twoSignificantDigits.format(metricForLifetime.getWeeklyETLUsageInPercent()));
					addDetailedStatus(details, "Avg time spent by getETLStreams() in ETL("+lifetimeIdentifier+") (s/run)", twoSignificantDigits.format(((double)metricForLifetime.getTimeinMillSecond4getETLStreams())/(1000.0*totalRunsNum)));
					addDetailedStatus(details, "Avg time spent by free space checks in ETL("+lifetimeIdentifier+") (s/run)", twoSignificantDigits.format(((double)metricForLifetime.getTimeinMillSecond4checkSizes())/(1000.0*totalRunsNum)));
					addDetailedStatus(details, "Avg time spent by prepareForNewPartition() in ETL("+lifetimeIdentifier+") (s/run)", twoSignificantDigits.format(((double)metricForLifetime.getTimeinMillSecond4prepareForNewPartition())/(1000.0*totalRunsNum)));
					addDetailedStatus(details, "Avg time spent by appendToETLAppendData() in ETL("+lifetimeIdentifier+") (s/run)", twoSignificantDigits.format(((double)metricForLifetime.getTimeinMillSecond4appendToETLAppendData())/(1000.0*totalRunsNum)));
					addDetailedStatus(details, "Avg time spent by commitETLAppendData() in ETL("+lifetimeIdentifier+") (s/run)", twoSignificantDigits.format(((double)metricForLifetime.getTimeinMillSecond4commitETLAppendData())/(1000.0*totalRunsNum)));
					addDetailedStatus(details, "Avg time spent by markForDeletion() in ETL("+lifetimeIdentifier+") (s/run)", twoSignificantDigits.format(((double)metricForLifetime.getTimeinMillSecond4markForDeletion())/(1000.0*totalRunsNum)));
					addDetailedStatus(details, "Avg time spent by runPostProcessors() in ETL("+lifetimeIdentifier+") (s/run)", twoSignificantDigits.format(((double)metricForLifetime.getTimeinMillSecond4runPostProcessors())/(1000.0*totalRunsNum)));
					addDetailedStatus(details, "Avg time spent by executePostETLTasks() in ETL("+lifetimeIdentifier+") (s/run)", twoSignificantDigits.format(((double)metricForLifetime.getTimeinMillSecond4executePostETLTasks())/(1000.0*totalRunsNum)));

					String bytesTransferedUnits = "";
					long bytesTransferred = metricForLifetime.getTotalSrcBytes();
					double bytesTransferredInUnits = bytesTransferred;
					if(bytesTransferred > 1024*10 && bytesTransferred <= 1024*1024) { 
						bytesTransferredInUnits =  bytesTransferred/1024.0;
						bytesTransferedUnits = "(KB)";
					} else if (bytesTransferred > 1024*1024 && bytesTransferred <= 1024*1024*1024) { 
						bytesTransferredInUnits =  bytesTransferred/(1024.0*1024.0);
						bytesTransferedUnits = "(MB)";
					} else if (bytesTransferred > 1024*1024*1024) {
						bytesTransferredInUnits =  bytesTransferred/(1024.0*1024.0*1024.0);
						bytesTransferedUnits = "(GB)";
					}

					addDetailedStatus(details, "Estimated bytes transferred in ETL ("+lifetimeIdentifier+")"+bytesTransferedUnits, twoSignificantDigits.format(bytesTransferredInUnits));
				}
			}
		}


		return JSONValue.toJSONString(details);
	}
	
	private static void addDetailedStatus(LinkedList<Map<String, String>> statuses, String name, String value) {
		Map<String, String> obj = new LinkedHashMap<String, String>();
		obj.put("name", name);
		obj.put("value", value);
		obj.put("source", "etl");
		statuses.add(obj);
	}
}
