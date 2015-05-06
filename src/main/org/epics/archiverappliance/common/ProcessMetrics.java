package org.epics.archiverappliance.common;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONObject;

/**
 * Maintains a days worth of a limited number of predefined process metrics. 
 * @author mshankar
 *
 */
public class ProcessMetrics {
	private static final int MINUTES_IN_DAY = 24*60;
	private static Logger logger = Logger.getLogger(ProcessMetrics.class.getName());
	
	public ProcessMetrics() { 
		
	}
	
	public class ProcessMetric {
		long timeInEpochSeconds;
		double systemLoadAverage;
		double heapUsedPercent;
		
		ProcessMetric() { 
			this.timeInEpochSeconds = TimeUtils.getCurrentEpochSeconds();
			systemLoadAverage = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
			MemoryUsage memoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
			heapUsedPercent = (((double) memoryUsage.getUsed())/memoryUsage.getMax())*100;
		}
		
		public long getTimeInEpochSeconds() {
			return timeInEpochSeconds;
		}
		public double getSystemLoadAverage() {
			return systemLoadAverage;
		}
		public double getHeapUsedPercent() {
			return heapUsedPercent;
		}
	}
	
	LinkedList<ProcessMetric> processMetrics = new LinkedList<ProcessMetric>();
	
	public void takeMeasurement() { 
		try {
			ProcessMetric metric = new ProcessMetric();
			if(processMetrics.size() >= MINUTES_IN_DAY) {
				int pre = processMetrics.size();
				while(processMetrics.size() >= MINUTES_IN_DAY) {
					processMetrics.pop();
				}
				int post = processMetrics.size();
				logger.debug("Popped " + (post - pre) + " process metrics");
			}
			processMetrics.add(metric);
			assert(processMetrics.size() <= MINUTES_IN_DAY);
		} catch (Exception ex) { 
			logger.error("Exception retrieving MBean information", ex);
		}
	}

	public HashMap<String, Object> getProcessMetricsJSON(String header) throws IOException {
		try {
			HashMap<String, Object> ret = new HashMap<String, Object>();
			ret.put(header + "uptime", ManagementFactory.getRuntimeMXBean().getUptime()/1000);
			ret.put(header + "start", TimeUtils.convertToHumanReadableString(ManagementFactory.getRuntimeMXBean().getStartTime()/1000));
			JSONEncoder<ProcessMetric> processMetricEnc = JSONEncoder.getEncoder(ProcessMetric.class);
			LinkedList<JSONObject> mets = new LinkedList<JSONObject>();
			for(ProcessMetric processMetric : processMetrics) {
				mets.add(processMetricEnc.encode(processMetric));
			}
			ret.put(header + "metrics", mets);
			return ret;
		} catch(Exception ex) { 
			throw new IOException(ex);
		}
	}
}
