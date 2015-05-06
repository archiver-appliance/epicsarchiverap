package org.epics.archiverappliance.common;

import java.util.LinkedHashMap;

/**
 * Small easy to use profiler.
 * Not much functionality here... Keep walking.
 * @author mshankar
 *
 */
public class PoorMansProfiler {
	private LinkedHashMap<String, Long> steps = new LinkedHashMap<String, Long>();
	private long startingTimeMS = -1;
	private long latestMarkMS = -1;
	
	public PoorMansProfiler() { 
		this.startingTimeMS = System.currentTimeMillis();
	}

	public void mark(String location) { 
		latestMarkMS = System.currentTimeMillis();
		steps.put(location, latestMarkMS);
	}
	
	public String toString() { 
		StringBuilder buf = new StringBuilder();
		buf.append("\n");
		long previousTimeMS = startingTimeMS;
		for(String location : steps.keySet()) {
			Long currentTimeMS = steps.get(location);
			buf.append(location);
			buf.append(" --> ");
			buf.append(currentTimeMS - previousTimeMS);
			buf.append("(ms)\n");
			previousTimeMS = currentTimeMS;
		}
		buf.append("Total");
		buf.append(" --> ");
		buf.append(latestMarkMS - startingTimeMS);
		buf.append("(ms)\n");
		
		return buf.toString();
	}
	
	public long totalTimeMS() { 
		return latestMarkMS - startingTimeMS;
	}
}
