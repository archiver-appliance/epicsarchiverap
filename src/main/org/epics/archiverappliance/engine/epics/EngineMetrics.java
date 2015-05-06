/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.epics;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.metadata.MetaGet;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.json.simple.JSONAware;
import org.json.simple.JSONValue;

/**
 * POJO with some basic metrics.
 * @author mshankar
 *
 */
public class EngineMetrics implements JSONAware {
	private int pvCount;
	private int connectedPVCount;
	private int disconnectedPVCount;
	private int pausedPVCount;
	private int totalEPICSChannels;
	private double eventRate;
	private double dataRate;
	private double secondsConsumedByWritter=0.00;
	//private static Logger logger=Logger.getLogger(EngineMetrics.class.getName());
	

	public double getSecondsConsumedByWritter() {
		return secondsConsumedByWritter;
	}
	public void setSecondsConsumedByWritter(double secondsConsumedByWritter) {
		this.secondsConsumedByWritter = secondsConsumedByWritter;
	}
	public double getEventRate() {
		return eventRate;
	}
	public void setEventRate(double eventRate) {
		this.eventRate = eventRate;
	}
	public double getDataRate() {
		return dataRate;
	}
	public void setDataRate(double dataRate) {
		this.dataRate = dataRate;
	}

	public int getPvCount() {
		return pvCount;
	}
	public void setPvCount(int pvCount) {
		this.pvCount = pvCount;
	}
	public int getDisconnectedPVCount() {
		return disconnectedPVCount;
	}
	public void setDisconnectedPVCount(int disconnectedPVCount) {
		this.disconnectedPVCount = disconnectedPVCount;
	}

	@Override
	public String toJSONString() {
		DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
		HashMap<String, String> engineMetrics = new HashMap<String, String>();
		engineMetrics.put("eventRate", twoSignificantDigits.format(eventRate));
		engineMetrics.put("dataRate", twoSignificantDigits.format(dataRate));
		engineMetrics.put("dataRateGBPerDay", twoSignificantDigits.format((dataRate*60*60*24)/(1024*1024*1024)));
		engineMetrics.put("dataRateGBPerYear", twoSignificantDigits.format((dataRate*60*60*24*365)/(1024*1024*1024)));
		engineMetrics.put("pvCount", Integer.toString(pvCount));
		engineMetrics.put("connectedPVCount", Integer.toString(connectedPVCount));
		engineMetrics.put("disconnectedPVCount", Integer.toString(disconnectedPVCount));
		engineMetrics.put("formattedWriteThreadSeconds", twoSignificantDigits.format(secondsConsumedByWritter));
		engineMetrics.put("secondsConsumedByWritter", Double.toString(secondsConsumedByWritter));

		return JSONValue.toJSONString(engineMetrics);
	}
	
	public String getDetails(EngineContext context) {
		DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
		LinkedList<Map<String, String>> details = new LinkedList<Map<String, String>>();
		addDetailedStatus(details, "Total PV count", Integer.toString(pvCount));
		addDetailedStatus(details, "Disconnected PV count", Integer.toString(disconnectedPVCount));
		addDetailedStatus(details, "Connected PV count", Integer.toString(connectedPVCount));
		addDetailedStatus(details, "Paused PV count", Integer.toString(pausedPVCount));
		addDetailedStatus(details, "Total channels", Integer.toString(totalEPICSChannels));
		addDetailedStatus(details, "Event Rate (in events/sec)", twoSignificantDigits.format(eventRate));
		addDetailedStatus(details, "Data Rate (in bytes/sec)", twoSignificantDigits.format(dataRate));
		addDetailedStatus(details, "Data Rate in (GB/day)", twoSignificantDigits.format((dataRate*60*60*24)/(1024*1024*1024)));
		addDetailedStatus(details, "Data Rate in (GB/year)", twoSignificantDigits.format((dataRate*60*60*24*365)/(1024*1024*1024)));
		addDetailedStatus(details, "Time consumed for writing samplebuffers to STS (in secs)", twoSignificantDigits.format(secondsConsumedByWritter));
		if(secondsConsumedByWritter != 0) { 
			double writesPerSec = eventRate * context.getWritePeriod() / secondsConsumedByWritter;
			double writeBytesPerSec = (dataRate * context.getWritePeriod() / secondsConsumedByWritter)/(1024*1024);
			addDetailedStatus(details, "Benchmark - writing at (events/sec)", twoSignificantDigits.format(writesPerSec));
			addDetailedStatus(details, "Benchmark - writing at (MB/sec)", twoSignificantDigits.format(writeBytesPerSec));
		}
		addDetailedStatus(details, "PVs pending computation of meta info", Integer.toString(MetaGet.getPendingMetaGetsSize()));
		
		return JSONValue.toJSONString(details);
	}
	
	private static void addDetailedStatus(LinkedList<Map<String, String>> details, String name, String value) {
		Map<String, String> obj = new LinkedHashMap<String, String>();
		obj.put("name", name);
		obj.put("value", value);
		obj.put("source", "engine");
		details.add(obj);
	}
	
	
	public static EngineMetrics computeEngineMetrics(EngineContext engineContext, ConfigService configService) {
        EngineMetrics engineMetrics = new EngineMetrics();
        // Event rate is in events/sec
        double eventRate = 0.0;
        // Data rate is in bytes/sec
        double dataRate = 0.0;
        int connectedChannels = 0;
        int disconnectedChannels = 0;
        int totalChannels = 0;
        
		Set<String> pausedPVs = configService.getPausedPVsInThisAppliance();
        
        Iterator<Entry<String, ArchiveChannel>> it=engineContext.getChannelList().entrySet().iterator();
        while(it.hasNext()){
        	Entry<String, ArchiveChannel> tempEntry=it.next();
        	ArchiveChannel channel=tempEntry.getValue();
        	PVMetrics pvMetrics = channel.getPVMetrics();
        	String pvName = channel.getName();
        	if(pausedPVs.contains(pvName)) {
        		// Skipping paused PV.
        		continue;
        	}
        	
        	totalChannels++;
        	if(pvMetrics==null) {
				disconnectedChannels++;
				continue;
			}
			if(!pvMetrics.isConnected()) { 
				disconnectedChannels++;
			} else { 
				connectedChannels++;
			}
			eventRate += pvMetrics.getEventRate();
			dataRate += pvMetrics.getStorageRate();
        }
		engineMetrics.setEventRate(eventRate);
		engineMetrics.setDataRate(dataRate);

		engineMetrics.setPvCount(totalChannels);
		engineMetrics.setConnectedPVCount(connectedChannels);
		engineMetrics.setDisconnectedPVCount(disconnectedChannels);
		engineMetrics.setPausedPVCount(pausedPVs.size());
		int totalchannelCount = engineContext.getChannelList().size();
		for(ArchiveChannel archiveChannel : engineContext.getChannelList().values()) { 
			totalchannelCount += archiveChannel.getMetaChannelCount();
		}
		engineMetrics.setTotalEPICSChannels(totalchannelCount);
		engineMetrics.setSecondsConsumedByWritter(engineContext.getAverageSecondsConsumedByWritter());

		return engineMetrics;
	}
	public int getConnectedPVCount() {
		return connectedPVCount;
	}
	public void setConnectedPVCount(int connectedPVCount) {
		this.connectedPVCount = connectedPVCount;
	}
	public int getTotalEPICSChannels() {
		return totalEPICSChannels;
	}
	public void setTotalEPICSChannels(int totalEPICSChannels) {
		this.totalEPICSChannels = totalEPICSChannels;
	}
	/**
	 * @return the pausedPVCount
	 */
	public int getPausedPVCount() {
		return pausedPVCount;
	}
	/**
	 * @param pausedPVCount the pausedPVCount to set
	 */
	public void setPausedPVCount(int pausedPVCount) {
		this.pausedPVCount = pausedPVCount;
	}
}

