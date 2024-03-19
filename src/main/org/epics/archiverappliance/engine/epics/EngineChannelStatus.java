/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.epics;

import java.util.LinkedHashMap;
import java.util.Map;

import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.json.simple.JSONAware;
import org.json.simple.JSONValue;

/**
 * POJO for engine status for a PV
 * @author mshankar
 *
 */
public class EngineChannelStatus implements JSONAware {
	private String pvName = "";
	private long epochSecondsOfLastEvent = 0L;
	private long lastRotateLogsEpochSeconds = 0L;
	private boolean isMonitored = true;
	private double samplingPeriod = 0;
	private long connectionFirstEstablishedEpochSeconds = 0L;
	private long connectionLastRestablishedEpochSeconds = 0L;
	private long connectionLossRegainCount = 0;
	private boolean connectionState = false;
	
	public EngineChannelStatus(PVMetrics metrics) {
		this.pvName = metrics.getPvName();
		this.epochSecondsOfLastEvent = metrics.getSecondsOfLastEvent();
		this.connectionState = metrics.isConnected();
		this.isMonitored = metrics.isMonitor();
		this.samplingPeriod = metrics.getSamplingPeriod();
		this.connectionFirstEstablishedEpochSeconds = metrics.getConnectionFirstEstablishedEpochSeconds();
		this.connectionLastRestablishedEpochSeconds = metrics.getConnectionLastRestablishedEpochSeconds();
		this.connectionLossRegainCount = metrics.getConnectionLossRegainCount();
	}

	public String getPvName() {
		return pvName;
	}
	public EngineChannelStatus setPvName(String pvName) {
		this.pvName = pvName;
		return this;
	}
	public long getEpochSecondsOfLastEvent() {
		return epochSecondsOfLastEvent;
	}
	public EngineChannelStatus setEpochSecondsOfLastEvent(long epochSecondsOfLastEvent) {
		this.epochSecondsOfLastEvent = epochSecondsOfLastEvent;
		return this;
	}
	public long getLastRotateLogsEpochSeconds() {
		return lastRotateLogsEpochSeconds;
	}
	public EngineChannelStatus setLastRotateLogsEpochSeconds(long lastRotateLogsEpochSeconds) {
		this.lastRotateLogsEpochSeconds = lastRotateLogsEpochSeconds;
		return this;
	}
	public boolean isMonitored() {
		return isMonitored;
	}
	public EngineChannelStatus setMonitored(boolean isMonitored) {
		this.isMonitored = isMonitored;
		return this;
	}
	public double getSamplingPeriod() {
		return samplingPeriod;
	}
	public EngineChannelStatus setSamplingPeriod(float samplingPeriod) {
		this.samplingPeriod = samplingPeriod;
		return this;
	}
	public long getConnectionFirstEstablishedEpochSeconds() {
		return connectionFirstEstablishedEpochSeconds;
	}
	public EngineChannelStatus setConnectionFirstEstablishedEpochSeconds(
			long connectionFirstEstablishedEpochSeconds) {
		this.connectionFirstEstablishedEpochSeconds = connectionFirstEstablishedEpochSeconds;
		return this;
	}
	public long getConnectionLastRestablishedEpochSeconds() {
		return connectionLastRestablishedEpochSeconds;
	}
	public EngineChannelStatus setConnectionLastRestablishedEpochSeconds(
			long connectionLastRestablishedEpochSeconds) {
		this.connectionLastRestablishedEpochSeconds = connectionLastRestablishedEpochSeconds;
		return this;
	}
	public long getConnectionLossRegainCount() {
		return connectionLossRegainCount;
	}
	public EngineChannelStatus setConnectionLossRegainCount(int connectionLossRegainCount) {
		this.connectionLossRegainCount = connectionLossRegainCount;
		return this;
	}
	public boolean isConnectionState() {
		return connectionState;
	}
	public EngineChannelStatus setConnectionState(boolean connectionState) {
		this.connectionState = connectionState;
		return this;
	}
	

	/**
	 * Return a RFC4627-compliant JSON version of this POJO.
	 * @return String RFC4627-compliant JSON
	 */
	@Override
	public String toJSONString() {
		Map<String, String> obj = new LinkedHashMap<String, String>();
		obj.put("pvName", pvName);
		obj.put("status", "Being archived");
		obj.put("isMonitored", Boolean.toString(isMonitored));
		obj.put("samplingPeriod", Float.toString((float)samplingPeriod));
		obj.put("connectionState", Boolean.toString(connectionState));
		obj.put("lastEvent", TimeUtils.convertToHumanReadableString(epochSecondsOfLastEvent));
		obj.put("lastRotateLogs", TimeUtils.convertToHumanReadableString(lastRotateLogsEpochSeconds));
		obj.put("connectionFirstEstablished", TimeUtils.convertToHumanReadableString(connectionFirstEstablishedEpochSeconds));
		obj.put("connectionLastRestablished", TimeUtils.convertToHumanReadableString(connectionLastRestablishedEpochSeconds));
		obj.put("connectionLossRegainCount", Long.toString(connectionLossRegainCount));
		return JSONValue.toJSONString(obj);
	}
}
