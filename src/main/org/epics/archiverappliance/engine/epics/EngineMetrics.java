/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.epics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.reports.Details;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.metadata.MetaGet;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVContext;
import org.epics.archiverappliance.engine.pv.PVMetrics;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * POJO with some basic metrics.
 * @author mshankar
 *
 */
public class EngineMetrics implements Details {
    private static final Logger logger = LogManager.getLogger(EngineMetrics.class);
    private int pvCount;
    private int connectedPVCount;
    private int disconnectedPVCount;
    private int pausedPVCount;
    private int totalEPICSChannels;
    private double eventRate;
    private double dataRate;
    private double secondsConsumedByWriter = 0.00;
    // private static Logger logger=Logger.getLogger(EngineMetrics.class.getName());

    public double getSecondsConsumedByWriter() {
        return secondsConsumedByWriter;
    }

    public void setSecondsConsumedByWriter(double secondsConsumedByWriter) {
        this.secondsConsumedByWriter = secondsConsumedByWriter;
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

    public Map<String, String> toJSONString() {
        DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
        HashMap<String, String> engineMetrics = new HashMap<String, String>();
        engineMetrics.put("eventRate", twoSignificantDigits.format(eventRate));
        engineMetrics.put("dataRate", twoSignificantDigits.format(dataRate));
        engineMetrics.put(
                "dataRateGBPerDay", twoSignificantDigits.format((dataRate * 60 * 60 * 24) / (1024 * 1024 * 1024)));
        engineMetrics.put(
                "dataRateGBPerYear",
                twoSignificantDigits.format((dataRate * 60 * 60 * 24 * 365) / (1024 * 1024 * 1024)));
        engineMetrics.put("pvCount", Integer.toString(pvCount));
        engineMetrics.put("connectedPVCount", Integer.toString(connectedPVCount));
        engineMetrics.put("disconnectedPVCount", Integer.toString(disconnectedPVCount));
        engineMetrics.put("formattedWriteThreadSeconds", twoSignificantDigits.format(secondsConsumedByWriter));
        engineMetrics.put("secondsConsumedByWriter", Double.toString(secondsConsumedByWriter));

        return engineMetrics;
    }

    @Override
    public LinkedList<Map<String, String>> details(ConfigService configService) {
        EngineContext context = configService.getEngineContext();
        DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
        LinkedList<Map<String, String>> details = new LinkedList<Map<String, String>>();
        details.add(this.metricDetail("Total PV count", Integer.toString(pvCount)));
        details.add(this.metricDetail("Disconnected PV count", Integer.toString(disconnectedPVCount)));
        details.add(this.metricDetail("Connected PV count", Integer.toString(connectedPVCount)));
        details.add(this.metricDetail("Paused PV count", Integer.toString(pausedPVCount)));
        details.add(this.metricDetail("Total channels", Integer.toString(totalEPICSChannels)));
        details.add(this.metricDetail(
                "Approx pending jobs in engine queue", Long.toString((context.getMainSchedulerPendingTasks()))));
        details.add(this.metricDetail("Event Rate (in events/sec)", twoSignificantDigits.format(eventRate)));
        details.add(this.metricDetail("Data Rate (in bytes/sec)", twoSignificantDigits.format(dataRate)));
        details.add(this.metricDetail(
                "Data Rate in (GB/day)",
                twoSignificantDigits.format((dataRate * 60 * 60 * 24) / (1024 * 1024 * 1024))));
        details.add(this.metricDetail(
                "Data Rate in (GB/year)",
                twoSignificantDigits.format((dataRate * 60 * 60 * 24 * 365) / (1024 * 1024 * 1024))));
        details.add(this.metricDetail(
                "Time consumed for writing samplebuffers to STS (in secs)",
                twoSignificantDigits.format(secondsConsumedByWriter)));
        if (secondsConsumedByWriter != 0) {
            double writesPerSec = eventRate * context.getWritePeriod() / secondsConsumedByWriter;
            double writeBytesPerSec = (dataRate * context.getWritePeriod() / secondsConsumedByWriter) / (1024 * 1024);
            details.add(this.metricDetail(
                    "Benchmark - writing at (events/sec)", twoSignificantDigits.format(writesPerSec)));
            details.add(this.metricDetail(
                    "Benchmark - writing at (MB/sec)", twoSignificantDigits.format(writeBytesPerSec)));
        }
        details.add(this.metricDetail(
                "PVs pending computation of meta info", Integer.toString(MetaGet.getPendingMetaGetsSize())));
        details.add(this.metricDetail(
                "Total number of reference counted channels", Integer.toString(PVContext.getChannelCount())));
        details.add(this.metricDetail("Total number of CAJ channels", Integer.toString(context.getCAJChannelCount())));

        details.addAll(context.getCAJContextDetails());

        return details;
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

        for (Entry<String, ArchiveChannel> tempEntry :
                engineContext.getChannelList().entrySet()) {
            ArchiveChannel channel = tempEntry.getValue();
            String pvName = channel.getName();
            try {
                if (channel.isPaused()) {
                    logger.debug("Skipping paused PV {}", pvName);
                    continue;
                }

                PVMetrics pvMetrics = channel.getPVMetrics();
                totalChannels++;
                if (pvMetrics == null) {
                    disconnectedChannels++;
                    continue;
                }
                if (!pvMetrics.isConnected()) {
                    disconnectedChannels++;
                } else {
                    connectedChannels++;
                }
                eventRate += pvMetrics.getEventRate();
                dataRate += pvMetrics.getStorageRate();
            } catch (Exception ex) {
                logger.error("Exception computing engine metrics for PV " + channel.getName(), ex);
            }
        }
        engineMetrics.setEventRate(eventRate);
        engineMetrics.setDataRate(dataRate);

        engineMetrics.setPvCount(totalChannels);
        engineMetrics.setConnectedPVCount(connectedChannels);
        engineMetrics.setDisconnectedPVCount(disconnectedChannels);
        engineMetrics.setPausedPVCount(engineContext.getPausedPVCount());
        int totalchannelCount = engineContext.getChannelList().size();
        for (ArchiveChannel archiveChannel : engineContext.getChannelList().values()) {
            totalchannelCount += archiveChannel.getMetaChannelCount();
        }
        engineMetrics.setTotalEPICSChannels(totalchannelCount);
        engineMetrics.setSecondsConsumedByWriter(engineContext.getAverageSecondsConsumedByWriter());

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

    @Override
    public ConfigService.WAR_FILE source() {
        return ConfigService.WAR_FILE.ENGINE;
    }
}
