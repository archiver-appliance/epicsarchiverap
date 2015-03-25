/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl.common;

import java.util.concurrent.ScheduledFuture;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.etl.common.ETLGatingState;

/**
 * A POJO for PV name, ETLSource, and ETLDest items,
 * which can be used as elements in a list (e.g.,
 * implementations of the ETLPVLookup interface,
 * such as PBThreeTierETLPVLookup).
 * @author rdh
 *
 */
public class ETLPVLookupItems {
	private String pvName;
	private ArchDBRTypes dbrType;
	private ETLSource source;
	private ETLDest dest;
	private int lifetimeorder = 0;
	private ETLMetricsForLifetime metricsForLifetime;
	private long lastETLCompleteEpochSeconds = 0L;
	private long lastETLTimeWeSpentInETLInMilliSeconds = 0L;
	private long totalTimeWeSpentInETLInMilliSeconds = 0L;
	private long time4getETLStreams;
	private long time4checkSizes;
	private long time4prepareForNewPartition;
	private long time4appendToETLAppendData;
	private long time4commitETLAppendData; 
	private long time4markForDeletion;
	private long time4runPostProcessors;
	private long time4executePostETLTasks;
	private long totalSrcBytes;
	
	private int numberofTimesWeETLed = 0;
	private ScheduledFuture<?> cancellingFuture;
	
	private OutOfSpaceHandling outOfSpaceHandling;
	private long outOfSpaceChunksDeleted = 0;
	
	private final ETLGatingState gatingState;
	
	public ETLPVLookupItems(String pvName, ArchDBRTypes dbrType, ETLSource source, ETLDest dest, int lifetimeorder, ETLMetricsForLifetime metricsForLifetime, OutOfSpaceHandling outOfSpaceHandling, ETLGatingState gatingState) {
		this.pvName = pvName;
		this.dbrType = dbrType;
		this.source = source;
		this.dest = dest;
		this.lifetimeorder = lifetimeorder;
		this.metricsForLifetime = metricsForLifetime;
		this.outOfSpaceHandling = outOfSpaceHandling;
		this.gatingState = gatingState;
	}

	public int getLifetimeorder() {
		return lifetimeorder;
	}

	public String getPvName() {
		return pvName;
	}
	
	public void setPvName(String pvName) {
		this.pvName = pvName;
	}
	
	public ETLSource getETLSource() {
		return source;
	}
	
	public void setETLSource(ETLSource source) {
		this.source = source;
	}
	
	public ETLDest getETLDest() {
		return dest;
	}
	
	public void setETLDest(ETLDest dest) {
		this.dest = dest;
	}
	
	@Override
	public boolean equals(Object obj) {
		ETLPVLookupItems other = (ETLPVLookupItems) obj;
		return this.pvName.equals(other.pvName) && this.lifetimeorder == other.lifetimeorder;
	}
	@Override
	public int hashCode() {
		return this.pvName.hashCode() + new Integer(this.lifetimeorder).hashCode();
	}

	public long getTotalTimeWeSpentInETLInMilliSeconds() {
		return totalTimeWeSpentInETLInMilliSeconds;
	}

	public int getNumberofTimesWeETLed() {
		return numberofTimesWeETLed;
	}
	
	public long getLastETLCompleteEpochSeconds() {
		return lastETLCompleteEpochSeconds;
	}

	public void addETLDurationInMillis(long pvETLStartEpochMilliSeconds, long pvETLEndEpochMilliSeconds) {
		lastETLTimeWeSpentInETLInMilliSeconds = (pvETLEndEpochMilliSeconds - pvETLStartEpochMilliSeconds);
		totalTimeWeSpentInETLInMilliSeconds += lastETLTimeWeSpentInETLInMilliSeconds;
		numberofTimesWeETLed++;
		lastETLCompleteEpochSeconds = pvETLEndEpochMilliSeconds/1000;
		metricsForLifetime.timeForOverallETLInMilliSeconds += lastETLTimeWeSpentInETLInMilliSeconds;
		metricsForLifetime.totalETLRuns = Math.max(numberofTimesWeETLed, metricsForLifetime.totalETLRuns);
		metricsForLifetime.updateApproximateGlobalLastETLTime(lastETLTimeWeSpentInETLInMilliSeconds);
	}

	public ArchDBRTypes getDbrType() {
		return dbrType;
	}
	
	public ScheduledFuture<?> getCancellingFuture() {
		return cancellingFuture;
	}

	public void setCancellingFuture(ScheduledFuture<?> cancellingFuture) {
		this.cancellingFuture = cancellingFuture;
	}
	
	public String toString() { 
		return pvName + "(" + lifetimeorder + ")";
	}

	public void addInfoAboutDetailedTime(long time4getETLStreams, long time4checkSizes, long time4prepareForNewPartition, long time4appendToETLAppendData,
			long time4commitETLAppendData, long time4markForDeletion, long time4runPostProcessors, long time4executePostETLTasks,
			long totalSrcBytes) {
		this.time4getETLStreams += time4getETLStreams;
		this.time4checkSizes += time4checkSizes;
		this.time4prepareForNewPartition += time4prepareForNewPartition;
		this.time4appendToETLAppendData += time4appendToETLAppendData;
		this.time4commitETLAppendData += time4commitETLAppendData; 
		this.time4markForDeletion += time4markForDeletion;
		this.time4runPostProcessors += time4runPostProcessors;
		this.time4executePostETLTasks += time4executePostETLTasks;
		this.totalSrcBytes += totalSrcBytes;
		
		metricsForLifetime.timeinMillSecond4getETLStreams += time4getETLStreams;
		metricsForLifetime.timeinMillSecond4checkSizes += time4checkSizes;
		metricsForLifetime.timeinMillSecond4prepareForNewPartition += time4prepareForNewPartition;
		metricsForLifetime.timeinMillSecond4appendToETLAppendData += time4appendToETLAppendData;
		metricsForLifetime.timeinMillSecond4commitETLAppendData += time4commitETLAppendData; 
		metricsForLifetime.timeinMillSecond4markForDeletion += time4markForDeletion;
		metricsForLifetime.timeinMillSecond4runPostProcessors += time4runPostProcessors;
		metricsForLifetime.timeinMillSecond4executePostETLTasks += time4executePostETLTasks;
		metricsForLifetime.totalSrcBytes += totalSrcBytes;
	}

	public long getTime4getETLStreams() {
		return time4getETLStreams;
	}

	public long getTime4prepareForNewPartition() {
		return time4prepareForNewPartition;
	}

	public long getTime4appendToETLAppendData() {
		return time4appendToETLAppendData;
	}

	public long getTime4commitETLAppendData() {
		return time4commitETLAppendData;
	}

	public long getTime4markForDeletion() {
		return time4markForDeletion;
	}

	public long getTime4runPostProcessors() {
		return time4runPostProcessors;
	}

	public long getTime4executePostETLTasks() {
		return time4executePostETLTasks;
	}

	public OutOfSpaceHandling getOutOfSpaceHandling() {
		return outOfSpaceHandling;
	}

	public long getOutOfSpaceChunksDeleted() {
		return outOfSpaceChunksDeleted;
	}
	
	public void outOfSpaceChunkDeleted() { 
		outOfSpaceChunksDeleted++;
	}

	public long getTotalSrcBytes() {
		return totalSrcBytes;
	}

	public long getLastETLTimeWeSpentInETLInMilliSeconds() {
		return lastETLTimeWeSpentInETLInMilliSeconds;
	}

	public long getTime4checkSizes() {
		return time4checkSizes;
	}

	/**
	 * @return the metricsForLifetime
	 */
	public ETLMetricsForLifetime getMetricsForLifetime() {
		return metricsForLifetime;
	}
	
	public ETLGatingState getGatingState() {
		return gatingState;
	}
}
