/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl.common;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLSource;

/**
 * ETL in the EAA often consists of multiple stages.
 * This encapsulates one such stage for a PV between one store in the datastores list and the next,
 * This also maintains the profiling details for this stage for this PV.
 * @author rdh
 */
public class ETLStage {
	private static final Logger logger = LogManager.getLogger();
	private String pvName;
	private ArchDBRTypes dbrType;
	private ETLSource source;
	private ETLDest dest;
	private int lifetimeorder = 0;
	private long delaybetweenETLJobsInSecs = 8 * 60 * 60;
	private ETLMetricsIntoStore metricsForLifetime;
	// Profiling details start here
	// This bool is to prevent multiple runs of ETL for the same PV for the same stage from interfering with each other
	private boolean currentlyRunning = false;
	private Instant lastETLStart = Instant.ofEpochSecond(0);
	private Instant nextETLStart = Instant.now().plus(366, ChronoUnit.DAYS);
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

	private Exception exceptionFromLastRun = null;
	
	
	public ETLStage(String pvName, ArchDBRTypes dbrType, ETLSource source, ETLDest dest, int lifetimeorder, ETLMetricsIntoStore metricsForLifetime, OutOfSpaceHandling outOfSpaceHandling) {
		this.pvName = pvName;
		this.dbrType = dbrType;
		this.source = source;
		this.dest = dest;
		this.lifetimeorder = lifetimeorder;
		this.metricsForLifetime = metricsForLifetime;
		this.outOfSpaceHandling = outOfSpaceHandling;
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
		ETLStage other = (ETLStage) obj;
		return this.pvName.equals(other.pvName) && this.lifetimeorder == other.lifetimeorder;
	}
	@Override
	public int hashCode() {
		return this.pvName.hashCode() + Integer.valueOf(this.lifetimeorder).hashCode();
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
	public ETLMetricsIntoStore getMetricsForLifetime() {
		return metricsForLifetime;
	}

	/*
	 * Is a ETLJob for this stage currently running
	 */
	public boolean isCurrentlyRunning() {
		return currentlyRunning;
	}

	public Instant getLastETLStart() {
		return lastETLStart;
	}
	
	public Instant getNextETLStart() {
		return nextETLStart;
	}

	public void beginRunning() {
		this.currentlyRunning = true;
		this.lastETLStart = Instant.now();
		this.nextETLStart = this.nextETLStart.plus(this.delaybetweenETLJobsInSecs, ChronoUnit.SECONDS);
		this.exceptionFromLastRun = null;
	}
	public void doneRunning() {
		this.currentlyRunning = false;
		this.lastETLStart = Instant.ofEpochSecond(0);
	}

    /**
     * Was there an exception in the last ETL run for this job Mostly used by unit tests.
     *
     * @return exceptionFromLastRun  &emsp;
     */
	public Exception getExceptionFromLastRun() {
		return exceptionFromLastRun;
	}

	public void setExceptionFromLastRun(Exception exceptionFromLastRun) {
		this.exceptionFromLastRun = exceptionFromLastRun;
	}

	public long getDelaybetweenETLJobsInSecs() {
		return delaybetweenETLJobsInSecs;
	}

	public void setDelaybetweenETLJobsInSecs(long delaybetweenETLJobsInSecs) {
		this.delaybetweenETLJobsInSecs = delaybetweenETLJobsInSecs;
        // Given the delay; find the next modulo
		// For example, if the delay is an hour we find the 0th min/0th second in the next hour
		Instant nextModuloZerothSec = Instant.ofEpochSecond(
			((Instant.now().getEpochSecond() + this.delaybetweenETLJobsInSecs)
			/this.delaybetweenETLJobsInSecs)
			*this.delaybetweenETLJobsInSecs);
        
        // Add a small buffer to this
        this.nextETLStart = nextModuloZerothSec.plusSeconds(5L * 60);
		logger.debug("Setting delay to {} with next ETL start at {} for stage {} -> {} for pv {}",
			this.delaybetweenETLJobsInSecs,
			TimeUtils.convertToHumanReadableString(this.nextETLStart),
			this.getETLSource().getName(),
			this.getETLDest().getName(),
			this.pvName
		);
	}

    public long getInitialDelay() { 
        Instant currentTime = Instant.now();
        // We compute the initial delay so that the ETL jobs run at a predictable time.
        long initialDelay = Duration.between(currentTime, this.nextETLStart)
                .getSeconds();
		return initialDelay;
    }
}
