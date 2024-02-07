package org.epics.archiverappliance.etl.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLInfo;
import org.epics.archiverappliance.etl.ETLInfoListStatistics;
import org.epics.archiverappliance.etl.ETLSource;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

/**
 * We schedule a ETLPVLookupItems with the appropriate thread using an ETLJob
 * @author mshankar
 *
 */
public class ETLJob implements Runnable {
    private static final Logger logger = LogManager.getLogger(ETLJob.class.getName());
    private final ETLPVLookupItems lookupItem;
    private boolean currentlyRunning = false;
    private long ETLRunStartEpochSeconds = 0;
    private final Instant runAsIfAtTime;
    private Exception exceptionFromLastRun = null;

    public ETLJob(ETLPVLookupItems lookupItem) {
        this.lookupItem = lookupItem;
        this.runAsIfAtTime = null;
    }

    /**
     * Mostly used by ETL unit tests.
     *
     * @param lookupItem    ETLPVLookupItems
     * @param runAsIfAtTime Instant
     */
    public ETLJob(ETLPVLookupItems lookupItem, Instant runAsIfAtTime) {
        this.lookupItem = lookupItem;
        this.runAsIfAtTime = runAsIfAtTime;
    }

    @Override
    public void run() {
        try {
            exceptionFromLastRun = null;
            if (this.runAsIfAtTime == null) {
                // We run ETL as if it were 10% of src partition seconds ago to give the previous lifetime time to
                // finish.
                long padding = Math.round(
                        this.lookupItem.getETLSource().getPartitionGranularity().getApproxSecondsPerChunk() * 0.1);
                Instant processingTime =
                        TimeUtils.convertFromEpochSeconds(TimeUtils.getCurrentEpochSeconds() - padding, 0);
                this.processETL(processingTime);
            } else {
                this.processETL(runAsIfAtTime);
            }
        } catch (Exception e) {
            logger.error("Exception processing ETL for " + lookupItem.toString(), e);
            exceptionFromLastRun = e;
        }
    }

    /**
     * Main ETL algorithm.
     *
     * @param processingTime Typically we'd use TimeUtils.now() for this. However, we can also run as if it's a certain
     *                       time by using this parameter.
     */
    public void processETL(Instant processingTime) {
        String pvName = lookupItem.getPvName();
        String jobDesc = lookupItem.toString();
        if (currentlyRunning) {
            logger.error("The previous ETL job (" + jobDesc + ") that began at "
                    + ((ETLRunStartEpochSeconds != 0)
                    ? TimeUtils.convertToHumanReadableString(ETLRunStartEpochSeconds)
                    : "Unknown")
                    + " is still running");
            return;
        }

        // We create a brand new context for each run.
        try (ETLContext etlContext = new ETLContext()) {
            currentlyRunning = true;
            ETLRunStartEpochSeconds = TimeUtils.getCurrentEpochSeconds();

            long pvETLStartEpochMilliSeconds = TimeUtils.getCurrentEpochMilliSeconds();

            if (logger.isDebugEnabled()) {
                logger.debug("Processing ETL for pv " + lookupItem.getPvName() + " from "
                        + lookupItem.getETLSource().getDescription()
                        + lookupItem.getETLDest().getDescription() + " as if it is "
                        + TimeUtils.convertToHumanReadableString(processingTime));
            }

            ETLSource curETLSource = lookupItem.getETLSource();
            ETLDest curETLDest = lookupItem.getETLDest();
            assert (curETLSource != null);
            assert (curETLDest != null);

            // Process each ETLInfo element in the list of ETLInfo
            // elements containing information about event streams
            // (e.g., files) ready for ETL for the current time and
            // PV name being processed.
            long time1 = System.currentTimeMillis();

            long time4getETLStreams = 0;
            List<ETLInfo> etlInfoList = curETLSource.getETLStreams(pvName, processingTime, etlContext);
            time4getETLStreams = time4getETLStreams + System.currentTimeMillis() - time1;
            if (etlInfoList != null && !etlInfoList.isEmpty()) {
                List<ETLInfo> movedList = new LinkedList<>();

                ETLInfoListStatistics etlInfoListStatistics = curETLDest
                        .etlInfoListProcessor(curETLSource)
                        .process(etlInfoList, pvName, movedList, this.lookupItem, etlContext);

                long time4markForDeletion = 0;
                long time4commitETLAppendData = 0;

                // Concatenate any append data for the current ETLDest
                // destination to this destination.
                try {
                    long time7 = System.currentTimeMillis();
                    boolean commitSuccessful = curETLDest.commitETLAppendData(pvName, etlContext);
                    time4commitETLAppendData = System.currentTimeMillis() - time7;

                    if (commitSuccessful) {
                        // Now that ETL processing has completed for the current
                        // event time and PV name being processed, loop through
                        // the list of ETLInfo elements containing information
                        // about event streams and mark each ETLSource event stream
                        // (e.g., file) for deletion.
                        for (ETLInfo infoItem : movedList) {
                            logger.debug("mark for deletion itemInfo key= " + infoItem.getKey());
                            long time4 = System.currentTimeMillis();
                            curETLSource.markForDeletion(infoItem, etlContext);
                            time4markForDeletion = time4markForDeletion + System.currentTimeMillis() - time4;
                        }
                    } else {
                        logger.error("Unsuccessful commiting ETL for pv " + pvName);
                    }
                } catch (IOException e) {
                    logger.error("IOException from prepareForNewPartition ", e);
                }

                long time4runPostProcessors = 0;
                try {
                    long time5 = System.currentTimeMillis();
                    curETLDest.runPostProcessors(pvName, lookupItem.getDbrType(), etlContext);
                    time4runPostProcessors = System.currentTimeMillis() - time5;
                } catch (Exception e) {
                    logger.error("Exception running post processors for pv " + pvName, e);
                }
                logger.debug("Executing post ETL tasks for this run");
                long time6 = System.currentTimeMillis();
                etlContext.executePostETLTasks();
                long time4executePostETLTasks = System.currentTimeMillis() - time6;
                logger.debug("Done executing post ETL tasks for this run");

                long pvETLEndEpochMilliSeconds = TimeUtils.getCurrentEpochMilliSeconds();
                lookupItem.addETLDurationInMillis(pvETLStartEpochMilliSeconds, pvETLEndEpochMilliSeconds);
                lookupItem.addInfoAboutDetailedTime(
                        time4getETLStreams,
                        etlInfoListStatistics.time4checkSizes(),
                        etlInfoListStatistics.time4prepareForNewPartition(),
                        etlInfoListStatistics.time4appendToETLAppendData(),
                        time4commitETLAppendData,
                        time4markForDeletion,
                        time4runPostProcessors,
                        time4executePostETLTasks,
                        etlInfoListStatistics.totalSrcBytes());
            } else {
                logger.debug("There were no ETL streams when running ETL for " + jobDesc);
            }
        } catch (IOException ex) {
            logger.error("IOException processing ETL for pv " + lookupItem.getPvName(), ex);
        } finally {
            currentlyRunning = false;
        }
    }

    /**
     * Was there an exception in the last ETL run for this job Mostly used by unit tests.
     *
     * @return exceptionFromLastRun  &emsp;
     */
    public Exception getExceptionFromLastRun() {
        return exceptionFromLastRun;
    }
}
