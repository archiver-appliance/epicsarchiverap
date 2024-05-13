package org.epics.archiverappliance.etl.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLInfo;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.etl.StorageMetrics;

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

    private static boolean notEnoughFreeSpace(
            long sizeOfSrcStream, StorageMetrics destMetrics, ETLPVLookupItems lookupItem, String key, String pvName)
            throws IOException {

        if (sizeOfSrcStream > 0 && destMetrics != null) {
            long freeSpace = destMetrics.getUsableSpace(lookupItem.getMetricsForLifetime());
            long freeSpaceBuffer = 1024 * 1024;
            // We leave space for at lease freeSpaceBuffer in the dest so that you can login and have some
            // room to repair damage coming in from an out of space condition.
            long estimatedSpaceNeeded = sizeOfSrcStream + freeSpaceBuffer;
            if (freeSpace < estimatedSpaceNeeded) {

                logger.error("No space on dest when moving ETLInfo with key = " + key
                        + " for PV " + pvName + " as we estimate we need "
                        + estimatedSpaceNeeded + " bytes but we only have " + freeSpace);
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    private static void checkAppendStatus(String pvName, boolean status, String key, PartitionGranularity granularity) {
        if (status) {
            if (logger.isDebugEnabled()) {
                logger.debug("Successfully appended ETLInfo with key = " + key
                        + " for PV " + pvName + "itemInfo partitionGranularity = "
                        + granularity);
            }
        } else {
            logger.warn("Invalid status when processing ETLInfo with key = " + key
                    + " for PV " + pvName + "itemInfo partitionGranularity = "
                    + granularity);
        }
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

        long time4getETLStreams = 0;
        long time4checkSizes = 0;
        long time4prepareForNewPartition = 0;
        long time4appendToETLAppendData = 0;
        long time4commitETLAppendData = 0;
        long time4markForDeletion = 0;
        long time4runPostProcessors = 0;
        long time4executePostETLTasks = 0;

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

            StorageMetrics destMetrics = null;
            if (curETLDest instanceof StorageMetrics) {
                destMetrics = (StorageMetrics) curETLDest;
            }

            // Process each ETLInfo element in the list of ETLInfo
            // elements containing information about event streams
            // (e.g., files) ready for ETL for the current time and
            // PV name being processed.
            long time1 = System.currentTimeMillis();
            long totalSrcBytes = 0;
            List<ETLInfo> etlInfoList = curETLSource.getETLStreams(pvName, processingTime, etlContext);
            time4getETLStreams = time4getETLStreams + System.currentTimeMillis() - time1;
            if (etlInfoList != null && !etlInfoList.isEmpty()) {
                List<ETLInfo> movedList = new LinkedList<>();

                ProcessInfoListResult processInfoListResult = processETLInfoList(
                        etlInfoList,
                        pvName,
                        totalSrcBytes,
                        destMetrics,
                        movedList,
                        time4checkSizes,
                        curETLDest,
                        etlContext,
                        time4prepareForNewPartition,
                        time4appendToETLAppendData);

                // Concatenate any append data for the current ETLDest
                // destination to this destination.
                try {
                    long time7 = System.currentTimeMillis();
                    boolean commitSuccessful = curETLDest.commitETLAppendData(pvName, etlContext);
                    time4commitETLAppendData = time4commitETLAppendData + System.currentTimeMillis() - time7;

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

                try {
                    long time5 = System.currentTimeMillis();
                    curETLDest.runPostProcessors(pvName, lookupItem.getDbrType(), etlContext);
                    time4runPostProcessors = time4runPostProcessors + System.currentTimeMillis() - time5;
                } catch (Exception e) {
                    logger.error("Exception running post processors for pv " + pvName, e);
                }
                logger.debug("Executing post ETL tasks for this run");
                long time6 = System.currentTimeMillis();
                etlContext.executePostETLTasks();
                time4executePostETLTasks = time4executePostETLTasks + System.currentTimeMillis() - time6;
                logger.debug("Done executing post ETL tasks for this run");

                long pvETLEndEpochMilliSeconds = TimeUtils.getCurrentEpochMilliSeconds();
                lookupItem.addETLDurationInMillis(pvETLStartEpochMilliSeconds, pvETLEndEpochMilliSeconds);
                lookupItem.addInfoAboutDetailedTime(
                        time4getETLStreams,
                        processInfoListResult.time4checkSizes(),
                        processInfoListResult.time4prepareForNewPartition(),
                        processInfoListResult.time4appendToETLAppendData(),
                        time4commitETLAppendData,
                        time4markForDeletion,
                        time4runPostProcessors,
                        time4executePostETLTasks,
                        processInfoListResult.totalSrcBytes());
            } else {
                logger.debug("There were no ETL streams when running ETL for " + jobDesc);
            }
        } catch (IOException ex) {
            logger.error("IOException processing ETL for pv " + lookupItem.getPvName(), ex);
        } finally {
            currentlyRunning = false;
        }
    }

    private ProcessInfoListResult processETLInfoList(
            List<ETLInfo> etlInfoList,
            String pvName,
            long totalSrcBytes,
            StorageMetrics destMetrics,
            List<ETLInfo> movedList,
            long time4checkSizes,
            ETLDest curETLDest,
            ETLContext etlContext,
            long time4prepareForNewPartition,
            long time4appendToETLAppendData)
            throws IOException {
        for (ETLInfo infoItem : etlInfoList) {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing ETLInfo with key = " + infoItem.getKey() + " for PV " + pvName
                        + "itemInfo partitionGranularity = "
                        + infoItem.getGranularity().toString() + " and size " + infoItem.getSize());
            }

            long checkSzStart = System.currentTimeMillis();
            long sizeOfSrcStream = infoItem.getSize();
            totalSrcBytes += sizeOfSrcStream;
            if (notEnoughFreeSpace(sizeOfSrcStream, destMetrics, this.lookupItem, infoItem.getKey(), pvName)) {
                if (deleteSrcStreamWhenOutOfSpace(
                        List.of(infoItem), this.lookupItem.getOutOfSpaceHandling(), movedList, lookupItem, pvName)) {
                    continue;
                } else {
                    break;
                }
            }
            long checkSzEnd = System.currentTimeMillis();
            time4checkSizes = time4checkSizes + (checkSzEnd - checkSzStart);

            try (EventStream stream = infoItem.getEv()) {
                long time2 = System.currentTimeMillis();
                time4prepareForNewPartition = time4prepareForNewPartition + System.currentTimeMillis() - time2;
                long time3 = System.currentTimeMillis();
                boolean status = curETLDest.appendToETLAppendData(pvName, stream, etlContext);
                movedList.add(infoItem);
                time4appendToETLAppendData = time4appendToETLAppendData + System.currentTimeMillis() - time3;
                checkAppendStatus(pvName, status, infoItem.getKey(), infoItem.getGranularity());
            } catch (IOException ex) {
                // TODO What do we do in the case of exceptions? Do we remove the source still? Do we stop the
                // engine from recording this PV?
                logger.error("Exception processing " + infoItem.getKey(), ex);
            }
        }
        return new ProcessInfoListResult(
                time4checkSizes, time4prepareForNewPartition, time4appendToETLAppendData, totalSrcBytes);
    }

    private boolean deleteSrcStreamWhenOutOfSpace(
            List<ETLInfo> infoItems,
            OutOfSpaceHandling outOfSpaceHandling,
            List<ETLInfo> movedList,
            ETLPVLookupItems lookupItem,
            String pvName) {

        if (outOfSpaceHandling == OutOfSpaceHandling.DELETE_SRC_STREAMS_WHEN_OUT_OF_SPACE) {
            logger.error("Not enough space on dest. Deleting src streams "
                    + infoItems.stream().map(ETLInfo::getKey).toList());
            movedList.addAll(infoItems);
            lookupItem.outOfSpaceChunkDeleted();
            return true;
        } else if (outOfSpaceHandling == OutOfSpaceHandling.SKIP_ETL_WHEN_OUT_OF_SPACE) {
            logger.warn("Not enough space on dest. Skipping ETL this time for pv " + pvName);
            return false;
        } else {
            // By default, we use the DELETE_SRC_STREAMS_IF_FIRST_DEST_WHEN_OUT_OF_SPACE...
            if (lookupItem.getLifetimeorder() == 0) {
                logger.error("Not enough space on dest. Deleting src stream "
                        + infoItems.stream().map(ETLInfo::getKey).toList());
                movedList.addAll(infoItems);
                lookupItem.outOfSpaceChunkDeleted();
                return true;
            } else {
                logger.warn("Not enough space on dest. Skipping ETL this time for pv " + pvName);
                return false;
            }
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

    private record ProcessInfoListResult(
            long time4checkSizes,
            long time4prepareForNewPartition,
            long time4appendToETLAppendData,
            long totalSrcBytes) {}
}
