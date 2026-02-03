package org.epics.archiverappliance.etl.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
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
 * An ETLJob does the actual work of moving data from one data source to another.
 * This is a runnable, we expect to execute this on some executor.
 * @author mshankar
 *
 */
public class ETLJob implements Runnable {
    private static final Logger logger = LogManager.getLogger(ETLJob.class.getName());
    private final ETLStage etlStage;
    private final Instant runAsIfAtTime;
    private final ConfigService configService;
    private final boolean allData;

    /**
     *
     * @param etlStage    ETLStage
     * @param runAsIfAtTime Instant
     */
    public ETLJob(ETLStage etlStage, Instant runAsIfAtTime, ConfigService configService) {
        this.etlStage = etlStage;
        this.runAsIfAtTime = runAsIfAtTime;
        this.configService = configService;
        this.allData = false;
    }
    /**
     *
     * @param etlStage    ETLStage
     * @param runAsIfAtTime Instant
     */
    public ETLJob(ETLStage etlStage, Instant runAsIfAtTime, ConfigService configService, boolean allData) {
        this.etlStage = etlStage;
        this.runAsIfAtTime = runAsIfAtTime;
        this.configService = configService;
        this.allData = allData;
    }

    @Override
    public void run() {
        try {
            this.processETL(runAsIfAtTime);
        } catch (Exception e) {
            logger.error("Exception processing ETL for " + etlStage.toString(), e);
            this.etlStage.setExceptionFromLastRun(e);
        }
    }

    /**
     * Main ETL algorithm.
     *
     * @param processingTime Typically we'd use TimeUtils.now() for this. However, we can also run as if it's a certain
     *                       time by using this parameter.
     */
    public void processETL(Instant processingTime) throws IOException {
        String pvName = etlStage.getPvName();
        String jobDesc = etlStage.toString();
        if (this.etlStage.isCurrentlyRunning()) {
            logger.error("The previous ETL job (" + jobDesc + ") that began at "
                    + TimeUtils.convertToHumanReadableString(this.etlStage.getLastETLStart())
                    + " is still running");
            return;
        }

        String namedFlagForSkippingDataSource =
                "SKIP_" + this.etlStage.getETLDest().getName() + "_FOR_ETL";
        boolean skipETLForThisDest = this.configService.getNamedFlag(namedFlagForSkippingDataSource);
        if (skipETLForThisDest) {
            logger.warn(
                    "Skipping ETL for dest {} for PV {} as the flag {} is set to true",
                    this.etlStage.getETLDest().getName(),
                    this.etlStage.getPvName(),
                    namedFlagForSkippingDataSource);
            return;
        }

        if (this.configService.getETLLookup().getIsRunningInsideUnitTests() || this.allData) {
            // Skip the check for times...
        } else {
            if (processingTime.isBefore(this.etlStage.getNextETLStart())) {
                logger.debug(
                        "Too early {} to trigger this stage for PV {} from {} to {}. Next job at {}",
                        TimeUtils.convertToHumanReadableString(processingTime),
                        etlStage.getPvName(),
                        etlStage.getETLSource().getName(),
                        etlStage.getETLDest().getName(),
                        TimeUtils.convertToHumanReadableString(this.etlStage.getNextETLStart()));
                return;
            }
        }

        // We create a brand new context for each run.
        try (ETLContext etlContext = new ETLContext()) {
            this.etlStage.beginRunning();

            long pvETLStartEpochMilliSeconds = TimeUtils.getCurrentEpochMilliSeconds();

            if (logger.isDebugEnabled()) {
                logger.debug("Processing ETL for pv " + etlStage.getPvName() + " from "
                        + etlStage.getETLSource().getDescription()
                        + etlStage.getETLDest().getDescription() + " as if it is "
                        + TimeUtils.convertToHumanReadableString(processingTime) + " using all data " + allData);
            }

            ETLSource curETLSource = etlStage.getETLSource();
            ETLDest curETLDest = etlStage.getETLDest();
            assert (curETLSource != null);
            assert (curETLDest != null);

            // Process each ETLInfo element in the list of ETLInfo
            // elements containing information about event streams
            // (e.g., files) ready for ETL for the current time and
            // PV name being processed.
            long time1 = System.currentTimeMillis();

            long time4getETLStreams = 0;
            List<ETLInfo> etlInfoList = !allData
                    ? curETLSource.getETLStreams(pvName, processingTime, etlContext)
                    : curETLSource.getAllStreams(pvName, etlContext);
            time4getETLStreams = time4getETLStreams + System.currentTimeMillis() - time1;
            if (etlInfoList != null && !etlInfoList.isEmpty()) {
                List<ETLInfo> movedList = new LinkedList<>();

                ETLInfoListStatistics etlInfoListStatistics = curETLDest
                        .etlInfoListProcessor(curETLSource)
                        .process(etlInfoList, pvName, movedList, this.etlStage, etlContext);

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
                    curETLDest.runPostProcessors(pvName, etlStage.getDbrType(), etlContext);
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
                etlStage.addETLDurationInMillis(pvETLStartEpochMilliSeconds, pvETLEndEpochMilliSeconds);
                etlStage.addInfoAboutDetailedTime(
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
            logger.error("IOException processing ETL for pv " + etlStage.getPvName(), ex);
            // Maybe we should throw this exception here?
        } finally {
            this.etlStage.doneRunning();
        }
    }
}
