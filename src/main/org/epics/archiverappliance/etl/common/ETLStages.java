package org.epics.archiverappliance.etl.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.StorageMetrics;

import java.io.Console;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;

/*
 * All the ETLStage's for a PV
 */
public class ETLStages implements Runnable {
    private static final Logger logger = LogManager.getLogger();
    private final String pvName;
    private final ExecutorService theWorker;
    private final LinkedList<ETLStage> etlStages = new LinkedList<ETLStage>();
    private ScheduledFuture<?> cancellingFuture;
    private final ConfigService configService;

    public ETLStages(String pvName, ExecutorService theWorker, ConfigService configService) {
        this.pvName = pvName;
        this.theWorker = theWorker;
        this.configService = configService;
    }

    public void addStage(ETLStage stage) {
        // We schedule using the source granularity or a shift (8 hours) whichever is smaller.
        long delaybetweenETLJobsInSecs =
                Math.min(stage.getETLSource().getPartitionGranularity().getApproxSecondsPerChunk(), 8 * 60 * 60);
        if (!this.etlStages.isEmpty()) {
            // With the GzTar plugin, the latter stages may have a smaller partition granularutity than the source.
            // No point kicking off ETL until the source has done its bit.
            delaybetweenETLJobsInSecs = Math.max(
                    delaybetweenETLJobsInSecs,
                    this.etlStages.stream()
                            .map(ETLStage::getDelaybetweenETLJobsInSecs)
                            .max(Long::compareTo)
                            .get());
        }
        stage.setDelaybetweenETLJobsInSecs(delaybetweenETLJobsInSecs);

        this.etlStages.add(stage);
    }

    public String getPvName() {
        return pvName;
    }

    public List<ETLStage> getStages() {
        return Collections.unmodifiableList(this.etlStages);
    }

    public void setCancellingFuture(ScheduledFuture<?> cancellingFuture) {
        this.cancellingFuture = cancellingFuture;
    }

    public ScheduledFuture<?> getCancellingFuture() {
        return cancellingFuture;
    }

    /*
     * Get the smallest delay between ETL jobs for this PV.
     * This is the interval with which we add to the scheduler
     */
    public long getMinDelaybetweenETLJobs() {
        if (this.etlStages.isEmpty()) return 8 * 60 * 60;
        return this.etlStages.stream()
                .map(ETLStage::getDelaybetweenETLJobsInSecs)
                .min(Long::compareTo)
                .get();
    }

    public long getInitialDelay() {
        if (this.etlStages.isEmpty()) {
            logger.error("No ETL Stages defined for PV {}", this.pvName);
            return 10 * 365 * 24 * 60 * 60; // Somewhere in the distant future
        }
        long minDelay = this.etlStages.stream()
                .map(ETLStage::getInitialDelay)
                .min(Long::compareTo)
                .get();
        // We want to schedule the trigger just slightly after the minimum delay.
        minDelay = minDelay + 10;
        return minDelay;
    }

    @Override
    public void run() {
        this.runAsIfAtTime(Instant.now());
    }

    public void runAsIfAtTime(Instant runAsIfAtTime) {
        try {
            CompletableFuture<Void> f = null;
            for (ETLStage etlStage : this.etlStages) {
                f = (f == null)
                        ? CompletableFuture.runAsync(new ETLJob(etlStage, runAsIfAtTime, configService), this.theWorker)
                        : f.thenCompose((b) -> CompletableFuture.runAsync(
                                new ETLJob(etlStage, runAsIfAtTime, configService), this.theWorker));
            }
            if (f == null) {
                throw new IOException("Completable future is null");
            }
            f.get(); // Wait for the future to complete
        } catch (Exception ex) {
            logger.error("Exception running ETL Job for PV " + this.pvName, ex);
        }
    }

    public void cancelJob() {
        List<Runnable> consolidateTasks = new LinkedList<Runnable>();

        for (ETLStage etlStage : this.etlStages) {
            ScheduledFuture<?> cancellingFuture = etlStage.getCancellingFuture();
            if (cancellingFuture != null) {
                cancellingFuture.cancel(false);
            }

            if (etlStage.getETLSource().consolidateOnShutdown()) {
                logger.debug("Need to consolidate data from etl source "
                        + ((StoragePlugin) etlStage.getETLSource()).getName() + " for pv " + pvName
                        + " for storage " + ((StorageMetrics) etlStage.getETLDest()).getName());
                Instant oneYearLaterTimeStamp = TimeUtils.convertFromEpochSeconds(
                        TimeUtils.getCurrentEpochSeconds()
                                + 365L * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                        0);
                try {
                    consolidateTasks.add(new ETLJob(etlStage, oneYearLaterTimeStamp, configService));
                } catch (Exception ex) {
                    logger.error("Exception running ETL Job for PV " + pvName, ex);
                }
            }
        }
        
        CompletableFuture.runAsync(new Runnable(){
            @Override
            public void run() {
                for (Runnable consolidateTask : consolidateTasks) {
                    try {
                        consolidateTask.run();
                    } catch (Exception ex) {
                        logger.error("Exception running ETL Job for PV " + pvName, ex);
                    }
                }
            }
        });
    }

    public Event getLatestEventFromDataStores() throws IOException {
        try (BasicContext context = new BasicContext()) {
            for (ETLStage etlEntry : this.etlStages) {
                Event e = etlEntry.getETLDest().getLastKnownEvent(context, pvName);
                if (e != null) return e;
            }
        }
        return null;
    }

    public Exception getAnyExceptionFromLastRun() {
        for (ETLStage etlEntry : this.etlStages) {
            if (etlEntry.getExceptionFromLastRun() != null) {
                return etlEntry.getExceptionFromLastRun();
            }
        }
        return null;
    }
}
