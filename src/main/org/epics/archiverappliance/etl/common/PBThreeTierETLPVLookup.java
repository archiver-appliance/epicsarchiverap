/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl.common;

import com.google.common.eventbus.Subscribe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.PVTypeInfoEvent;
import org.epics.archiverappliance.config.PVTypeInfoEvent.ChangeType;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.etl.StorageMetrics;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Holds runtime state for ETL.
 * For now, gets all of the info from PVTypeInfo.
 *
 * @author rdh
 * @version 4-Jun-2012, Luofeng Li:added codes to create one ETL thread for each ETL
 */
public final class PBThreeTierETLPVLookup {
    private static final Logger logger = LogManager.getLogger(PBThreeTierETLPVLookup.class.getName());
    private static final Logger configlogger = LogManager.getLogger("config." + PBThreeTierETLPVLookup.class.getName());

    private ConfigService configService = null;

    public static boolean isRunningInsideUnitTests = false;

    /**
     * One scheduled thread pool executor to schedule them all.
     * All the work is now done in virtual threads.
     * For now, a threadpool of size 1 for the scheduleWorker should be adequate; not much is done in this thread.
     * Most of the work happens in the theWorker virtual thread
     */
    private ScheduledThreadPoolExecutor scheduleWorker =
            new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "ETL main scheduler thread "));

    private ExecutorService theWorker = Executors.newVirtualThreadPerTaskExecutor();

    /*
     * ETLStages for a PV are collected here.
     */
    private final ConcurrentHashMap<String, ETLStages> etlStagesForPVs = new ConcurrentHashMap<String, ETLStages>();

    /*
     * Collect appliance level metrics based on storage name
     */
    private final ETLMetrics applianceMetrics = new ETLMetrics();

    public PBThreeTierETLPVLookup(ConfigService configService) {
        this.configService = configService;
        configService.addShutdownHook(new ETLShutdownThread(this));
    }

    /**
     * Initialize the ETL background scheduled executors and create the runtime state for various ETL components.
     */
    public void postStartup() {
        configlogger.info(
                "Beginning ETL post startup");
        configService.getEventBus().register(this);
        this.startETLJobsOnStartup();
        configlogger.debug("Done initializing ETL jobs on post startup.");
    }

    @Subscribe
    public void pvTypeInfoChanged(PVTypeInfoEvent event) {
        String pvName = event.getPvName();
        PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
        logger.debug(
                "Received PVTypeInfo changed event for {} ChangeType: {} Paused: {} Appliance: {}",
                pvName,
                event.getChangeType(),
                typeInfo.isPaused(),
                typeInfo.getApplianceIdentity());
        if (event.getChangeType() == ChangeType.TYPEINFO_DELETED
                || typeInfo.isPaused()
                || !typeInfo.getApplianceIdentity()
                        .equals(configService.getMyApplianceInfo().getIdentity())) {
            logger.debug("Deleting ETL jobs for {} based on PVTypeInfo change ", pvName);
            deleteETLJobs(pvName);
        } else if (!typeInfo.isPaused()
                && typeInfo.getApplianceIdentity()
                        .equals(configService.getMyApplianceInfo().getIdentity())) {
            logger.debug("Adding ETL jobs for {} based on PVTypeInfo change", pvName);
            addETLJobs(pvName, typeInfo);
        }
    }

    private void startETLJobsOnStartup() {
        try {
            Set<String> pVsForThisAppliance = configService.getPVsForThisAppliance();
            if (pVsForThisAppliance != null) {
                for (String pvName : pVsForThisAppliance) {
                    if (!etlStagesForPVs.containsKey(pvName)) {
                        PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
                        if (!typeInfo.isPaused()) {
                            addETLJobs(pvName, typeInfo);
                        } else {
                            logger.info("Skipping adding ETL jobs for paused PV " + pvName);
                        }
                    }
                }
            } else {
                configlogger.info("There are no PVs on this appliance yet");
            }
        } catch (Throwable t) {
            configlogger.error("Excepting syncing ETL jobs with config service", t);
        }
    }

    /**
     * Add jobs for each of the ETL lifetime transitions.
     * @param pvName
     * @param typeInfo
     */
    private void addETLJobs(String pvName, PVTypeInfo typeInfo) {
        if (!etlStagesForPVs.containsKey(pvName)) {
            // Precompute the chunkKey when we have access to the typeInfo. The keyConverter should store it in its
            // cache.
            String chunkKey = configService.getPVNameToKeyConverter().convertPVNameToKey(pvName);
            logger.debug("Adding etl jobs for pv " + pvName + " for chunkkey " + chunkKey);
            String[] dataSources = typeInfo.getDataStores();
            if (dataSources == null || dataSources.length < 2) {
                logger.warn("Skipping adding PV to ETL as it has less than 2 datasources" + pvName);
                return;
            }

            ETLStages etlStages = new ETLStages(pvName, theWorker);
            etlStagesForPVs.put(pvName, etlStages);

            for (int etllifetimeid = 0; etllifetimeid < dataSources.length - 1; etllifetimeid++) {
                try {
                    String sourceStr = dataSources[etllifetimeid];
                    ETLSource etlSource = StoragePluginURLParser.parseETLSource(sourceStr, configService);
                    String destStr = dataSources[etllifetimeid + 1];
                    ETLDest etlDest = StoragePluginURLParser.parseETLDest(destStr, configService);
                    applianceMetrics.createMetricIfNoExists(etlSource.getName());
                    applianceMetrics.createMetricIfNoExists(etlDest.getName());
                    ETLStage etlStage = new ETLStage(
                            pvName,
                            typeInfo.getDBRType(),
                            etlSource,
                            etlDest,
                            etllifetimeid,
                            applianceMetrics.get(etlDest.getName()),
                            determineOutOfSpaceHandling(configService));
                    if (etlDest instanceof StorageMetrics) {
                        // At least on some of the test machines, checking free space seems to take the longest time. In
                        // this, getting the fileStore seems to take the longest time.
                        // The plainPB plugin caches the fileStore; so we make a call once when adding to initialize
                        // this upfront.
                        ((StorageMetrics) etlDest).getUsableSpace(etlStage.getMetricsForLifetime());
                    }
                    etlStages.addStage(etlStage);
                } catch (Throwable t) {
                    logger.error("Exception get  for pv " + pvName, t);
                }
            }
            // We schedule the ETLStages which then runs each ETLStage in a virtual thread.
            if (!scheduleWorker.isShutdown()) {
                ScheduledFuture<?> cancellingFuture = scheduleWorker.scheduleWithFixedDelay(
                        etlStages,
                        etlStages.getInitialDelay(),
                        etlStages.getMinDelaybetweenETLJobs(),
                        TimeUnit.SECONDS);
                etlStages.setCancellingFuture(cancellingFuture);
                logger.debug("Scheduled all ETL Stages for " + pvName
                        + " with initial delay of " + etlStages.getInitialDelay() + " and between job delay of "
                        + etlStages.getMinDelaybetweenETLJobs());
            } else {
                logger.error("ETL thread pool executor is already shutdown. Should only happen in tests");
            }
        } else {
            logger.debug("Not adding ETL jobs for PV already in pvsForWhomWeHaveAddedETLJobs " + pvName);
        }
    }

    /**
     * Cancel the ETL jobs for each of the ETL lifetime transitions and also remove from internal structures.
     * @param pvName The name of PV.
     */
    public void deleteETLJobs(String pvName) {
        if (etlStagesForPVs.containsKey(pvName)) {
            logger.debug(
                    "deleting etl jobs for  pv " + pvName + " from the locally cached copy of pvs for this appliance");
            etlStagesForPVs.get(pvName).cancelJob();
            etlStagesForPVs.remove(pvName);
        } else {
            logger.debug("Not deleting ETL jobs for PV missing from pvsForWhomWeHaveAddedETLJobs " + pvName);
        }
    }

    /**
     * Get the internal state for all the ETL lifetime transitions for a pv
     * @param pvName The name of PV.
     * @return LinkedList  &emsp;
     */
    public ETLStages getETLStages(String pvName) {
        return etlStagesForPVs.get(pvName);
    }

    /**
     * Get the latest (last known) entry from the stores for this PV.
     * @param pvName The name of PV.
     * @return Event LatestEventFromDataStores
     * @throws IOException  &emsp;
     */
    public Event getLatestEventFromDataStores(String pvName) throws IOException {
        if (this.etlStagesForPVs.containsKey(pvName)) {
            return this.getETLStages(pvName).getLatestEventFromDataStores();
        }
        return null;
    }

    private static final class ETLShutdownThread implements Runnable {
        private PBThreeTierETLPVLookup theLookup = null;

        public ETLShutdownThread(PBThreeTierETLPVLookup theLookup) {
            this.theLookup = theLookup;
        }

        @Override
        public void run() {
            logger.debug("Shutting down ETL threads.");
            String message = "Exception shutting down ETL";
            try {
                theLookup.scheduleWorker.shutdown();
            } catch (Throwable t) {
                logger.error(message, t);
            }
            LinkedHashSet<String> pvNames = new LinkedHashSet<String>(theLookup.etlStagesForPVs.keySet());
            for (String pvName : pvNames) {
                try {
                    theLookup.deleteETLJobs(pvName);
                } catch (Throwable t) {
                    logger.error(message, t);
                }
            }
            try {
                theLookup.theWorker.shutdown();
            } catch (Throwable t) {
                logger.error(message, t);
            }
        }
    }

    public ETLMetrics getApplianceMetrics() {
        return applianceMetrics;
    }

    /**
     * Some unit tests want to run the ETL jobs manually; so we shut down the threads.
     * We should probably write a pausable thread pool executor
     * Use with care.
     */
    public void manualControlForUnitTests() {
        logger.error("Shutting down ETL for unit tests...");
        this.scheduleWorker.shutdownNow();
        isRunningInsideUnitTests = true;
    }

    public static OutOfSpaceHandling determineOutOfSpaceHandling(ConfigService configService) {
        String outOfSpaceHandler = configService
                .getInstallationProperties()
                .getProperty(
                        "org.epics.archiverappliance.etl.common.OutOfSpaceHandling",
                        OutOfSpaceHandling.DELETE_SRC_STREAMS_IF_FIRST_DEST_WHEN_OUT_OF_SPACE.toString());
        return OutOfSpaceHandling.valueOf(outOfSpaceHandler);
    }

    public void addETLJobsForUnitTests(String pvName, PVTypeInfo typeInfo) {
        logger.warn("addETLJobsForUnitTests This message should only be called from the unit tests.");
        addETLJobs(pvName, typeInfo);
    }
}
