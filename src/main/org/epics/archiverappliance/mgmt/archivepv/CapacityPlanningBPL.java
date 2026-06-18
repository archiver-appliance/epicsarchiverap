/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *****
 */
package org.epics.archiverappliance.mgmt.archivepv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceAggregateInfo;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.etl.StorageMetrics;
import org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningData.CPStaticData;
import org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningData.ETLMetrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Primary capacity planning logic for the default capacity planning as outlined in the doc.
 * @author luofeng
 *
 */
public class CapacityPlanningBPL {
    private static Logger logger = LogManager.getLogger(CapacityPlanningBPL.class.getName());
    private static boolean isDebug = false;
    /**
     * The maximum percentage of capacity (storage, writer time and ETL time) that an appliance may
     * use before it is considered unavailable for new PVs.
     */
    private static float percentageLimitation = 80;

    private static Logger configlogger = LogManager.getLogger("config." + CapacityPlanningBPL.class.getName());

    /***
     * get the appliance for this pv.
     * this method implements the capacity planning
     * @param pvName the name of the pv.
     * @param configService the local configService
     * @param pvTypeInfo the pvTypeInfo of this pv.
     * @return  the ApplianceInfo of the appliance which this pv will be added
     * @throws IOException  error occurs during the capacity planning.
     * in this case, this method will return the ApplianceInfo of the local  appliance
     */
    public static ApplianceInfo pickApplianceForPV(String pvName, ConfigService configService, PVTypeInfo pvTypeInfo)
            throws IOException {

        try {
            String[] dataStores = pvTypeInfo.getDataStores();

            HashMap<String, Integer> destinationPartitionSeconds = new HashMap<String, Integer>();
            for (String dataStore : dataStores) {
                ETLSource etlSource = StoragePluginURLParser.parseETLSource(dataStore, configService);
                if (etlSource == null) {
                    logger.debug("the ETLSource of " + dataStore + " is null");
                    if (isDebug) logger.error("the ETLSource of " + dataStore + " is null");
                    continue;
                }

                ETLDest etlDest = StoragePluginURLParser.parseETLDest(dataStore, configService);
                if (etlDest instanceof StorageMetrics) {
                    int partitionSecond = etlSource.getPartitionGranularity().getApproxSecondsPerChunk();
                    String destinationName = ((StorageMetrics) etlDest).getName();
                    destinationPartitionSeconds.put(destinationName, partitionSecond);
                    if (isDebug) logger.error(destinationName + " is added into destinationPartitionSeconds");
                }
            }

            if (isDebug) logger.error("destinationPartitionSeconds.size()1=" + destinationPartitionSeconds.size());

            float pvStorageRate = pvTypeInfo.getComputedStorageRate();

            CPStaticData cpStaticData = CapacityPlanningData.getMetricsForAppliances(configService);
            ConcurrentHashMap<ApplianceInfo, CapacityPlanningData> appliances = cpStaticData.cpApplianceMetrics;

            HashMap<ApplianceInfo, CapacityPlanningData> nullETLMetricsAppliances =
                    new HashMap<ApplianceInfo, CapacityPlanningData>();
            for (Entry<ApplianceInfo, CapacityPlanningData> nullCheckEntry : appliances.entrySet()) {

                CapacityPlanningData nullCheckMetrics = nullCheckEntry.getValue();

                ApplianceInfo nullCheckAppliance = nullCheckEntry.getKey();

                // check whether the appliance has all the destinations of the PV being added

                ConcurrentHashMap<String, ETLMetrics> nullCheckEtlMetrics = nullCheckMetrics.getEtlMetrics();
                if (nullCheckEtlMetrics.size() == 0) {

                    logger.debug(nullCheckAppliance.getIdentity() + " has no elements in the ETLMetrics");
                    if (isDebug) logger.error(nullCheckAppliance.getIdentity() + " has no elements in the ETLMetrics");
                    nullETLMetricsAppliances.put(nullCheckAppliance, nullCheckMetrics);
                }
            }

            if (nullETLMetricsAppliances.size() != 0) {
                // compare the total PV event rate after adding the PV and return the best appliance

                Iterator<Entry<ApplianceInfo, CapacityPlanningData>> applianceIterator =
                        appliances.entrySet().iterator();
                ArrayList<ApplianceAndTotalRate> applianceTotalRates = new ArrayList<ApplianceAndTotalRate>();
                while (applianceIterator.hasNext()) {

                    Entry<ApplianceInfo, CapacityPlanningData> rateEntry =
                            (Entry<ApplianceInfo, CapacityPlanningData>) applianceIterator.next();

                    ApplianceInfo rateApplianceInfo = rateEntry.getKey();

                    CapacityPlanningData rateMetrics = rateEntry.getValue();
                    ApplianceAggregateInfo rateAggregateInfo =
                            rateMetrics.getApplianceAggregateDifferenceFromLastFetch(configService);

                    float totalDataRate = (float) rateAggregateInfo.getTotalStorageRate()
                            + rateEntry.getValue().getCurrentTotalStorageRate();

                    applianceTotalRates.add(new ApplianceAndTotalRate(rateApplianceInfo, totalDataRate));
                }

                ApplianceAndTotalRate bestAppliance = null;
                for (ApplianceAndTotalRate candidate : applianceTotalRates) {
                    if (bestAppliance == null) bestAppliance = candidate;
                    if (candidate.getTotalDataRate() < bestAppliance.getTotalDataRate()) {
                        bestAppliance = candidate;
                    }
                }

                if (bestAppliance == null) {
                    throw (new Exception("bestAppliance is null"));
                } else {
                    logger.debug(
                            "the ETLMetrics in one appliance of the cluster has no elements, so the capacity planning just uses the dataRate!");
                    if (isDebug)
                        logger.error(
                                "the ETLMetrics in one appliance of the cluster has no elements, so the capacity planning just uses the dataRate!");
                    return bestAppliance.getAppInfo();
                }
            }

            // For each appliance, estimate the storage the PV would add and compare it against the
            // storage, writer time and ETL time available on that appliance, marking the appliance
            // unavailable if any limit would be exceeded.
            for (Entry<ApplianceInfo, CapacityPlanningData> entry : appliances.entrySet()) {
                CapacityPlanningData cpMetrics = entry.getValue();

                cpMetrics.setAvailable(true);

                // check whether the appliance has all the destinations of the PV being added

                ConcurrentHashMap<String, ETLMetrics> etlMetrics = cpMetrics.getEtlMetrics();

                ApplianceAggregateInfo aggregateInfo =
                        cpMetrics.getApplianceAggregateDifferenceFromLastFetch(configService);
                float totalDataRate = cpMetrics.getCurrentTotalStorageRate();

                float totalDataRateForPvAdded = (float) aggregateInfo.getTotalStorageRate();

                HashMap<String, Long> estimatedStorageByDestination = aggregateInfo.getTotalStorageImpact();

                for (Entry<String, Integer> addingEntry : destinationPartitionSeconds.entrySet()) {
                    String addingDestinationName = addingEntry.getKey();
                    ETLMetrics addingEtlMetrics = etlMetrics.get(addingDestinationName);
                    if (addingEtlMetrics != null) {
                        addingEtlMetrics.estimateStoragePVadded =
                                estimatedStorageByDestination.get(addingDestinationName);
                    }
                }

                for (Entry<String, Long> estimateEntry : estimatedStorageByDestination.entrySet()) {
                    Long totalEstimateStorage = estimateEntry.getValue();
                    String estimateDestinationName = estimateEntry.getKey();

                    Integer tempSeconds = destinationPartitionSeconds.get(estimateDestinationName);
                    if (tempSeconds != null) {
                        totalEstimateStorage = totalEstimateStorage + (long) (pvStorageRate * tempSeconds);
                    }
                }

                // The estimated storage has been computed above; now compare it against the
                // available storage for each destination on this appliance.
                for (Entry<String, ETLMetrics> availabilityEntry : etlMetrics.entrySet()) {
                    ETLMetrics availabilityEtlMetrics = availabilityEntry.getValue();
                    String availabilityDestinationName = availabilityEtlMetrics.identity;
                    long availableStorage = availabilityEtlMetrics.etlStorageAvailable;

                    Integer availabilitySeconds = destinationPartitionSeconds.get(availabilityDestinationName);

                    if (availabilitySeconds != null) {
                        long estimateStorageSize = estimatedStorageByDestination.get(availabilityDestinationName);
                        if (estimateStorageSize > availableStorage) {
                            cpMetrics.setAvailable(false);
                            configlogger.error("There is not enough storage to accommodate " + pvName + " for "
                                    + availabilityDestinationName + ". Estimated storage for " + pvName + " is "
                                    + estimateStorageSize + " while available storage is " + availableStorage);

                            if (isDebug) {

                                logger.error(availabilityDestinationName + ":  testestimateStorageSize7777="
                                        + estimateStorageSize + ",availableStorage=" + availableStorage);
                                logger.error("testestimateStorageSize7777>availableStorage");
                            }
                        }

                    } else {

                        if (isDebug)
                            logger.error("availabilitySeconds=null, and availabilityDestinationName="
                                    + availabilityDestinationName);
                    }

                    if (!cpMetrics.isAvailable()) {

                        if (isDebug) logger.error("testtempCapacityPlanningMetrics.isAvailable()---1---");
                        break;
                    }
                } // end for
                if (!cpMetrics.isAvailable()) {
                    if (isDebug) logger.error("testtempCapacityPlanningMetrics.isAvailable()---2---");
                    continue;
                }

                float currentUsedWriterPercentage =
                        cpMetrics.getEngineWriteThreadUsage(PVTypeInfo.getSecondsToBuffer(configService));
                if (currentUsedWriterPercentage > CapacityPlanningBPL.percentageLimitation) {
                    cpMetrics.setAvailable(false);
                    if (isDebug) logger.error("test exceed the limitation");
                    configlogger.error("There is not enough time left for writer to write " + pvName
                            + " into short term storage. Estimated writing percentage for "
                            + pvName + " is "
                            + currentUsedWriterPercentage + " while the percentage limitation is "
                            + CapacityPlanningBPL.percentageLimitation);
                }
                float percentageForWriter =
                        currentUsedWriterPercentage * (totalDataRateForPvAdded + totalDataRate) / (totalDataRate);
                cpMetrics.setPercentageTimeForWriter(percentageForWriter);
                if (percentageForWriter > CapacityPlanningBPL.percentageLimitation) {
                    cpMetrics.setAvailable(false);

                    configlogger.error("There is not enough time left for writer to write " + pvName
                            + " into short term storage. Estimated writing percentage for "
                            + pvName + " is "
                            + percentageForWriter + " while the percentage limitation is "
                            + CapacityPlanningBPL.percentageLimitation);
                    if (isDebug) logger.error("testpercentageForWriter>CapacityPlanningBPL.percentageLimitation");
                }

                // Normalize ETL time:
                //   ETL time after the PV is added = ((estimateStorageSize + usedStorage) / usedStorage) * etlTimeTaken
                //   estimateStorageSize = sum(pvAddedDataRate * partitionTime)
                for (Entry<String, ETLMetrics> etlEntry : etlMetrics.entrySet()) {
                    ETLMetrics etlMetric = etlEntry.getValue();
                    long storageUsed = etlMetric.totalSpace - etlMetric.etlStorageAvailable;
                    double etlTimePercentage = etlMetric.etlTimeTaken;
                    if (etlTimePercentage > CapacityPlanningBPL.percentageLimitation) {
                        cpMetrics.setAvailable(false);
                        configlogger.error("There is not enough time left for ETL to write " + pvName + " into "
                                + etlMetric.identity + ". Estimated percentage time is " + etlTimePercentage
                                + " while the percentage limitation is " + CapacityPlanningBPL.percentageLimitation);
                        if (isDebug) logger.error("testtemptempETLTimeTaken>CapacityPlanningBPL.percentageLimitation");
                    }

                    double estimatedEtlTimePercentage = etlTimePercentage
                            * (double) (storageUsed + etlMetric.estimateStoragePVadded)
                            / (double) storageUsed;
                    etlMetric.estimateETLtimePercentageAfterPVadded = estimatedEtlTimePercentage;

                    if (estimatedEtlTimePercentage > CapacityPlanningBPL.percentageLimitation) {
                        if (isDebug)
                            logger.error(
                                    "testtempEstimateETLtimePercentageAfterPVadded>CapacityPlanningBPL.percentageLimitation");
                        cpMetrics.setAvailable(false);
                        configlogger.error("There is not enough time left for ETL to write " + pvName + " into "
                                + etlMetric.identity + ". Estimated percentage time is "
                                + estimatedEtlTimePercentage + " while the percentage limitation is "
                                + CapacityPlanningBPL.percentageLimitation);
                    }
                }
                if (isDebug) logger.error(entry.getKey().getIdentity() + " is called");
            } // end for

            // Normalization is done. Now find the highest average percentage across all factors
            // (writer and each ETL destination), starting with the average writer time percentage.
            Iterator<Entry<ApplianceInfo, CapacityPlanningData>> availableApplianceIterator =
                    appliances.entrySet().iterator();
            float averagePercentageWriter = 0;

            int availableAppliancesNum = 0;

            HashMap<String, Double> averagePercentageETL = new HashMap<String, Double>();

            if (isDebug) logger.error("destinationPartitionSeconds.size()2=" + destinationPartitionSeconds.size());
            while (availableApplianceIterator.hasNext()) {

                // iterate over all appliances
                Entry<ApplianceInfo, CapacityPlanningData> averageEntry =
                        (Entry<ApplianceInfo, CapacityPlanningData>) availableApplianceIterator.next();
                CapacityPlanningData averageMetrics = averageEntry.getValue();
                if (!averageMetrics.isAvailable()) {
                    if (isDebug) logger.error(averageEntry.getKey().getIdentity() + " is not available");
                    continue;
                }
                availableAppliancesNum++;
                // compute writer time
                averagePercentageWriter += averageMetrics.getPercentageTimeForWriter();
                ConcurrentHashMap<String, ETLMetrics> etlMetricsForAppliance = averageMetrics.getEtlMetrics();

                if (isDebug) logger.error("etlMetricsForAppliance.size()=" + etlMetricsForAppliance.size());
                // compute ETL Time

                // accumulate ETL time across all ETL destinations in this appliance
                for (Entry<String, Integer> averageAddingEntry : destinationPartitionSeconds.entrySet()) {
                    String averageDestinationName = averageAddingEntry.getKey();
                    ETLMetrics averageEtlMetrics = etlMetricsForAppliance.get(averageDestinationName);
                    if (averageEtlMetrics != null) {
                        Double currentEtlSum = averagePercentageETL.get(averageDestinationName);
                        if (currentEtlSum == null) {
                            averagePercentageETL.put(
                                    averageDestinationName, averageEtlMetrics.estimateETLtimePercentageAfterPVadded);
                            if (isDebug) logger.error(averageDestinationName + " is null ");
                        } else {
                            averagePercentageETL.put(
                                    averageDestinationName,
                                    currentEtlSum + averageEtlMetrics.estimateETLtimePercentageAfterPVadded);
                            if (isDebug) logger.error(averageDestinationName + " is added ");
                        }
                    }
                }
            }

            ArrayList<NormalizationFactor> allNormalizationFactor = new ArrayList<NormalizationFactor>();
            // compute average.
            if (isDebug) logger.error("availableAppliancesNum=" + availableAppliancesNum);
            if (availableAppliancesNum == 0) {
                configlogger.error("there is no appliance available and use the local appliance as the default");
                return configService.getMyApplianceInfo();
            }
            averagePercentageWriter = averagePercentageWriter / availableAppliancesNum;
            allNormalizationFactor.add(new NormalizationFactor("writer", averagePercentageWriter));

            for (Entry<String, Double> etlAverageEntry : averagePercentageETL.entrySet()) {
                String etlDestinationName = etlAverageEntry.getKey();
                Double averageEtlPercentage = etlAverageEntry.getValue();
                averagePercentageETL.put(etlDestinationName, averageEtlPercentage / availableAppliancesNum);
                allNormalizationFactor.add(
                        new NormalizationFactor(etlDestinationName, averageEtlPercentage.floatValue()));
            }

            // pick the factor with the maximum average percentage
            NormalizationFactor resultFactor = new NormalizationFactor("tempFactor", 0);
            for (NormalizationFactor factor : allNormalizationFactor) {
                if (factor.getPercentage() >= resultFactor.getPercentage()) {
                    resultFactor = factor;
                }
            }
            if (resultFactor.getIdentify().equals("tempFactor")) {

                if (isDebug) logger.error("allNormalizationFactor.size()=" + allNormalizationFactor.size());
                configlogger.error("there are some error in computing the max (percentage)");
                configlogger.error(
                        "there is one error during capacity planning computing and use the local appliance as the default");
                return configService.getMyApplianceInfo();
            }
            logger.debug("the max (percentage) is :" + resultFactor.getIdentify() + ",value:"
                    + resultFactor.getPercentage());

            ApplianceInfo minResultApplianceInfo = null;
            String minIdentify = resultFactor.getIdentify();
            if (minIdentify.equals("writer")) {

                for (Entry<ApplianceInfo, CapacityPlanningData> applianceEntry : appliances.entrySet()) {
                    CapacityPlanningData candidateMetrics = applianceEntry.getValue();

                    ApplianceInfo candidateAppliance = applianceEntry.getKey();
                    if (!candidateMetrics.isAvailable()) continue;
                    if (minResultApplianceInfo == null) minResultApplianceInfo = candidateAppliance;

                    CapacityPlanningData minMetrics = appliances.get(minResultApplianceInfo);
                    float minPercentageTimeWriter = minMetrics.getPercentageTimeForWriter();
                    float candidateWriterPercentage = candidateMetrics.getPercentageTimeForWriter();
                    if (candidateWriterPercentage < minPercentageTimeWriter)
                        minResultApplianceInfo = candidateAppliance;
                }
            } else {

                for (Entry<ApplianceInfo, CapacityPlanningData> applianceEntry : appliances.entrySet()) {
                    CapacityPlanningData candidateMetrics = applianceEntry.getValue();

                    ApplianceInfo candidateAppliance = applianceEntry.getKey();
                    if (!candidateMetrics.isAvailable()) continue;
                    if (minResultApplianceInfo == null) minResultApplianceInfo = candidateAppliance;
                    CapacityPlanningData minMetrics = appliances.get(minResultApplianceInfo);

                    ConcurrentHashMap<String, ETLMetrics> minEtlMetrics = minMetrics.getEtlMetrics();

                    ETLMetrics minEtlMetric = minEtlMetrics.get(minIdentify);
                    double minEtlTimePercentage = minEtlMetric.estimateETLtimePercentageAfterPVadded;

                    ConcurrentHashMap<String, ETLMetrics> candidateEtlMetrics = candidateMetrics.getEtlMetrics();

                    ETLMetrics candidateEtlMetric = candidateEtlMetrics.get(minIdentify);
                    double candidateEtlTimePercentage = candidateEtlMetric.estimateETLtimePercentageAfterPVadded;
                    if (candidateEtlTimePercentage < minEtlTimePercentage) {
                        minResultApplianceInfo = candidateAppliance;
                    }
                }
            }

            if (minResultApplianceInfo == null) {
                logger.error(
                        "there is one error during capacity planning computing and use the local appliance as the default");
                return configService.getMyApplianceInfo();
            }
            return minResultApplianceInfo;

        } catch (Exception e) {
            logger.error("Exception during capacity planning, returning this appliance", e);
            return configService.getMyApplianceInfo();
        }
    }
}
