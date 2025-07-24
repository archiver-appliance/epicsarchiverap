package org.epics.archiverappliance.etl.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.reports.Details;
import org.epics.archiverappliance.config.ConfigService;

import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ETLStageDetails implements Details {
    private static final Logger logger = LogManager.getLogger();

    private final String pvName;

    public ETLStageDetails(String pvName) {
        this.pvName = pvName;
    }

    @Override
    public ConfigService.WAR_FILE source() {
        return ConfigService.WAR_FILE.ETL;
    }

    @Override
    public LinkedList<Map<String, String>> details(ConfigService configService) {

        DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
        LinkedList<Map<String, String>> statuses = new LinkedList<Map<String, String>>();
        statuses.add(metricDetail("Name (from ETL)", pvName));
        ETLStages etlStages = configService.getETLLookup().getETLStages(pvName);
        if (etlStages == null) {
            logger.info("Cannot find ETLStages for pv {}", pvName);
            return statuses;
        }

        statuses.add(metricDetail(
                "ETL Stages will be triggered at",
                TimeUtils.convertToHumanReadableString(TimeUtils.now()
                        .plusSeconds(etlStages.getCancellingFuture().getDelay(TimeUnit.SECONDS)))));

        for (ETLStage etlStage : etlStages.getStages()) {
            statuses.add(metricDetail(
                    "<b>ETL " + etlStage.getLifetimeorder() + "</b>",
                    "<b>" + etlStage.getETLSource().getName()
                            + " &raquo; "
                            + etlStage.getETLDest().getName()
                            + "</b>"));
            statuses.add(metricDetail(
                    "ETL " + etlStage.getLifetimeorder() + " partition granularity of source",
                    etlStage.getETLSource().getPartitionGranularity().toString()));
            statuses.add(metricDetail(
                    "ETL " + etlStage.getLifetimeorder() + " partition granularity of dest",
                    etlStage.getETLDest().getPartitionGranularity().toString()));
            statuses.add(metricDetail(
                    "ETL " + etlStage.getLifetimeorder() + " delay between jobs (s)",
                    Long.toString(etlStage.getDelaybetweenETLJobsInSecs())));
            if (etlStage.getLastETLCompleteEpochSeconds() != 0) {
                statuses.add(metricDetail(
                        "ETL " + etlStage.getLifetimeorder() + " last completed",
                        TimeUtils.convertToHumanReadableString(etlStage.getLastETLCompleteEpochSeconds())));
                statuses.add(metricDetail(
                        "ETL " + etlStage.getLifetimeorder() + " last job took (ms)",
                        Long.toString(etlStage.getLastETLTimeWeSpentInETLInMilliSeconds())));
            }
            statuses.add(metricDetail(
                    "ETL " + etlStage.getLifetimeorder() + " next job runs at",
                    TimeUtils.convertToHumanReadableString(etlStage.getNextETLStart())));
            if (etlStage.getNumberofTimesWeETLed() != 0) {
                statuses.add(metricDetail(
                        "ETL " + etlStage.getLifetimeorder() + " total time performing ETL(ms)",
                        Long.toString(etlStage.getTotalTimeWeSpentInETLInMilliSeconds())));
                statuses.add(metricDetail(
                        "ETL " + etlStage.getLifetimeorder() + " average time performing ETL(ms)",
                        Long.toString(etlStage.getTotalTimeWeSpentInETLInMilliSeconds()
                                / etlStage.getNumberofTimesWeETLed())));
                statuses.add(metricDetail(
                        "ETL " + etlStage.getLifetimeorder() + " number of times we performed ETL",
                        Integer.toString(etlStage.getNumberofTimesWeETLed())));
                statuses.add(metricDetail(
                        "ETL " + etlStage.getLifetimeorder() + " out of space chunks deleted",
                        Long.toString(etlStage.getOutOfSpaceChunksDeleted())));
                String bytesTransferedUnits = "";
                long bytesTransferred = etlStage.getTotalSrcBytes();
                double bytesTransferredInUnits = bytesTransferred;
                if (bytesTransferred > 1024 * 10 && bytesTransferred <= 1024 * 1024) {
                    bytesTransferredInUnits = bytesTransferred / 1024.0;
                    bytesTransferedUnits = "(KB)";
                } else if (bytesTransferred > 1024 * 1024) {
                    bytesTransferredInUnits = bytesTransferred / (1024.0 * 1024.0);
                    bytesTransferedUnits = "(MB)";
                }
                statuses.add(metricDetail(
                        "ETL " + etlStage.getLifetimeorder() + " approx bytes transferred" + bytesTransferedUnits,
                        twoSignificantDigits.format(bytesTransferredInUnits)));

                statuses.add(metricDetail(
                        "ETL Total time spent by getETLStreams() in ETL(" + etlStage.getLifetimeorder() + ") (ms)",
                        Long.toString(etlStage.getTime4getETLStreams())));
                statuses.add(metricDetail(
                        "ETL Total time spent by free space checks in ETL(" + etlStage.getLifetimeorder() + ") (ms)",
                        Long.toString(etlStage.getTime4checkSizes())));
                statuses.add(metricDetail(
                        "ETL Total time spent by prepareForNewPartition() in ETL(" + etlStage.getLifetimeorder()
                                + ") (ms)",
                        Long.toString(etlStage.getTime4prepareForNewPartition())));
                statuses.add(metricDetail(
                        "ETL Total time spent by appendToETLAppendData() in ETL(" + etlStage.getLifetimeorder()
                                + ") (ms)",
                        Long.toString(etlStage.getTime4appendToETLAppendData())));
                statuses.add(metricDetail(
                        "ETL Total time spent by commitETLAppendData() in ETL(" + etlStage.getLifetimeorder()
                                + ") (ms)",
                        Long.toString(etlStage.getTime4commitETLAppendData())));
                statuses.add(metricDetail(
                        "ETL Total time spent by markForDeletion() in ETL(" + etlStage.getLifetimeorder() + ") (ms)",
                        Long.toString(etlStage.getTime4markForDeletion())));
                statuses.add(metricDetail(
                        "ETL Total time spent by runPostProcessors() in ETL(" + etlStage.getLifetimeorder() + ") (ms)",
                        Long.toString(etlStage.getTime4runPostProcessors())));
                statuses.add(metricDetail(
                        "ETL Total time spent by executePostETLTasks() in ETL(" + etlStage.getLifetimeorder()
                                + ") (ms)",
                        Long.toString(etlStage.getTime4runPostProcessors())));

            } else {
                statuses.add(metricDetail(
                        "ETL " + etlStage.getLifetimeorder() + " number of times we performed ETL", "None so far"));
            }
        }
        return statuses;
    }
}
