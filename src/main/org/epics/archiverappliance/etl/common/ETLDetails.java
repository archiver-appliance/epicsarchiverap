package org.epics.archiverappliance.etl.common;

import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.reports.Details;
import org.epics.archiverappliance.config.ConfigService;

import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ETLDetails implements Details {

    private final String pvName;

    public ETLDetails(String pvName) {
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
        for (ETLPVLookupItems lookupItem : configService.getETLLookup().getLookupItemsForPV(pvName)) {
            statuses.add(metricDetail(
                    "ETL " + lookupItem.getLifetimeorder() + " partition granularity of source",
                    lookupItem.getETLSource().getPartitionGranularity().toString()));
            statuses.add(metricDetail(
                    "ETL " + lookupItem.getLifetimeorder() + " partition granularity of dest",
                    lookupItem.getETLDest().getPartitionGranularity().toString()));
            if (lookupItem.getLastETLCompleteEpochSeconds() != 0) {
                statuses.add(metricDetail(
                        "ETL " + lookupItem.getLifetimeorder() + " last completed",
                        TimeUtils.convertToHumanReadableString(lookupItem.getLastETLCompleteEpochSeconds())));
                statuses.add(metricDetail(
                        "ETL " + lookupItem.getLifetimeorder() + " last job took (ms)",
                        Long.toString(lookupItem.getLastETLTimeWeSpentInETLInMilliSeconds())));
            }
            statuses.add(metricDetail(
                    "ETL " + lookupItem.getLifetimeorder() + " next job runs at",
                    TimeUtils.convertToHumanReadableString(
                            lookupItem.getCancellingFuture().getDelay(TimeUnit.SECONDS)
                                    + (TimeUtils.now().toEpochMilli() / 1000))));
            if (lookupItem.getNumberofTimesWeETLed() != 0) {
                statuses.add(metricDetail(
                        "ETL " + lookupItem.getLifetimeorder() + " total time performing ETL(ms)",
                        Long.toString(lookupItem.getTotalTimeWeSpentInETLInMilliSeconds())));
                statuses.add(metricDetail(
                        "ETL " + lookupItem.getLifetimeorder() + " average time performing ETL(ms)",
                        Long.toString(lookupItem.getTotalTimeWeSpentInETLInMilliSeconds()
                                / lookupItem.getNumberofTimesWeETLed())));
                statuses.add(metricDetail(
                        "ETL " + lookupItem.getLifetimeorder() + " number of times we performed ETL",
                        Integer.toString(lookupItem.getNumberofTimesWeETLed())));
                statuses.add(metricDetail(
                        "ETL " + lookupItem.getLifetimeorder() + " out of space chunks deleted",
                        Long.toString(lookupItem.getOutOfSpaceChunksDeleted())));
                String bytesTransferedUnits = "";
                long bytesTransferred = lookupItem.getTotalSrcBytes();
                double bytesTransferredInUnits = bytesTransferred;
                if (bytesTransferred > 1024 * 10 && bytesTransferred <= 1024 * 1024) {
                    bytesTransferredInUnits = bytesTransferred / 1024.0;
                    bytesTransferedUnits = "(KB)";
                } else if (bytesTransferred > 1024 * 1024) {
                    bytesTransferredInUnits = bytesTransferred / (1024.0 * 1024.0);
                    bytesTransferedUnits = "(MB)";
                }
                statuses.add(metricDetail(
                        "ETL " + lookupItem.getLifetimeorder() + " approx bytes transferred" + bytesTransferedUnits,
                        twoSignificantDigits.format(bytesTransferredInUnits)));

                statuses.add(metricDetail(
                        "ETL Total time spent by getETLStreams() in ETL(" + lookupItem.getLifetimeorder() + ") (ms)",
                        Long.toString(lookupItem.getTime4getETLStreams())));
                statuses.add(metricDetail(
                        "ETL Total time spent by free space checks in ETL(" + lookupItem.getLifetimeorder() + ") (ms)",
                        Long.toString(lookupItem.getTime4checkSizes())));
                statuses.add(metricDetail(
                        "ETL Total time spent by prepareForNewPartition() in ETL(" + lookupItem.getLifetimeorder()
                                + ") (ms)",
                        Long.toString(lookupItem.getTime4prepareForNewPartition())));
                statuses.add(metricDetail(
                        "ETL Total time spent by appendToETLAppendData() in ETL(" + lookupItem.getLifetimeorder()
                                + ") (ms)",
                        Long.toString(lookupItem.getTime4appendToETLAppendData())));
                statuses.add(metricDetail(
                        "ETL Total time spent by commitETLAppendData() in ETL(" + lookupItem.getLifetimeorder()
                                + ") (ms)",
                        Long.toString(lookupItem.getTime4commitETLAppendData())));
                statuses.add(metricDetail(
                        "ETL Total time spent by markForDeletion() in ETL(" + lookupItem.getLifetimeorder() + ") (ms)",
                        Long.toString(lookupItem.getTime4markForDeletion())));
                statuses.add(metricDetail(
                        "ETL Total time spent by runPostProcessors() in ETL(" + lookupItem.getLifetimeorder()
                                + ") (ms)",
                        Long.toString(lookupItem.getTime4runPostProcessors())));
                statuses.add(metricDetail(
                        "ETL Total time spent by executePostETLTasks() in ETL(" + lookupItem.getLifetimeorder()
                                + ") (ms)",
                        Long.toString(lookupItem.getTime4runPostProcessors())));

            } else {
                statuses.add(metricDetail(
                        "ETL " + lookupItem.getLifetimeorder() + " number of times we performed ETL", "None so far"));
            }
        }
        return statuses;
    }
}
