package org.epics.archiverappliance.etl.common;

import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.reports.Details;
import org.epics.archiverappliance.config.ConfigService;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class ETLMetrics implements Details {
    private final LinkedHashMap<String, ETLMetricsIntoStore> etlMetricsIntoStores = new LinkedHashMap<String, ETLMetricsIntoStore>();

    public void createMetricIfNoExists(String destName) {
        synchronized(etlMetricsIntoStores) {
            if(!etlMetricsIntoStores.containsKey(destName)) {
                etlMetricsIntoStores.put(destName, new ETLMetricsIntoStore(destName));
            }            
        }
    }

    public ETLMetricsIntoStore get(String destName) {
        return etlMetricsIntoStores.get(destName);
    }

    public Map<String, String> metrics() {
        HashMap<String, String> metrics = new HashMap<String, String>();

        double maxETLPercentage = 0.0;
        long currentEpochSeconds = TimeUtils.getCurrentEpochSeconds();
        for (ETLMetricsIntoStore etlMetricsIntoStore : etlMetricsIntoStores.values()) {
            double etlPercentage = (double) ((etlMetricsIntoStore.getTimeForOverallETLInMilliSeconds() / 1000) * 100)
                    / (currentEpochSeconds - etlMetricsIntoStore.getStartOfMetricsMeasurementInEpochSeconds());
            maxETLPercentage = Math.max(etlPercentage, maxETLPercentage);
            metrics.put(
                    "totalETLRuns(" + etlMetricsIntoStore.toString() + ")",
                    Long.toString(etlMetricsIntoStore.getTotalETLRuns()));
            metrics.put(
                    "timeForOverallETLInSeconds(" + etlMetricsIntoStore.toString() + ")",
                    Long.toString(etlMetricsIntoStore.getTimeForOverallETLInMilliSeconds() / 1000));
        }
        DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
        metrics.put("maxETLPercentage", twoSignificantDigits.format(maxETLPercentage));
        return metrics;
    }

    @Override
    public LinkedList<Map<String, String>> details(ConfigService configService) {
        DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
        LinkedList<Map<String, String>> details = new LinkedList<Map<String, String>>();
        if (etlMetricsIntoStores.isEmpty()) {
            details.add(metricDetail("Startup", "In Progress"));
        } else {
            for (ETLMetricsIntoStore etlMetricsIntoStore : etlMetricsIntoStores.values()) {
                String destIdentifier = etlMetricsIntoStore.toString();
                long totalRunsNum = etlMetricsIntoStore.getTotalETLRuns();
                RunsFormatter runsFormatter = new RunsFormatter(twoSignificantDigits, totalRunsNum);
                if (totalRunsNum != 0) {
                    long timeForOverallETLInMillis = etlMetricsIntoStore.getTimeForOverallETLInMilliSeconds();
                    details.add(metricDetail(
                            "<b>Total number of ETL runs into " + destIdentifier + " so far</b>",
                            Long.toString(totalRunsNum)));
                    double avgETLTimeInSeconds = ((double) timeForOverallETLInMillis) / (totalRunsNum * 1000.0);
                    details.add(metricDetail(
                            "Average time spent in ETL into " + destIdentifier + " (s/run)",
                            runsFormatter.getFormatted(timeForOverallETLInMillis)));
                    double timeSpentInETLPercent = (avgETLTimeInSeconds * 100)
                            / (TimeUtils.getCurrentEpochSeconds()
                                    - etlMetricsIntoStore.getStartOfMetricsMeasurementInEpochSeconds());
                    details.add(metricDetail(
                            "Average percentage of time spent in ETL",
                            twoSignificantDigits.format(timeSpentInETLPercent)));
                    details.add(metricDetail(
                            "Approximate time taken by last ETL job (s)",
                            twoSignificantDigits.format(
                                    etlMetricsIntoStore.getApproximateLastGlobalETLTimeInMillis() / 1000)));
                    details.add(metricDetail(
                            "Estimated weekly usage in ETL (%)",
                            twoSignificantDigits.format(etlMetricsIntoStore.getWeeklyETLUsageInPercent())));
                    details.add(metricDetail(
                            "Avg time spent by getETLStreams (s/run)",
                            runsFormatter.getFormatted(etlMetricsIntoStore.getTimeinMillSecond4getETLStreams())));
                    details.add(metricDetail(
                            "Avg time spent by free space checks (s/run)",
                            runsFormatter.getFormatted(etlMetricsIntoStore.getTimeinMillSecond4checkSizes())));
                    details.add(metricDetail(
                            "Avg time spent by prepareForNewPartition() (s/run)",
                            runsFormatter.getFormatted(
                                    etlMetricsIntoStore.getTimeinMillSecond4prepareForNewPartition())));
                    details.add(metricDetail(
                            "Avg time spent by appendToETLAppendData() (s/run)",
                            runsFormatter.getFormatted(etlMetricsIntoStore.getTimeinMillSecond4appendToETLAppendData())));
                    details.add(metricDetail(
                            "Avg time spent by commitETLAppendData() (s/run)",
                            runsFormatter.getFormatted(etlMetricsIntoStore.getTimeinMillSecond4commitETLAppendData())));
                    details.add(metricDetail(
                            "Avg time spent by markForDeletion() in ETL (s/run)",
                            runsFormatter.getFormatted(etlMetricsIntoStore.getTimeinMillSecond4markForDeletion())));
                    details.add(metricDetail(
                            "Avg time spent by runPostProcessors() in ETL (s/run)",
                            runsFormatter.getFormatted(etlMetricsIntoStore.getTimeinMillSecond4runPostProcessors())));
                    details.add(metricDetail(
                            "Avg time spent by executePostETLTasks() in ETL (s/run)",
                            runsFormatter.getFormatted(etlMetricsIntoStore.getTimeinMillSecond4executePostETLTasks())));

                    String bytesTransferedUnits = "";
                    long bytesTransferred = etlMetricsIntoStore.getTotalSrcBytes();
                    double bytesTransferredInUnits = bytesTransferred;
                    if (bytesTransferred > 1024 * 10 && bytesTransferred <= 1024 * 1024) {
                        bytesTransferredInUnits = bytesTransferred / 1024.0;
                        bytesTransferedUnits = "(KB)";
                    } else if (bytesTransferred > 1024 * 1024 && bytesTransferred <= 1024 * 1024 * 1024) {
                        bytesTransferredInUnits = bytesTransferred / (1024.0 * 1024.0);
                        bytesTransferedUnits = "(MB)";
                    } else if (bytesTransferred > 1024 * 1024 * 1024) {
                        bytesTransferredInUnits = bytesTransferred / (1024.0 * 1024.0 * 1024.0);
                        bytesTransferedUnits = "(GB)";
                    }

                    details.add(metricDetail(
                            "Estimated bytes transferred in ETL (" + destIdentifier + ")" + bytesTransferedUnits,
                            twoSignificantDigits.format(bytesTransferredInUnits)));
                }
            }
        }

        return details;
    }

    private record RunsFormatter(DecimalFormat twoSignificantDigits, long totalRunsNum) {
        String getFormatted(long longMetric) {
            return twoSignificantDigits.format(((double) longMetric) / (1000.0 * totalRunsNum));
        }
    }

    @Override
    public ConfigService.WAR_FILE source() {
        return ConfigService.WAR_FILE.ETL;
    }
}
