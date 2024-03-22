package org.epics.archiverappliance.etl.common;

import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.reports.Details;
import org.epics.archiverappliance.config.ConfigService;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ETLMetrics implements Details {
    private final List<ETLMetricsForLifetime> etlMetricsForLifetimeList = new LinkedList<ETLMetricsForLifetime>();

    public void add(int lifetimeID) {
        etlMetricsForLifetimeList.add(new ETLMetricsForLifetime(lifetimeID));
    }

    public ETLMetricsForLifetime get(int lifetimeID) {
        return etlMetricsForLifetimeList.get(lifetimeID);
    }

    public Map<String, String> metrics() {
        HashMap<String, String> metrics = new HashMap<String, String>();

        double maxETLPercentage = 0.0;
        long currentEpochSeconds = TimeUtils.getCurrentEpochSeconds();
        for (ETLMetricsForLifetime metricForLifetime : etlMetricsForLifetimeList) {
            double etlPercentage = (double) ((metricForLifetime.getTimeForOverallETLInMilliSeconds() / 1000) * 100)
                    / (currentEpochSeconds - metricForLifetime.getStartOfMetricsMeasurementInEpochSeconds());
            maxETLPercentage = Math.max(etlPercentage, maxETLPercentage);
            metrics.put(
                    "totalETLRuns(" + metricForLifetime.getLifeTimeId() + ")",
                    Long.toString(metricForLifetime.getTotalETLRuns()));
            metrics.put(
                    "timeForOverallETLInSeconds(" + metricForLifetime.getLifeTimeId() + ")",
                    Long.toString(metricForLifetime.getTimeForOverallETLInMilliSeconds() / 1000));
        }
        DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
        metrics.put("maxETLPercentage", twoSignificantDigits.format(maxETLPercentage));
        return metrics;
    }

    @Override
    public LinkedList<Map<String, String>> details(ConfigService configService) {
        DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
        LinkedList<Map<String, String>> details = new LinkedList<Map<String, String>>();
        List<ETLMetricsForLifetime> metricsForLifetime = etlMetricsForLifetimeList;
        if (metricsForLifetime.isEmpty()) {
            details.add(metricDetail("Startup", "In Progress"));
        } else {
            for (ETLMetricsForLifetime metricForLifetime : metricsForLifetime) {
                String lifetimeIdentifier =
                        metricForLifetime.getLifeTimeId() + "&raquo;" + (metricForLifetime.getLifeTimeId() + 1);
                long totalRunsNum = metricForLifetime.getTotalETLRuns();
                RunsFormatter runsFormatter = new RunsFormatter(twoSignificantDigits, totalRunsNum);
                if (totalRunsNum != 0) {
                    long timeForOverallETLInMillis = metricForLifetime.getTimeForOverallETLInMilliSeconds();
                    details.add(metricDetail(
                            "Total number of ETL(" + lifetimeIdentifier + ") runs so far",
                            Long.toString(totalRunsNum)));
                    double avgETLTimeInSeconds = ((double) timeForOverallETLInMillis) / (totalRunsNum * 1000.0);
                    details.add(metricDetail(
                            "Average time spent in ETL(" + lifetimeIdentifier + ") (s/run)",
                            runsFormatter.getFormatted(timeForOverallETLInMillis)));
                    double timeSpentInETLPercent = (avgETLTimeInSeconds * 100)
                            / (TimeUtils.getCurrentEpochSeconds()
                                    - metricForLifetime.getStartOfMetricsMeasurementInEpochSeconds());
                    details.add(metricDetail(
                            "Average percentage of time spent in ETL(" + lifetimeIdentifier + ")",
                            twoSignificantDigits.format(timeSpentInETLPercent)));
                    details.add(metricDetail(
                            "Approximate time taken by last job in ETL(" + lifetimeIdentifier + ") (s)",
                            twoSignificantDigits.format(
                                    metricForLifetime.getApproximateLastGlobalETLTimeInMillis() / 1000)));
                    details.add(metricDetail(
                            "Estimated weekly usage in ETL(" + lifetimeIdentifier + ") (%)",
                            twoSignificantDigits.format(metricForLifetime.getWeeklyETLUsageInPercent())));
                    details.add(metricDetail(
                            "Avg time spent by getETLStreams() in ETL(" + lifetimeIdentifier + ") (s/run)",
                            runsFormatter.getFormatted(metricForLifetime.getTimeinMillSecond4getETLStreams())));
                    details.add(metricDetail(
                            "Avg time spent by free space checks in ETL(" + lifetimeIdentifier + ") (s/run)",
                            runsFormatter.getFormatted(metricForLifetime.getTimeinMillSecond4checkSizes())));
                    details.add(metricDetail(
                            "Avg time spent by prepareForNewPartition() in ETL(" + lifetimeIdentifier + ") (s/run)",
                            runsFormatter.getFormatted(
                                    metricForLifetime.getTimeinMillSecond4prepareForNewPartition())));
                    details.add(metricDetail(
                            "Avg time spent by appendToETLAppendData() in ETL(" + lifetimeIdentifier + ") (s/run)",
                            runsFormatter.getFormatted(metricForLifetime.getTimeinMillSecond4appendToETLAppendData())));
                    details.add(metricDetail(
                            "Avg time spent by commitETLAppendData() in ETL(" + lifetimeIdentifier + ") (s/run)",
                            runsFormatter.getFormatted(metricForLifetime.getTimeinMillSecond4commitETLAppendData())));
                    details.add(metricDetail(
                            "Avg time spent by markForDeletion() in ETL(" + lifetimeIdentifier + ") (s/run)",
                            runsFormatter.getFormatted(metricForLifetime.getTimeinMillSecond4markForDeletion())));
                    details.add(metricDetail(
                            "Avg time spent by runPostProcessors() in ETL(" + lifetimeIdentifier + ") (s/run)",
                            runsFormatter.getFormatted(metricForLifetime.getTimeinMillSecond4runPostProcessors())));
                    details.add(metricDetail(
                            "Avg time spent by executePostETLTasks() in ETL(" + lifetimeIdentifier + ") (s/run)",
                            runsFormatter.getFormatted(metricForLifetime.getTimeinMillSecond4executePostETLTasks())));

                    String bytesTransferedUnits = "";
                    long bytesTransferred = metricForLifetime.getTotalSrcBytes();
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
                            "Estimated bytes transferred in ETL (" + lifetimeIdentifier + ")" + bytesTransferedUnits,
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
