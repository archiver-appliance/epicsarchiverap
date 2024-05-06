package org.epics.archiverappliance.retrieval.bpl.reports;

import org.epics.archiverappliance.common.reports.MetricsDetails;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.retrieval.RetrievalMetrics;

import java.util.LinkedList;
import java.util.Map;

public class InstanceReportDetails implements MetricsDetails {

    @Override
    public LinkedList<Map<String, String>> metricsDetails(ConfigService configService) {
        return RetrievalMetrics.calculateSummedMetrics(configService).details(configService);
    }
}
