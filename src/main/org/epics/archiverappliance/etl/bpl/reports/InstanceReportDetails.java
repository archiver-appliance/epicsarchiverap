package org.epics.archiverappliance.etl.bpl.reports;

import org.epics.archiverappliance.common.reports.MetricsDetails;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.common.ETLMetrics;

import java.util.LinkedList;
import java.util.Map;

public class InstanceReportDetails implements MetricsDetails {

    @Override
    public LinkedList<Map<String, String>> metricsDetails(ConfigService configService) {
        ETLMetrics etlMetrics = configService.getETLLookup().getApplianceMetrics();
        return etlMetrics.details(configService);
    }
}
