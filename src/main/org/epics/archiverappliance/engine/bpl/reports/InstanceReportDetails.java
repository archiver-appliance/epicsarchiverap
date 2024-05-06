package org.epics.archiverappliance.engine.bpl.reports;

import org.epics.archiverappliance.common.reports.MetricsDetails;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.epics.EngineMetrics;

import java.util.LinkedList;
import java.util.Map;

public class InstanceReportDetails implements MetricsDetails {

    @Override
    public LinkedList<Map<String, String>> metricsDetails(ConfigService configService) {
        return EngineMetrics.computeEngineMetrics(configService.getEngineContext(), configService)
                .details(configService);
    }
}
