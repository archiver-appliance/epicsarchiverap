/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl.bpl.reports;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.common.ETLMetricsForLifetime;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Gets the ETL appliance metrics
 * @author mshankar
 *
 */
public class ApplianceMetrics implements BPLAction {

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(getETLMetrics(configService));
        }
    }

    private String getETLMetrics(ConfigService configService) {
        HashMap<String, String> metrics = new HashMap<String, String>();
        List<ETLMetricsForLifetime> metricsForLifetime =
                configService.getETLLookup().getApplianceMetrics();
        if (metricsForLifetime == null || metricsForLifetime.size() < 1) {
            metrics.put("Startup", "In Progress");
        } else {
            double maxETLPercentage = 0.0;
            long currentEpochSeconds = TimeUtils.getCurrentEpochSeconds();
            for (ETLMetricsForLifetime metricForLifetime : metricsForLifetime) {
                double etlPercentage = (metricForLifetime.getTimeForOverallETLInMilliSeconds() / 1000)
                        * 100
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
        }

        return JSONValue.toJSONString(metrics);
    }
}
