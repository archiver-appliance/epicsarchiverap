/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.reports;

import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Summary metrics for an appliance.
 * @author mshankar
 *
 */
public class ApplianceMetrics implements BPLAction {
    private static final Logger logger = LogManager.getLogger(ApplianceMetrics.class);

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        logger.info("Generating appliance metrics report");
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            LinkedList<Map<String, String>> result = new LinkedList<Map<String, String>>();
            Map<String, Long> pvCounts = ApplianceMetrics.getAppliancePVCounts(configService);
            for (ApplianceInfo info : configService.getAppliancesInCluster()) {
                HashMap<String, String> applianceInfo = getBasicMetrics(configService, result, info, pvCounts);

                logger.debug("Asking for appliance metrics from engine using " + info.getEngineURL()
                        + "/getApplianceMetrics");
                JSONObject engineMetrics =
                        GetUrlContent.getURLContentAsJSONObject(info.getEngineURL() + "/getApplianceMetrics");
                logger.debug(
                        "Asking for appliance metrics from ETL using " + info.getEtlURL() + "/getApplianceMetrics");
                JSONObject etlMetrics =
                        GetUrlContent.getURLContentAsJSONObject(info.getEtlURL() + "/getApplianceMetrics");
                logger.debug("Asking for appliance metrics from retrieval using " + info.getRetrievalURL()
                        + "/getApplianceMetrics");
                combineMetrics(info, applianceInfo, engineMetrics, etlMetrics, logger);
            }
            out.println(JSONValue.toJSONString(result));
        }
    }

    /*
     * Return a map of appliance identity -> PV counts.
     */
    static Map<String, Long> getAppliancePVCounts(ConfigService configService) {
        Map<String, Long> pvCounts = configService
                .queryPVTypeInfos(
                        Predicates.alwaysTrue(),
                        Projections.<Map.Entry<String, PVTypeInfo>, String>singleAttribute("applianceIdentity"))
                .stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (String applianceIdentity : pvCounts.keySet()) {
            logger.info("PVCount {} Count {}", applianceIdentity, pvCounts.get(applianceIdentity));
        }
        return pvCounts;
    }

    static HashMap<String, String> getBasicMetrics(
            ConfigService configService,
            LinkedList<Map<String, String>> result,
            ApplianceInfo info,
            Map<String, Long> pvCounts) {
        HashMap<String, String> applianceInfo = new HashMap<String, String>();
        result.add(applianceInfo);
        applianceInfo.put("instance", info.getIdentity());
        applianceInfo.put("pvCount", Long.toString(pvCounts.getOrDefault(info.getIdentity(), 0L)));
        return applianceInfo;
    }

    static void combineMetrics(
            ApplianceInfo info,
            HashMap<String, String> applianceInfo,
            JSONObject engineMetrics,
            JSONObject etlMetrics,
            Logger logger) {
        combineGenericMetrics(info, applianceInfo, engineMetrics, etlMetrics, logger);

        applianceInfo.put("capacityUtilized", "N/A");
    }

    static void combineGenericMetrics(
            ApplianceInfo info,
            HashMap<String, String> applianceInfo,
            JSONObject engineMetrics,
            JSONObject etlMetrics,
            Logger logger) {
        JSONObject retrievalMetrics =
                GetUrlContent.getURLContentAsJSONObject(info.getRetrievalURL() + "/getApplianceMetrics");
        if (engineMetrics != null && etlMetrics != null && retrievalMetrics != null) {
            logger.debug("All of the components are working for " + info.getIdentity());
            applianceInfo.put("status", "Working");
        } else {
            logger.debug("At least one of the components is not working for " + info.getIdentity());
            StringWriter buf = new StringWriter();
            buf.append("Stopped - ");
            if (engineMetrics == null) buf.append("engine ");
            if (etlMetrics == null) buf.append("ETL ");
            if (retrievalMetrics == null) buf.append("retrieval ");
            applianceInfo.put("status", buf.toString());
        }
        GetUrlContent.combineJSONObjects(applianceInfo, engineMetrics);
        logger.debug("Done combining metrics from the engine");
        GetUrlContent.combineJSONObjects(applianceInfo, etlMetrics);
        logger.debug("Done combining metrics from etl");
        GetUrlContent.combineJSONObjects(applianceInfo, retrievalMetrics);
        logger.debug("Done combining metrics from retrieval");
    }
}
