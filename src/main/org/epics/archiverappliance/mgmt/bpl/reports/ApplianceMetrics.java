/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
            for (ApplianceInfo info : configService.getAppliancesInCluster()) {
                HashMap<String, String> applianceInfo = new HashMap<String, String>();
                result.add(applianceInfo);
                applianceInfo.put("instance", info.getIdentity());

                int pvCount = 0;
                for (@SuppressWarnings("unused") String pvName : configService.getPVsForThisAppliance()) {
                    pvCount++;
                }
                applianceInfo.put("pvCount", Integer.toString(pvCount));

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
                GetUrlContent.combineJSONObjects(applianceInfo, etlMetrics);
                GetUrlContent.combineJSONObjects(applianceInfo, retrievalMetrics);

                applianceInfo.put("capacityUtilized", "N/A");
            }
            out.println(JSONValue.toJSONString(result));
        }
    }
}
