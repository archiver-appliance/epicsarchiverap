package org.epics.archiverappliance.mgmt.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class InstanceReportDetails implements BPLAction {
    private static Logger logger = LogManager.getLogger(InstanceReportDetails.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String applianceIdentity = req.getParameter("appliance");
        logger.info("Getting the detailed instance metrics for the appliance " + applianceIdentity);
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        String applianceDetailsURLSnippet =
                "/getInstanceMetricsForAppliance?appliance=" + URLEncoder.encode(applianceIdentity, "UTF-8");
        ApplianceInfo info = configService.getAppliance(applianceIdentity);
        try (PrintWriter out = resp.getWriter()) {
            LinkedList<Map<String, String>> result = new LinkedList<Map<String, String>>();
            addDetailedStatus(result, "Appliance Identity", applianceIdentity);
            JSONArray engineStatusVars =
                    GetUrlContent.getURLContentAsJSONArray(info.getEngineURL() + applianceDetailsURLSnippet);
            if (engineStatusVars == null) {
                logger.warn("No status vars from engine using URL " + info.getEngineURL() + applianceDetailsURLSnippet);
            } else {
                GetUrlContent.combineJSONArrays(result, engineStatusVars);
            }

            JSONArray etlStatusVars =
                    GetUrlContent.getURLContentAsJSONArray(info.getEtlURL() + applianceDetailsURLSnippet);
            if (etlStatusVars == null) {
                logger.warn("No status vars from ETL using URL " + info.getEtlURL() + applianceDetailsURLSnippet);
            } else {
                GetUrlContent.combineJSONArrays(result, etlStatusVars);
            }

            JSONArray retrievalStatusVars =
                    GetUrlContent.getURLContentAsJSONArray(info.getRetrievalURL() + applianceDetailsURLSnippet);
            if (retrievalStatusVars == null) {
                logger.warn("No status vars from retrieval using URL " + info.getRetrievalURL()
                        + applianceDetailsURLSnippet);
            } else {
                GetUrlContent.combineJSONArrays(result, retrievalStatusVars);
            }

            out.println(JSONValue.toJSONString(result));
        }
    }

    private static void addDetailedStatus(LinkedList<Map<String, String>> statuses, String name, String value) {
        Map<String, String> obj = new LinkedHashMap<String, String>();
        obj.put("name", name);
        obj.put("value", value);
        obj.put("source", "mgmt");
        statuses.add(obj);
    }
}
