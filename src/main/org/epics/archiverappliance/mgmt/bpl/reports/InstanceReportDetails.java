package org.epics.archiverappliance.mgmt.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.reports.Details;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class InstanceReportDetails implements BPLAction {
    private static final Logger logger = LogManager.getLogger(InstanceReportDetails.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String applianceIdentity = req.getParameter("appliance");
        logger.info("Getting the detailed instance metrics for the appliance " + applianceIdentity);
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(metricsDetails(configService, applianceIdentity)));
        }
    }

    public LinkedList<Map<String, String>> metricsDetails(ConfigService configService, String applianceIdentity) {
        ApplianceInfo info = configService.getAppliance(applianceIdentity);

        String applianceDetailsURLSnippet = "/getInstanceMetricsForAppliance?appliance="
                + URLEncoder.encode(applianceIdentity, StandardCharsets.UTF_8);
        LinkedList<Map<String, String>> result = new LinkedList<Map<String, String>>();
        result.add(Details.metricDetail("mgmt", "Appliance Identity", applianceIdentity));
        getInstanceDetails(info.getEngineURL(), applianceDetailsURLSnippet, ConfigService.WAR_FILE.ENGINE, result);

        getInstanceDetails(info.getEtlURL(), applianceDetailsURLSnippet, ConfigService.WAR_FILE.ENGINE, result);

        getInstanceDetails(
                info.getRetrievalURL(), applianceDetailsURLSnippet, ConfigService.WAR_FILE.RETRIEVAL, result);
        return result;
    }

    private static void getInstanceDetails(
            String info,
            String applianceDetailsURLSnippet,
            ConfigService.WAR_FILE warFile,
            LinkedList<Map<String, String>> result) {
        JSONArray engineStatusVars = GetUrlContent.getURLContentAsJSONArray(info + applianceDetailsURLSnippet);
        if (engineStatusVars == null) {
            logger.warn("No status vars from " + warFile.name() + " using URL " + info + applianceDetailsURLSnippet);
        } else {
            GetUrlContent.combineJSONArrays(result, engineStatusVars);
        }
    }
}
