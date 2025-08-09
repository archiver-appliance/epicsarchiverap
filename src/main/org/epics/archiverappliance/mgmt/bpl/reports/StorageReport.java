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
import java.util.LinkedList;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class StorageReport implements BPLAction {
    private static Logger logger = LogManager.getLogger(StorageReport.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        logger.info("Generating storage report");
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            LinkedList<Map<String, String>> result = new LinkedList<Map<String, String>>();
            Map<String, Long> pvCounts = ApplianceMetrics.getAppliancePVCounts(configService);
            for (ApplianceInfo info : configService.getAppliancesInCluster()) {
                var applianceInfo = ApplianceMetrics.getBasicMetrics(configService, result, info, pvCounts);

                // The getApplianceMetrics here is not a typo. We redisplay some of the appliance metrics in this page.
                JSONObject engineMetrics =
                        GetUrlContent.getURLContentAsJSONObject(info.getEngineURL() + "/getApplianceMetrics");
                JSONObject etlMetrics =
                        GetUrlContent.getURLContentAsJSONObject(info.getEtlURL() + "/getApplianceMetrics");
                ApplianceMetrics.combineMetrics(info, applianceInfo, engineMetrics, etlMetrics, logger);
            }
            out.println(JSONValue.toJSONString(result));
        }
    }
}
