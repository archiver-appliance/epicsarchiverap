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
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class InstanceReport implements BPLAction {
    private static final Logger logger = LogManager.getLogger(InstanceReport.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        logger.info("Generating Instance report");
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

                // The getApplianceMetrics here is not a typo. We redisplay some of the appliance metrics in this page.
                JSONObject engineMetrics =
                        GetUrlContent.getURLContentAsJSONObject(info.getEngineURL() + "/getApplianceMetrics");
                JSONObject etlMetrics =
                        GetUrlContent.getURLContentAsJSONObject(info.getEtlURL() + "/getApplianceMetrics");
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

                long vmStartTime = ManagementFactory.getRuntimeMXBean().getStartTime();
                Duration vmInterval = Duration.between(
                        Instant.ofEpochMilli(vmStartTime), Instant.ofEpochMilli(System.currentTimeMillis()));

                applianceInfo.put("MGMT_uptime", vmInterval.toString());

                applianceInfo.put("errors", Integer.toString(0));
                applianceInfo.put("capacityUtilized", "N/A");
            }
            out.println(JSONValue.toJSONString(result));
        }
    }
}
