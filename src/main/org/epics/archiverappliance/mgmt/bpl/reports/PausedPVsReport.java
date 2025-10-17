package org.epics.archiverappliance.mgmt.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Generate a report of PVs that are currently paused.
 *
 *
 * @epics.BPLAction - Return a list of PVs that are currently paused.
 * @epics.BPLActionParam limit - Limit this report to this many PVs per appliance in the cluster. Optional, if unspecified, there are no limits enforced.
 * @epics.BPLActionEnd
 *
 * @author mshankar
 *
 */
public class PausedPVsReport implements BPLAction {
    private static Logger logger = LogManager.getLogger(PausedPVsReport.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String limit = req.getParameter("limit");
        logger.info("Paused PVs report for " + (limit == null ? "default limit " : ("limit " + limit)));
        LinkedList<String> pausedPVForThisApplianceURLs = new LinkedList<String>();
        for (ApplianceInfo info : configService.getAppliancesInCluster()) {
            String mgmtURL = info.getMgmtURL();
            String pausedPVForThisApplianceURL =
                    mgmtURL + "/getPausedPVsForThisAppliance" + (limit == null ? "" : ("?limit=" + limit));
            if (limit != null) {
                pausedPVForThisApplianceURL = pausedPVForThisApplianceURL + "?limit=" + limit;
            }
            pausedPVForThisApplianceURLs.add(pausedPVForThisApplianceURL);
        }

        JSONArray pausedPVs = GetUrlContent.combineJSONArrays(pausedPVForThisApplianceURLs);
        LinkedList<HashMap<String, String>> retVal = new LinkedList<HashMap<String, String>>();
        for (Object pausedPVObj : pausedPVs) {
            String pvName = (String) pausedPVObj;
            HashMap<String, String> pvDetails = new HashMap<String, String>();
            pvDetails.put("pvName", pvName);
            PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
            pvDetails.put("instance", typeInfo.getApplianceIdentity());
            pvDetails.put("modificationTime", TimeUtils.convertToHumanReadableString(typeInfo.getModificationTime()));
            retVal.add(pvDetails);
        }

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONArray.toJSONString(retVal));
        }
    }
}
