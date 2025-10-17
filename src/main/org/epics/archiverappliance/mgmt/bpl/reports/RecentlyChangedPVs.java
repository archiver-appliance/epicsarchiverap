package org.epics.archiverappliance.mgmt.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Generate a report of PVs that were recently changed.
 * As the lastModifiedTime is also set during creation, this is only those PVs whose creation and modification times are different.
 *
 *
 * @epics.BPLAction - Return a list of PVs sorted by descending PVTypeInfo modification timestamp.
 * @epics.BPLActionParam limit - Limit this report to this many PVs per appliance in the cluster. Optional, if unspecified, there are no limits enforced.
 * @epics.BPLActionEnd
 *
 * @author mshankar
 *
 */
public class RecentlyChangedPVs implements BPLAction {
    private static Logger logger = LogManager.getLogger(RecentlyChangedPVs.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String limit = req.getParameter("limit");
        logger.info("Recently changed PVs report for " + (limit == null ? "default limit " : ("limit " + limit)));
        LinkedList<String> recentlyChangedURLs = new LinkedList<String>();
        for (ApplianceInfo info : configService.getAppliancesInCluster()) {
            String mgmtUrl = info.getMgmtURL();
            String recentlyChangedURL = mgmtUrl + "/getRecentlyModifiedPVsForThisInstance";
            if (limit != null) {
                recentlyChangedURL = recentlyChangedURL + "?limit=" + limit;
            }
            recentlyChangedURLs.add(recentlyChangedURL);
        }

        JSONArray recentlyChangedPVs = GetUrlContent.combineJSONArrays(recentlyChangedURLs);

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(recentlyChangedPVs.toJSONString());
        }
    }
}
