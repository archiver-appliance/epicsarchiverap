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
import java.util.LinkedList;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * A report that looks at the potentially slow changing PV's that have dropped events. This is basically those PV's whose "How many events lost totally so far?" is greater than the "How many events so far?". For now, we restrict to event losses from incorrect timestamps.
 *
 *
 * @epics.BPLAction - Return a list of PVs which have lost more events than events from the IOC sorted by number of times we've lost events. This does not include PVs that have been paused.
 * @epics.BPLActionParam limit - Limit this report to this many PVs per appliance in the cluster. Optional, if unspecified, there are no limits enforced.
 * @epics.BPLActionEnd
 *
 *
 * @author mshankar
 *
 */
public class SlowChangingPVsWithDroppedEvents implements BPLAction {
    private static Logger logger = LogManager.getLogger(SlowChangingPVsWithDroppedEvents.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String limit = req.getParameter("limit");
        logger.info("Report for PVs that have dropped more events than actual events for "
                + (limit == null ? "default limit " : ("limit " + limit)));
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        LinkedList<String> storageRateURLs = new LinkedList<String>();
        for (ApplianceInfo info : configService.getAppliancesInCluster()) {
            storageRateURLs.add(info.getEngineURL() + "/getSlowChangingPVsWithDroppedEvents"
                    + (limit == null ? "" : ("?limit=" + limit)));
        }
        try (PrintWriter out = resp.getWriter()) {
            JSONArray neverConnPVs = GetUrlContent.combineJSONArrays(storageRateURLs);
            out.println(JSONValue.toJSONString(neverConnPVs));
        }
    }
}
