package org.epics.archiverappliance.mgmt.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.json.simple.JSONArray;
import org.json.simple.JSONAware;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Generate a report of PVs recently added to this instance....
 * @author mshankar
 *
 */
public class RecentlyAddedPVsforThisInstance implements BPLAction {
    private static Logger logger = LogManager.getLogger(RecentlyAddedPVsforThisInstance.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, final ConfigService configService)
            throws IOException {
        String limitStr = req.getParameter("limit");
        int limit = 100;
        logger.info("Recently added PVs report for this instance for "
                + (limitStr == null ? ("default limit(" + limit + ")") : ("limit " + limitStr)));
        if (limitStr != null) {
            limit = Integer.parseInt(limitStr);
        }

        final String identity = configService.getMyApplianceInfo().getIdentity();
        final class RecentlyAddedPVInfo implements JSONAware {
            String pvName;
            Instant creationTimeStamp;

            RecentlyAddedPVInfo(String pvName, Instant creationTimeStamp) {
                this.pvName = pvName;
                this.creationTimeStamp = creationTimeStamp;
            }

            @Override
            public String toJSONString() {
                HashMap<String, String> jsonObj = new HashMap<String, String>();
                jsonObj.put("pvName", pvName);
                jsonObj.put("instance", identity);
                jsonObj.put("creationTime", TimeUtils.convertToHumanReadableString(this.creationTimeStamp));
                return JSONValue.toJSONString(jsonObj);
            }
        }

        LinkedList<RecentlyAddedPVInfo> myPVs = new LinkedList<RecentlyAddedPVInfo>();
        for (String pv : configService.getPVsForThisAppliance()) {
            PVTypeInfo typeInfo = configService.getTypeInfoForPV(pv);
            if (typeInfo != null) {
                myPVs.add(new RecentlyAddedPVInfo(pv, typeInfo.getCreationTime()));
            }
        }

        Collections.sort(myPVs, new Comparator<RecentlyAddedPVInfo>() {
            @Override
            public int compare(RecentlyAddedPVInfo o1, RecentlyAddedPVInfo o2) {
                return o2.creationTimeStamp.compareTo(o1.creationTimeStamp);
            }
        });

        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONArray.toJSONString(myPVs.subList(0, Math.min(limit, myPVs.size()))));
        }
    }
}
