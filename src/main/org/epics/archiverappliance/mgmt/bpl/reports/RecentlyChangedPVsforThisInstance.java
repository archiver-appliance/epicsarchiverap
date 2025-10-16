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

public class RecentlyChangedPVsforThisInstance implements BPLAction {
    private static Logger logger = LogManager.getLogger(RecentlyChangedPVsforThisInstance.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String limitStr = req.getParameter("limit");
        int limit = 100;
        logger.info("Recently changed PVs report for this instance for "
                + (limitStr == null ? ("default limit(" + limit + ")") : ("limit " + limitStr)));
        if (limitStr != null) {
            limit = Integer.parseInt(limitStr);
        }

        final String identity = configService.getMyApplianceInfo().getIdentity();
        final class RecentlyChangedPVInfo implements JSONAware {
            String pvName;
            Instant modificationTimeStamp;

            RecentlyChangedPVInfo(String pvName, Instant creationTimeStamp) {
                this.pvName = pvName;
                this.modificationTimeStamp = creationTimeStamp;
            }

            @Override
            public String toJSONString() {
                HashMap<String, String> jsonObj = new HashMap<String, String>();
                jsonObj.put("pvName", pvName);
                jsonObj.put("instance", identity);
                jsonObj.put("modificationTime", TimeUtils.convertToHumanReadableString(this.modificationTimeStamp));
                return JSONValue.toJSONString(jsonObj);
            }
        }

        LinkedList<RecentlyChangedPVInfo> myPVs = new LinkedList<RecentlyChangedPVInfo>();
        for (String pv : configService.getPVsForThisAppliance()) {
            PVTypeInfo typeInfo = configService.getTypeInfoForPV(pv);
            if (typeInfo != null && !typeInfo.getCreationTime().equals(typeInfo.getModificationTime())) {
                myPVs.add(new RecentlyChangedPVInfo(pv, typeInfo.getModificationTime()));
            }
        }

        Collections.sort(myPVs, new Comparator<RecentlyChangedPVInfo>() {
            @Override
            public int compare(RecentlyChangedPVInfo o1, RecentlyChangedPVInfo o2) {
                return o1.modificationTimeStamp.compareTo(o2.modificationTimeStamp);
            }
        });

        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONArray.toJSONString(myPVs.subList(0, Math.min(limit, myPVs.size()))));
        }
    }
}
