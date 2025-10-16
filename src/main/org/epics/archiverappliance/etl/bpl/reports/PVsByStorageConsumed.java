package org.epics.archiverappliance.etl.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.bpl.reports.StorageWithLifetime.StorageConsumedByPV;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class PVsByStorageConsumed implements BPLAction {
    private static Logger logger = LogManager.getLogger(PVsByStorageConsumed.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String limitStr = req.getParameter("limit");
        logger.info(
                "Storage consumed report for " + (limitStr == null ? "default limit(100) " : ("limit " + limitStr)));
        if (limitStr == null || limitStr.equals("")) limitStr = "100";
        int limit = Integer.parseInt(limitStr);
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        String applianceIdentity = configService.getMyApplianceInfo().getIdentity();
        List<StorageConsumedByPV> pvsByStorageConsumed = StorageWithLifetime.getPVSByStorageConsumed(configService);
        if (pvsByStorageConsumed.size() > limit) {
            pvsByStorageConsumed = pvsByStorageConsumed.subList(0, limit);
        }

        LinkedList<HashMap<String, String>> result = new LinkedList<HashMap<String, String>>();
        try (PrintWriter out = resp.getWriter()) {
            DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
            for (StorageConsumedByPV storageConsumedByPV : pvsByStorageConsumed) {
                HashMap<String, String> pvStorageConsumed = new HashMap<String, String>();
                result.add(pvStorageConsumed);
                pvStorageConsumed.put("pvName", storageConsumedByPV.pvName);
                pvStorageConsumed.put("instance", applianceIdentity);
                pvStorageConsumed.put("storageConsumed", Long.toString(storageConsumedByPV.storageConsumed));
                pvStorageConsumed.put(
                        "storageConsumedInMB",
                        twoSignificantDigits.format(storageConsumedByPV.storageConsumed * 1.0 / (1024 * 1024)));
            }

            out.println(JSONValue.toJSONString(result));
        }
    }
}
