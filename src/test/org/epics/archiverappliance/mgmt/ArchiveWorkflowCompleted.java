package org.epics.archiverappliance.mgmt;

import com.google.common.net.UrlEscapers;
import org.awaitility.Awaitility;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class ArchiveWorkflowCompleted {
    @SuppressWarnings("unchecked")
    public static void isArchiveRequestComplete(String pvNameToArchive) {
        Awaitility.await()
                .pollInterval(10, TimeUnit.SECONDS)
                .atMost(10, TimeUnit.MINUTES)
                .until(() -> {
                    JSONArray neverConnectedPVs = GetUrlContent.getURLContentAsJSONArray(
                            "http://localhost:17665/mgmt/bpl/getNeverConnectedPVs");
                    for (Object obj : neverConnectedPVs) {
                        HashMap<String, String> pv = (HashMap<String, String>) obj;
                        if (pv.get("pvName").equals(pvNameToArchive)) {
                            return false;
                        }
                    }
                    return true;
                });
        String pvStatusURL = "http://localhost:17665/mgmt/bpl/getPVStatus?pv="
                + UrlEscapers.urlFormParameterEscaper().escape(pvNameToArchive);
        Awaitility.await()
                .pollInterval(10, TimeUnit.SECONDS)
                .atMost(10, TimeUnit.MINUTES)
                .until(() -> {
                    HashMap<String, String> pvStatus = (HashMap<String, String>)
                            GetUrlContent.getURLContentAsJSONArray(pvStatusURL).get(0);
                    return pvStatus.get("status").equals("Being archived");
                });
    }
}
