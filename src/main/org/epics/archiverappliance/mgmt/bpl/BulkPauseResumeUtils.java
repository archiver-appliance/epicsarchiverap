package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;

/**
 * Some utility methods for bulk pause and resume...
 * @author mshankar
 */
public class BulkPauseResumeUtils {
    private static Logger logger = LogManager.getLogger(BulkPauseResumeUtils.class);

    /**
     * Get a list of PVNames based on if this is a POST or GET.
     * @param req HttpServletRequest
     * @param configService  ConfigService
     * @return LinkedList String PV names
     * @throws IOException  &emsp;
     */
    public static LinkedList<String> getPVNames(HttpServletRequest req, ConfigService configService)
            throws IOException {
        LinkedList<String> pvNames = null;
        if (req.getMethod().equals("POST")) {
            pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req);
        } else {
            pvNames = PVsMatchingParameter.getMatchingPVs(req, configService, false, -1);
        }
        return pvNames;
    }

    public static List<HashMap<String, String>> pauseResumePVs(
            List<String> pvNames, ConfigService configService, boolean askingToPausePV) throws IOException {
        List<HashMap<String, String>> retVal = new LinkedList<HashMap<String, String>>();
        HashMap<String, HashMap<String, String>> retValMap = new HashMap<String, HashMap<String, String>>();
        for (String pvName : pvNames) {
            String realName = configService.getRealNameForAlias(pvName);
            if (realName != null) pvName = realName;

            HashMap<String, String> pvPauseResumeStatus = new HashMap<String, String>();
            pvPauseResumeStatus.put("pvName", pvName);
            retVal.add(pvPauseResumeStatus);
            retValMap.put(pvName, pvPauseResumeStatus);

            ApplianceInfo info = configService.getApplianceForPV(pvName);
            if (info == null) {
                pvPauseResumeStatus.put(
                        "validation", "Trying to pause PV " + pvName + " that is not currently being archived.");
                logger.error(pvPauseResumeStatus.get("validation"));
                pvPauseResumeStatus.put("validation", "Unable to pause PV " + pvName);
            } else {
                PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
                if (askingToPausePV && typeInfo.isPaused()) {
                    pvPauseResumeStatus.put("validation", "Trying to pause PV " + pvName + " that is already paused.");
                    logger.error(pvPauseResumeStatus.get("validation"));
                    pvPauseResumeStatus.put("validation", "PV " + pvName + " is already paused");
                } else if (!askingToPausePV && !typeInfo.isPaused()) {
                    pvPauseResumeStatus.put("validation", "Trying to resume PV " + pvName + " that is not paused.");
                    logger.error(pvPauseResumeStatus.get("validation"));
                    pvPauseResumeStatus.put("validation", "PV " + pvName + " is not paused");
                } else {
                    logger.debug("Changing the typeinfo for pause/resume for PV " + pvName);
                    typeInfo.setPaused(askingToPausePV);
                    typeInfo.setModificationTime(TimeUtils.now());
                    configService.updateTypeInfoForPV(pvName, typeInfo);
                    pvPauseResumeStatus.put("status", "ok");
                }
            }
        }
        return retVal;
    }
}
