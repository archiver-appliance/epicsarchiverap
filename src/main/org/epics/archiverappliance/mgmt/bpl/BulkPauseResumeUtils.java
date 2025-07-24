package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigService.EAABulkOperation;
import org.epics.archiverappliance.config.ConfigService.WAR_FILE;
import org.epics.archiverappliance.config.PVTypeInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

    public static class PauseResumeBulkOperation implements EAABulkOperation<Map<String, Map<String, String>>> {
        private final Map<String, List<String>> pvNamesByAppliance;
        private final boolean askingToPausePV;

        public PauseResumeBulkOperation(Map<String, List<String>> pvNamesByAppliance, boolean askingToPausePV) {
            this.pvNamesByAppliance = pvNamesByAppliance;
            this.askingToPausePV = askingToPausePV;
        }

        @Override
        public Map<String, Map<String, String>> call(ConfigService configService) {
            HashMap<String, Map<String, String>> bulkStatus = new HashMap<String, Map<String, String>>();
            if (!configService.getWarFile().equals(WAR_FILE.MGMT)) {
                // According to Hz documentation, the executor service does not run on Hz clients
                logger.error("We should't really be here {}", configService.getWarFile());
                return bulkStatus;
            }
            // Pause/resume only those PV's that are being archived on this appliance.
            List<String> pvNamesOnThisAppliance = this.pvNamesByAppliance.get(
                    configService.getMyApplianceInfo().getIdentity());
            if (pvNamesOnThisAppliance != null && !pvNamesOnThisAppliance.isEmpty()) {
                for (String pvName : pvNamesOnThisAppliance) {
                    logger.debug("Bulk pause/resume for PV {}", pvName);
                    HashMap<String, String> pvPauseResumeStatus = new HashMap<String, String>();
                    bulkStatus.put(pvName, pvPauseResumeStatus);
                    PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
                    if (askingToPausePV && typeInfo.isPaused()) {
                        pvPauseResumeStatus.put(
                                "validation", "Trying to pause PV " + pvName + " that is already paused.");
                        logger.error(pvPauseResumeStatus.get("validation"));
                        pvPauseResumeStatus.put("validation", "PV " + pvName + " is already paused");
                    } else if (!askingToPausePV && !typeInfo.isPaused()) {
                        pvPauseResumeStatus.put("validation", "Trying to resume PV " + pvName + " that is not paused.");
                        logger.error(pvPauseResumeStatus.get("validation"));
                        pvPauseResumeStatus.put("validation", "PV " + pvName + " is not paused");
                    } else {
                        logger.debug(
                                "Setting the paused status in the typeinfo for PV {} to {}",
                                pvName,
                                this.askingToPausePV);
                        typeInfo.setPaused(askingToPausePV);
                        typeInfo.setModificationTime(TimeUtils.now());
                        configService.updateTypeInfoForPV(pvName, typeInfo);
                        pvPauseResumeStatus.put("status", "ok");
                    }
                }
            }
            return bulkStatus;
        }
    }

    public static List<HashMap<String, String>> pauseResumePVs(
            List<String> pvNames, ConfigService configService, boolean askingToPausePV) throws IOException {
        List<HashMap<String, String>> retVal = new LinkedList<HashMap<String, String>>();
        HashMap<String, HashMap<String, String>> retValMap = new HashMap<String, HashMap<String, String>>();
        LinkedList<String> realPVNames = new LinkedList<String>();
        for (String pvName : pvNames) {
            String realName = configService.getRealNameForAlias(pvName);
            if (realName != null) pvName = realName;
            realPVNames.add(pvName);

            HashMap<String, String> pvPauseResumeStatus = new HashMap<String, String>();
            pvPauseResumeStatus.put("pvName", pvName);
            retVal.add(pvPauseResumeStatus);
            retValMap.put(pvName, pvPauseResumeStatus);
        }
        Map<String, List<String>> pvsByAppliance = configService.breakDownPVsByAppliance(realPVNames);
        PauseResumeBulkOperation bulkOperation = new PauseResumeBulkOperation(pvsByAppliance, askingToPausePV);
        Map<String, Map<String, Map<String, String>>> statusesByAppliance =
                configService.executeClusterWide(bulkOperation);
        for (Map<String, Map<String, String>> statusByAppliance : statusesByAppliance.values()) {
            for (String pvName : statusByAppliance.keySet()) {
                Map<String, String> statuses = statusByAppliance.get(pvName);
                retValMap.get(pvName).putAll(statuses);
            }
        }
        for (String pvName : pvNames) {
            if (retValMap.get(pvName).containsKey("validation")
                    || retValMap.get(pvName).containsKey("status")) {
                // We got some status
            } else {
                retValMap
                        .get(pvName)
                        .put(
                                "validation",
                                "Trying to pause PV " + pvName
                                        + " that is not currently being archived or on an instance that is not active.");
            }
        }
        return retVal;
    }
}
