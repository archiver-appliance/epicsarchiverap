package org.epics.archiverappliance.mgmt.bpl;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @epics.BPLAction - Consolidate the data for this PV until the specified store. The PV needs to be paused first.
 * @epics.BPLActionParam pv - The name of the pv.
 * @epics.BPLActionParam storage - The name of the store until which we'll consolidate data. This is typically a string like STS or MTS. To get a list of names of stores for a PV, please see /getStoresForPV
 * @epics.BPLActionParam date - Optional date to run the ETL process against, default is one year in the future.
 * @epics.BPLActionEnd
 *
 * @author mshankar
 *
 */
public class ConsolidatePBFilesForOnePV implements BPLAction {
    private static final Logger logger = LogManager.getLogger(ConsolidatePBFilesForOnePV.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String pvName = req.getParameter("pv");
        String storageName = req.getParameter("storage");

        if (StringUtils.isEmpty(pvName) || StringUtils.isEmpty(storageName)) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        String date = req.getParameter("date");
        if(date == null || date.equals("")) {
            date = TimeUtils.convertToISO8601String(TimeUtils.plusDays(TimeUtils.now(), 366));
        }

        String realName = configService.getRealNameForAlias(pvName);
        if (realName != null) pvName = realName;

        ApplianceInfo info = configService.getApplianceForPV(pvName);
        if (info == null) {
            logger.debug("Unable to find appliance for PV " + pvName);
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
        if (typeInfo == null) {
            logger.debug("Unable to find typeinfo for PV...");
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        HashMap<String, Object> infoValues = new HashMap<String, Object>();
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

        boolean foundPlugin = false;
        for (String store : typeInfo.getDataStores()) {
            StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
            if (plugin.getName().equals(storageName)) {
                logger.debug("Found the storage plugin identifier by " + storageName);
                foundPlugin = true;
                break;
            }
        }

        if (!foundPlugin) {
            try (PrintWriter out = resp.getWriter()) {
                infoValues.put("validation", "Cannot find storage with name " + storageName + " for pv " + pvName);
                out.println(JSONValue.toJSONString(infoValues));
                return;
            }
        }

        if (!typeInfo.isPaused()) {
            infoValues.put("validation", "Cannot consolidate unless PV " + pvName + " is paused.");
            logger.error(infoValues.get("validation"));
            try (PrintWriter out = resp.getWriter()) {
                out.println(JSONValue.toJSONString(infoValues));
            }
            return;
        }

        String ETLConsolidateURL = info.getEtlURL() + "/consolidateDataForPV"
                + "?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                + "&storage=" + URLEncoder.encode(storageName, StandardCharsets.UTF_8)
                + "&date=" + URLEncoder.encode(date, StandardCharsets.UTF_8);
        logger.info("Consolidating data for PV using URL " + ETLConsolidateURL);

        JSONObject pvStatus = GetUrlContent.getURLContentAsJSONObject(ETLConsolidateURL);
        if (pvStatus != null && !pvStatus.equals("")) {
            try (PrintWriter out = resp.getWriter()) {
                out.println(JSONValue.toJSONString(pvStatus));
            }
        } else {
            try (PrintWriter out = resp.getWriter()) {
                infoValues.put("validation", "Unable to consolidate data for PV " + pvName);
                out.println(JSONValue.toJSONString(infoValues));
            }
        }
    }
}
