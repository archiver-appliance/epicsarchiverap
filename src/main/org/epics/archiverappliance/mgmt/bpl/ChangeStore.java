package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BPLAction;
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
import java.util.HashMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 *
 * @epics.BPLAction - Change the file type of the PV to the specified type. The PV needs to be paused first. For best results, consolidate all the data to one store. Note, this is actually changing the data so you should make a backup just in case. There is every chance that this may leave the data for this PV in an inconsistent state. It is also possible that the plugin may do nothing in which case you may have to rename the existing PV to a new PV; delete this PV and issue a fresh archival request.
 * @epics.BPLActionParam pv - The name of the pv.
 * @epics.BPLActionParam newbackend - The new backend - A storage plugin url
 * @epics.BPLActionParam storage - The name of the store
 * @epics.BPLActionEnd
 *
 * Note, this is actually a dangerous function in that it can leave the PV in all kinds of inconsistent states.
 * Use with caution.
 *
 */
public class ChangeStore implements BPLAction {
    private static final Logger logger = LogManager.getLogger(ChangeStore.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String pvName = req.getParameter("pv");
        if (pvName == null || pvName.isEmpty()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        String newbackend = req.getParameter("newbackend");
        if (newbackend == null || newbackend.isEmpty()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        String storage = req.getParameter("storage");
        if (storage == null || storage.isEmpty()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        // String pvNameFromRequest = pvName;
        String realName = configService.getRealNameForAlias(pvName);
        if (realName != null) pvName = realName;

        ApplianceInfo info = configService.getApplianceForPV(pvName);
        if (info == null) {
            logger.debug("Unable to find appliance for PV " + pvName);
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        if (!info.getIdentity().equals(configService.getMyApplianceInfo().getIdentity())) {
            // We should proxy this call to the actual appliance hosting the PV.
            String redirectURL =
                    info.getMgmtURL() + "/convertFiles?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                            + "&newbackend=" + URLEncoder.encode(newbackend, StandardCharsets.UTF_8);
            logger.debug("Routing request to the appliance hosting the PV " + pvName + " using URL " + redirectURL);
            JSONObject status = GetUrlContent.getURLContentAsJSONObject(redirectURL);
            resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
            try (PrintWriter out = resp.getWriter()) {
                out.println(JSONValue.toJSONString(status));
            }
            return;
        }

        PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
        if (typeInfo == null) {
            logger.debug("Unable to find typeinfo for PV " + pvName);
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        HashMap<String, Object> infoValues = new HashMap<String, Object>();
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

        if (!typeInfo.isPaused()) {
            String msg = "Need to pause PV before changing type for " + pvName;
            logger.error(msg);
            infoValues.put("validation", msg);
            try (PrintWriter out = resp.getWriter()) {
                out.println(JSONValue.toJSONString(infoValues));
            }
            return;
        }

        boolean foundPlugin = false;
        for (String store : typeInfo.getDataStores()) {
            StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
            if (plugin.getName().equals(storage)) {
                logger.debug("Found the storage plugin identifier by " + storage);
                foundPlugin = true;
                break;
            }
        }

        if (!foundPlugin) {
            try (PrintWriter out = resp.getWriter()) {
                infoValues.put("validation", "Cannot find storage with name " + storage + " for pv " + pvName);
                out.println(JSONValue.toJSONString(infoValues));
                return;
            }
        }

        String etlConsolidateURL = info.getEtlURL() + "/consolidateDataForPV"
                + "?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                + "&storage=" + URLEncoder.encode(storage, StandardCharsets.UTF_8)
                + "&newbackend=" + URLEncoder.encode(newbackend, StandardCharsets.UTF_8);
        logger.info("Consolidating data for PV using URL " + etlConsolidateURL);

        // Update the type info in the database.
        configService.updateTypeInfoForPV(pvName, typeInfo);

        JSONObject pvStatus = GetUrlContent.getURLContentAsJSONObject(etlConsolidateURL);
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
