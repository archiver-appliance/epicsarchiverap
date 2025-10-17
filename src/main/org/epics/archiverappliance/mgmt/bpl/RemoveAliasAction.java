package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.HashMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 *
 * @epics.BPLAction - Remove an alias for the specified PV. This is only supported for PVs who have completed their archive PV workflow.
 * @epics.BPLActionParam pv - The real name of the pv.
 * @epics.BPLActionParam aliasname - The alias name of the pv.
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class RemoveAliasAction implements BPLAction {
    private static Logger logger = LogManager.getLogger(RemoveAliasAction.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String pvName = req.getParameter("pv");
        if (pvName == null || pvName.equals("")) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        String aliasName = req.getParameter("aliasname");
        if (aliasName == null || aliasName.equals("")) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        logger.info("Removing alias " + aliasName + " for pv " + pvName);

        PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
        if (typeInfo == null) {
            logger.error(
                    "We do not yet support removing aliases for PVs which have not completed their workflow " + pvName);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return;
        }

        ApplianceInfo infoForPV = configService.getApplianceForPV(pvName);
        if (infoForPV.equals(configService.getMyApplianceInfo())) {
            configService.removeAlias(aliasName, pvName);
            HashMap<String, Object> infoValues = new HashMap<String, Object>();
            infoValues.put("status", "ok");
            infoValues.put("desc", "Removed an alias " + aliasName + " for PV " + pvName);
            try (PrintWriter out = resp.getWriter()) {
                out.println(JSONValue.toJSONString(infoValues));
            }
            return;
        } else {
            // Route to the appliance that hosts the PVTypeInfo
            String redirectURL = infoForPV.getMgmtURL()
                    + "/removeAlias?pv="
                    + URLEncoder.encode(pvName, "UTF-8")
                    + "&aliasname="
                    + URLEncoder.encode(aliasName, "UTF-8");

            logger.info("Redirecting removeAlias request for PV to " + infoForPV.getIdentity() + " using URL "
                    + redirectURL);
            JSONObject status = GetUrlContent.getURLContentAsJSONObject(redirectURL);
            try (PrintWriter out = resp.getWriter()) {
                out.println(JSONValue.toJSONString(status));
            }
            return;
        }
    }
}
