package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Gets the names of the data stores for this PV.
 *
 * @epics.BPLAction - Gets the names and definitions of the data stores for this PV. Every store in a PV's typeinfo is expected to have a name - this is typically "name=STS" or something similar. This call returns the names of all the stores for a PV with their URI representations as a dictionary.
 * @epics.BPLActionParam pv - The name of the pv.
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class GetStoresForPV implements BPLAction {
    private static Logger logger = LogManager.getLogger(GetStoresForPV.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String pvName = req.getParameter("pv");
        logger.debug("Getting typeinfo for PV " + pvName);
        if (pvName == null || pvName.equals("")) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        // String pvNameFromRequest = pvName;
        String realName = configService.getRealNameForAlias(pvName);
        if (realName != null) pvName = realName;

        PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
        if (typeInfo == null) {
            logger.warn("Cannot find typeinfo for " + pvName);
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            HashMap<String, String> stores = new HashMap<String, String>();
            for (String store : typeInfo.getDataStores()) {
                StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
                stores.put(plugin.getName(), store);
            }
            out.println(JSONValue.toJSONString(stores));
        } catch (Exception ex) {
            logger.error("Exception marshalling typeinfo for pv " + pvName, ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
