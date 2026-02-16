package org.epics.archiverappliance.etl.bpl;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ChangeStore implements BPLAction {
    private static final Logger logger = LogManager.getLogger(ChangeStore.class);

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String pvName = req.getParameter("pv");
        String storageName = req.getParameter("storage");
        String newPlugin = req.getParameter("newbackend");
        if (StringUtils.isEmpty(pvName) || StringUtils.isEmpty(storageName) || StringUtils.isEmpty(newPlugin)) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        try {
            ETLExecutor.moveDataFromOneStorageToAnother(configService, pvName, storageName, newPlugin);
            resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
            HashMap<String, Object> infoValues = new HashMap<String, Object>();
            try (PrintWriter out = resp.getWriter()) {
                infoValues.put("status", "ok");
                infoValues.put("desc", "Successfully changed the storage for PV " + pvName + " into " + storageName);
                out.println(JSONValue.toJSONString(infoValues));
            }
        } catch (IOException e) {
            logger.error("Exception consolidating the partitions for pv: " + pvName + " into store: " + storageName, e);
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
        }
    }
}
