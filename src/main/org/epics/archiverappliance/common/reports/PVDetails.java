package org.epics.archiverappliance.common.reports;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.Map;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public interface PVDetails extends BPLAction {
    Logger logger = LogManager.getLogger(PVDetails.class);

    @Override
    default void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String pvName = req.getParameter("pv");
        if (StringUtils.isEmpty(pvName)) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        logger.info("Getting the detailed status for PV " + pvName);
        String detailedStatus = null;
        try {
            detailedStatus = JSONValue.toJSONString(pvDetails(configService, pvName));
        } catch (Exception e) {
            logger.error("No status for PV " + pvName + " in this.", e);
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);

            return;
        }
        if (detailedStatus != null) {
            resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
            try (PrintWriter out = resp.getWriter()) {
                out.print(detailedStatus);
            }
        } else {
            logger.debug("No status for PV " + pvName + " in this.");
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    LinkedList<Map<String, String>> pvDetails(ConfigService configService, String pvName) throws Exception;
}
