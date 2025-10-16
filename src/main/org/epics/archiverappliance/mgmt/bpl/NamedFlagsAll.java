package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 *
 * @epics.BPLAction - Get all the named flags in the system and their values as a JSON dict
 * @epics.BPLActionEnd
 *
 * @author mshankar
 *
 */
public class NamedFlagsAll implements BPLAction {
    private static Logger logger = LogManager.getLogger(NamedFlagsAll.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        HashMap<String, Boolean> ret = new HashMap<String, Boolean>();
        for (String namedFlagName : configService.getNamedFlagNames()) {
            boolean namedValue = configService.getNamedFlag(namedFlagName);
            ret.put(namedFlagName, namedValue);
        }

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONObject.toJSONString(ret));
        } catch (Exception ex) {
            logger.error("Exception getting all named flags", ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
