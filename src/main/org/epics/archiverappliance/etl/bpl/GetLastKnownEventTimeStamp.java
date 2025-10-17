package org.epics.archiverappliance.etl.bpl;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class GetLastKnownEventTimeStamp implements BPLAction {

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String pvName = req.getParameter("pv");
        if (pvName == null || pvName.equals("")) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        Event event = configService.getETLLookup().getLatestEventFromDataStores(pvName);
        if (event != null) {
            resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
            HashMap<String, String> result = new HashMap<String, String>();
            result.put("timestamp", TimeUtils.convertToISO8601String(event.getEventTimeStamp()));
            try (PrintWriter out = resp.getWriter()) {
                out.println(JSONValue.toJSONString(result));
            }
        } else {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
    }
}
