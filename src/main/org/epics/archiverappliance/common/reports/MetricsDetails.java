package org.epics.archiverappliance.common.reports;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface MetricsDetails extends BPLAction {

    @Override
    default void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(metricsDetails(configService)));
        }
    }

    LinkedList<Map<String, String>> metricsDetails(ConfigService configService);
}
