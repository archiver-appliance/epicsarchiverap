package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 *
 * @epics.BPLAction - Resume archiving the specified PV.
 * @epics.BPLActionParam pv - The name of the pv. You can also pass in GLOB wildcards here and multiple PVs as a comma separated list. If you have more PVs that can fit in a GET, send the pv's as a CSV <code>pv=pv1,pv2,pv3</code> as the body of a POST.
 * @epics.BPLActionEnd
 *
 * @author mshankar
 *
 */
public class ResumeArchivingPV implements BPLAction {
    private static Logger logger = LogManager.getLogger(ResumeArchivingPV.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        if (req.getMethod().equals("POST")) {
            resumeMultiplePVs(req, resp, configService);
            return;
        }

        String pvName = req.getParameter("pv");
        if (pvName == null || pvName.equals("")) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        if (pvName.contains(",") || pvName.contains("*") || pvName.contains("?")) {
            resumeMultiplePVs(req, resp, configService);
        } else {
            // We only have one PV in the request
            List<String> pvNames = new LinkedList<String>();
            pvNames.add(pvName);
            resumeMultiplePVs(pvNames, resp, configService);
        }
    }

    private void resumeMultiplePVs(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException, UnsupportedEncodingException {
        LinkedList<String> pvNames = BulkPauseResumeUtils.getPVNames(req, configService);
        resumeMultiplePVs(pvNames, resp, configService);
    }

    private void resumeMultiplePVs(List<String> pvNames, HttpServletResponse resp, ConfigService configService)
            throws IOException, UnsupportedEncodingException {
        boolean askingToPausePV = false;
        List<HashMap<String, String>> response =
                BulkPauseResumeUtils.pauseResumePVs(pvNames, configService, askingToPausePV);

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

        if (pvNames.size() == 1) {
            try (PrintWriter out = resp.getWriter()) {
                out.println(JSONValue.toJSONString(response.getFirst()));
            }
            return;
        }

        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(response));
        }
    }
}
