package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Get a list of PVs for this appliance as a JSON array
 * @author mshankar
 *
 */
public class GetPVsForThisAppliance implements BPLAction {
    private static Logger logger = LogManager.getLogger(GetPVsForThisAppliance.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        logger.debug("Getting pvs for appliance "
                + configService.getMyApplianceInfo().getIdentity());
        LinkedList<String> pvsOnThisAppliance = new LinkedList<String>();
        for (String pvName : configService.getPVsForThisAppliance()) {
            pvsOnThisAppliance.add(pvName);
        }
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(pvsOnThisAppliance));
        } catch (Exception ex) {
            logger.error(
                    "Exception getting pvs for appliance "
                            + configService.getMyApplianceInfo().getIdentity(),
                    ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
