package org.epics.archiverappliance.mgmt.policy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Get various appliance properties as name/value constructs mostly for the UI.
 * @author mshankar
 *
 */
public class GetApplianceProps implements BPLAction {
    private static Logger logger = LogManager.getLogger(GetApplianceProps.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        logger.debug("Getting appliance properties");
        HashMap<String, Object> props = new HashMap<String, Object>();
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        props.put(
                "minimum_sampling_period",
                configService
                        .getInstallationProperties()
                        .getProperty("org.epics.archiverappliance.mgmt.bpl.ArchivePVAction.minimumSamplingPeriod"));
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(props));
        } catch (Exception ex) {
            logger.error(
                    "Exception getting list of appliance properties "
                            + configService.getMyApplianceInfo().getIdentity(),
                    ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
