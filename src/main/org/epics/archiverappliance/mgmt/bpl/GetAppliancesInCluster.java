package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 *
 * @epics.BPLAction - Get the appliance information for all the appliances in the cluster that are active.
 * @epics.BPLActionEnd
 *
 * @author mshankar
 *
 */
public class GetAppliancesInCluster implements BPLAction {
    private static Logger logger = LogManager.getLogger(GetAppliancesInCluster.class.getName());

    @SuppressWarnings("unchecked")
    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            JSONArray retVal = new JSONArray();
            JSONEncoder<ApplianceInfo> jsonEncoder = JSONEncoder.getEncoder(ApplianceInfo.class);
            for (ApplianceInfo applianceInfo : configService.getAppliancesInCluster()) {
                JSONObject output = jsonEncoder.encode(applianceInfo);
                retVal.add(output);
            }

            out.println(retVal.toJSONString());
        } catch (Exception ex) {
            logger.error("Exception returning appliances in cluster", ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
