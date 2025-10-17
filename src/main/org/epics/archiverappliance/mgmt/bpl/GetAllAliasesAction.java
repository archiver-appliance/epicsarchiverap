package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 *
 * @epics.BPLAction - Get all the aliases in the cluster and the PV's they are mapped to.
 * @epics.BPLActionEnd
 *
 * @author mshankar
 *
 */
public class GetAllAliasesAction implements BPLAction {
    private static Logger logger = LogManager.getLogger(GetAllAliasesAction.class.getName());

    private static class AliasAndSrc {
        String aliasName;
        String srcPVName;

        AliasAndSrc(String aliasName, String srcPVName) {
            this.aliasName = aliasName;
            this.srcPVName = srcPVName;
        }

        HashMap<String, String> toJSON() {
            HashMap<String, String> ret = new HashMap<String, String>();
            ret.put("aliasName", this.aliasName);
            ret.put("srcPVName", this.srcPVName);
            return ret;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            JSONArray retVal = new JSONArray();
            for (String alias : configService.getAllAliases()) {
                retVal.add(new AliasAndSrc(alias, configService.getRealNameForAlias(alias)).toJSON());
            }
            out.println(retVal.toJSONString());
        } catch (Exception ex) {
            logger.error("Exception returning appliances in cluster", ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
