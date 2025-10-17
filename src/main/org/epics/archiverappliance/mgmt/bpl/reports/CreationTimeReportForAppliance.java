/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ChannelArchiverDataServerPVInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Creation time and paused status of all PVs in this appliance
 *
 * @author mshankar
 *
 */
public class CreationTimeReportForAppliance implements BPLAction {
    private static final Logger logger = LogManager.getLogger(CreationTimeReportForAppliance.class);

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        logger.info("Getting the creation time for PV's in this appliance");
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

        Pattern pattern = null;
        if (req.getParameter("regex") != null) {
            String nameToMatch = req.getParameter("regex");
            logger.debug("Finding PV's for regex " + nameToMatch);
            pattern = Pattern.compile(nameToMatch);
        }

        Set<String> pausedPVs = configService.getPausedPVsInThisAppliance();
        try (PrintWriter out = resp.getWriter()) {
            out.println("[");
            boolean first = true;
            for (String pvName : configService.getPVsForThisAppliance()) {
                if (pattern != null && !pattern.matcher(pvName).matches()) continue;

                if (first) {
                    first = false;
                } else {
                    out.println(",");
                }
                HashMap<String, String> ret = new HashMap<String, String>();
                ret.put("pvName", pvName);
                // We approx the earliest sample to be the creation time of the PVTypeInfo.
                long earliestSample =
                        configService.getTypeInfoForPV(pvName).getCreationTime().toEpochMilli() / 1000;
                List<ChannelArchiverDataServerPVInfo> externalServerInfos =
                        configService.getChannelArchiverDataServers(pvName);
                if (externalServerInfos != null && !externalServerInfos.isEmpty()) {
                    // If we have ChannelArchiver integration, we pick up the timestamp from there...
                    for (ChannelArchiverDataServerPVInfo externalServerInfo : externalServerInfos) {
                        earliestSample = Math.min(externalServerInfo.getStartSec(), earliestSample);
                    }
                }
                ret.put("creationTS", Long.toString(earliestSample));
                ret.put("paused", pausedPVs.contains(pvName) ? "true" : "false");
                JSONObject.writeJSONString(ret, out);
            }
            out.println("]");
        }
    }
}
