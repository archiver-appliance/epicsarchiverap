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
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Details for the PV's that are currently in METAINFO_REQUESTED requested state in the archive workflow.
 *
 * @epics.BPLAction - Get a list of PVs that are currently in METAINFO_REQUESTED state.
 * @epics.BPLActionEnd
 *
 * @author mshankar
 *
 */
public class MetaGetsAction implements BPLAction {
    private static final Logger logger = LogManager.getLogger(MetaGetsAction.class);

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        logger.info("Getting the status of metagets from the engine.");
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        LinkedList<String> neverConnUrls = new LinkedList<String>();
        for (ApplianceInfo info : configService.getAppliancesInCluster()) {
            neverConnUrls.add(info.getEngineURL() + "/getMetaGetsForThisAppliance");
        }
        try (PrintWriter out = resp.getWriter()) {
            JSONArray neverConnPVs = GetUrlContent.combineJSONArrays(neverConnUrls);
            out.println(JSONValue.toJSONString(neverConnPVs));
        }
    }
}
