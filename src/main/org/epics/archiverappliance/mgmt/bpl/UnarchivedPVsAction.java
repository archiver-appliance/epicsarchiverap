/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.ArchivedPVsInList;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Given a list of PVs, determine those that are not being archived/have pending requests.
 * Of course, you can use the status call but that makes calls to the engine etc and can be stressful if you are checking several thousand PVs
 * All this does is check the configservice...
 *
 * @epics.BPLAction - Given a list of PVs, determine those that are not being archived/have pending requests/have aliases.
 * @epics.BPLActionParam pv - A list of pv names. Send as a CSV using a POST or JSON array.
 * @epics.BPLActionEnd
 *
 * @author mshankar
 *
 */
public class UnarchivedPVsAction implements BPLAction {
    private static final Logger logger = LogManager.getLogger(UnarchivedPVsAction.class);

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        logger.info("Determining PVs that are unarchived ");

        LinkedList<String> pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req);
        List<String> archivedPVs = ArchivedPVsInList.getArchivedPVs(pvNames, configService);
        Set<String> unarchivedPVs = (new HashSet<String>(pvNames));
        unarchivedPVs.removeAll(archivedPVs);

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            JSONValue.writeJSONString(unarchivedPVs, out);
        }
    }
}
