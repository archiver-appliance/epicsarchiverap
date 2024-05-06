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
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedList;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Given a list of PVs, determine those that are being archived.
 * Of course, you can use the status call but that makes calls to the engine etc and can be stressful if you are checking several thousand PVs
 * All this does is check the configservice...
 *
 * @epics.BPLAction - Given a list of PVs, determine those that are being archived.
 * @epics.BPLActionParam pv - A list of pv names. Send as a CSV using a POST.
 * @epics.BPLActionEnd
 *
 * @author mshankar
 *
 */
public class ArchivedPVsAction implements BPLAction {
    private static final Logger logger = LogManager.getLogger(ArchivedPVsAction.class);

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        logger.info("Determining PVs that are archived ");
        LinkedList<String> pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req);
        LinkedList<String> archivedPVs = new LinkedList<String>();
        for (String pvName : pvNames) {
            PVTypeInfo typeInfo = null;
            logger.debug("Check for the name as it came in from the user " + pvName);
            typeInfo = configService.getTypeInfoForPV(pvName);
            if (typeInfo != null) {
                archivedPVs.add(pvName);
                continue;
            }
            logger.debug("Check for the normalized name");
            typeInfo = configService.getTypeInfoForPV(PVNames.normalizeChannelName(pvName));
            if (typeInfo != null) {
                archivedPVs.add(pvName);
                continue;
            }
            logger.debug("Check for aliases");
            String aliasRealName = configService.getRealNameForAlias(PVNames.normalizeChannelName(pvName));
            if (aliasRealName != null) {
                typeInfo = configService.getTypeInfoForPV(aliasRealName);
                if (typeInfo != null) {
                    archivedPVs.add(pvName);
                    continue;
                }
            }
            logger.debug("Check for fields");
            String fieldName = PVNames.getFieldName(pvName);
            if (fieldName != null) {
                typeInfo = configService.getTypeInfoForPV(PVNames.channelNamePVName(pvName));
                if (typeInfo != null) {
                    if (Arrays.asList(typeInfo.getArchiveFields()).contains(fieldName)) {
                        archivedPVs.add(pvName);
                        continue;
                    }
                }
            }
        }

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            JSONValue.writeJSONString(archivedPVs, out);
        }
    }
}
