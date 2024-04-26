/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.pva.actions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.mgmt.bpl.ArchivedPVsAction;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.MustBeArrayException;
import org.epics.pva.data.nt.PVATable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

/**
 * Given a list of PVs, determine those that are being archived. Of course, you
 * can use the status call but that makes calls to the engine etc and can be
 * stressful if you are checking several thousand PVs All this does is check the
 * configservice...
 * <p>
 * Given a list of PVs, determine those that are being archived.
 * <p>
 * example request
 * <p>
 * epics:nt/NTTable:1.0
 * <ul>
 *   <li>string[] labels [pv]</li>
 *   <li>structure value
 *   <ul>
 *     <li>string[] pv [test_0,test_1,test_10,test_100...]</li>
 *   </ul>
 *   </li>
 * </ul>
 * example result:
 * <p>
 * epics:nt/NTTable:1.0
 * <ul>
 *   <li>string[] labels [pv,status]</li>
 *   <li>structure value
 *   <ul>
 *     <li>string[] pv [test_0,test_1,test_10,test_100...]</li>
 *     <li>string[] status [Being archived,Initial sampling,Being archived,Being archived,...]</li>
 *   </ul>
 *   </li>
 * </ul>
 * <p>
 * Based on {@link ArchivedPVsAction}
 * @author mshankar, shroffk
 *
 */
public class PvaGetArchivedPVs implements PvaAction {
    private static final Logger logger = LogManager.getLogger(PvaGetArchivedPVs.class);

    public static final String NAME = "archivedPVStatus";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public PVAStructure request(PVAStructure args, ConfigService configService) throws PvaActionException {
        logger.info("Determining PVs that are archived ");
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        for (String pvName :
                NTUtil.extractStringArray(PVATable.fromStructure(args).getColumn("pv"))) {
            PVTypeInfo typeInfo = null;
            logger.debug("Check for the name as it came in from the user " + pvName);
            typeInfo = configService.getTypeInfoForPV(pvName);
            if (typeInfo != null) {
                map.put(pvName, "Archived");
                continue;
            }
            logger.debug("Check for the normalized name");
            typeInfo = configService.getTypeInfoForPV(PVNames.normalizePVName(pvName));
            if (typeInfo != null) {
                map.put(pvName, "Archived");
                continue;
            }
            logger.debug("Check for aliases");
            String aliasRealName = configService.getRealNameForAlias(PVNames.normalizePVName(pvName));
            if (aliasRealName != null) {
                typeInfo = configService.getTypeInfoForPV(aliasRealName);
                if (typeInfo != null) {
                    map.put(pvName, "Archived");
                    continue;
                }
            }
            logger.debug("Check for fields");
            String fieldName = PVNames.getFieldName(pvName);
            if (fieldName != null) {
                typeInfo = configService.getTypeInfoForPV(PVNames.stripFieldNameFromPVName(pvName));
                if (typeInfo != null) {
                    if (Arrays.asList(typeInfo.getArchiveFields()).contains(fieldName)) {
                        map.put(pvName, "Archived");
                        continue;
                    }
                }
            }
            map.put(pvName, "Not Archived");
        }
        String[] pvs = new String[map.size()];
        String[] statuses = new String[map.size()];
        int counter = 0;
        for (Entry<String, String> entry : map.entrySet()) {
            pvs[counter] = entry.getKey();
            statuses[counter] = entry.getValue();
            counter++;
        }
        try {
            return PVATable.PVATableBuilder.aPVATable()
                    .name(NAME)
                    .addColumn(new PVAStringArray("pv", pvs))
                    .addColumn(new PVAStringArray("status", statuses))
                    .build();
        } catch (MustBeArrayException e) {
            throw new RuntimeException(e);
        }
    }
}
