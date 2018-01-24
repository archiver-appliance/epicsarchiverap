/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.pva.actions;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.mgmt.bpl.ArchivedPVsAction;
import org.epics.nt.NTTable;
import org.epics.pvaccess.server.rpc.RPCResponseCallback;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;

/**
 * Given a list of PVs, determine those that are being archived.
 * Of course, you can use the status call but that makes calls to the engine etc and can be stressful if you are checking several thousand PVs
 * All this does is check the configservice...
 * 
 * Given a list of PVs, determine those that are being archived.
 * 
 * Based on {@link ArchivedPVsAction}
 * @author mshankar, shroffk
 *
 */
public class PvaGetArchivedPVs implements PvaAction {
	private static final Logger logger = Logger.getLogger(PvaGetArchivedPVs.class);
	
	public static final String NAME = "archivedPVStatus";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public void request(PVStructure args, RPCResponseCallback callback, ConfigService configService) {
		logger.info("Determining PVs that are archived ");
		LinkedHashMap<String, String> map = new LinkedHashMap<>();
		for(String pvName : NTUtil.extractStringArray(NTTable.wrap(args).getColumn(PVStringArray.class, "pv"))) {
			PVTypeInfo typeInfo = null;
			logger.debug("Check for the name as it came in from the user " + pvName);
			typeInfo = configService.getTypeInfoForPV(pvName);
			if(typeInfo != null)  {
				map.put(pvName, "Archived");
				continue;
			}
			logger.debug("Check for the normalized name");
			typeInfo = configService.getTypeInfoForPV(PVNames.normalizePVName(pvName));
			if(typeInfo != null) {
				map.put(pvName, "Archived");
				continue;
			}
			logger.debug("Check for aliases");
			String aliasRealName = configService.getRealNameForAlias(PVNames.normalizePVName(pvName));
			if(aliasRealName != null) { 
				typeInfo = configService.getTypeInfoForPV(aliasRealName);
				if(typeInfo != null) {
					map.put(pvName, "Archived");
					continue;
				}
			}
			logger.debug("Check for fields");
			String fieldName = PVNames.getFieldName(pvName);
			if(fieldName != null) { 
				typeInfo = configService.getTypeInfoForPV(PVNames.stripFieldNameFromPVName(pvName));
				if(typeInfo != null) { 
					if(Arrays.asList(typeInfo.getArchiveFields()).contains(fieldName)) {
						map.put(pvName, "Archived");
						continue;
					}
				}
			}
			map.put(pvName, "Not Archived");
		}
		NTTable resultTable = NTTable.createBuilder()
				.addColumn("pv", ScalarType.pvString)
				.addColumn("status", ScalarType.pvString)
				.create();
		int counter = 0;
		for (Entry<String, String> entry : map.entrySet()) {
			resultTable.getColumn(PVStringArray.class, "pv").put(counter, 1, new String[] {entry.getKey()} , 0);
			resultTable.getColumn(PVStringArray.class, "status").put(counter, 1, new String[] {entry.getValue()}, 0);
			counter++;
		}
		callback.requestDone(StatusFactory.getStatusCreate().getStatusOK(), resultTable.getPVStructure());
	}
}
