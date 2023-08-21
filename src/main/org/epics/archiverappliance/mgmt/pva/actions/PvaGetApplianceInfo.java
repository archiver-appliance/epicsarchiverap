package org.epics.archiverappliance.mgmt.pva.actions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.bpl.GetApplianceInfo;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.MustBeArrayException;
import org.epics.pva.data.nt.NotValueException;
import org.epics.pva.data.nt.PVATable;
import org.epics.pva.data.nt.PVAURI;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Based on {@link GetApplianceInfo}
 * 
 * @author Kunal Shroff, mshankar
 *
 */
public class PvaGetApplianceInfo implements PvaAction {

	private static Logger logger = LogManager.getLogger(PvaGetApplianceInfo.class.getName());

	public static final String NAME = "getApplianceInfo";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public PVAStructure request(PVAStructure args, ConfigService configService) throws PvaActionException {
		String id = null;
		LinkedHashMap<String, String> applianceInfoMap;
		PVAURI uri = PVAURI.fromStructure(args);
		Map<String, String> queryName;
		try {
			queryName = uri.getQuery();
			if (queryName.containsKey("id")) {
				id = queryName.get("id");
			}
		} catch (NotValueException e) {
			logger.error("Failed to parse input args: " + args, e);
		}

		ApplianceInfo applianceInfo;
		if (id == null || id.equals("")) {
			applianceInfo = configService.getMyApplianceInfo();
			logger.debug("No id specified, returning the id of this appliance " + applianceInfo.getIdentity());
		} else {
			logger.debug("Getting Appliance info for appliance with identity " + id);
			applianceInfo = configService.getAppliance(id);
		}

		if (applianceInfo == null) {
			logger.warn("Cannot find appliance info for " + id);
			throw new PvaActionException("Cannot find appliance info for " + id);
		} else {
			applianceInfoMap = new LinkedHashMap<String, String>();
			applianceInfoMap.put("identity", applianceInfo.getIdentity());
			applianceInfoMap.put("mgmtURL", applianceInfo.getMgmtURL());
			applianceInfoMap.put("engineURL", applianceInfo.getEngineURL());
			applianceInfoMap.put("retrievalURL", applianceInfo.getRetrievalURL());
			applianceInfoMap.put("etlURL", applianceInfo.getEtlURL());
		}

		try {
			return PVATable.PVATableBuilder.aPVATable()
					.name(NAME)
					.addColumn(new PVAStringArray("Key", applianceInfoMap.keySet().toArray(new String[applianceInfoMap.size()])))
					.addColumn(new PVAStringArray("Value", applianceInfoMap.values().toArray(new String[applianceInfoMap.size()])))
					.build();
		} catch (MustBeArrayException e) {
			throw new ResponseConstructionException(e);
		}
	}
}
