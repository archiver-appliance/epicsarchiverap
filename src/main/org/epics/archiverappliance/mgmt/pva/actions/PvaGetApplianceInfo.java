package org.epics.archiverappliance.mgmt.pva.actions;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.bpl.GetApplianceInfo;
import org.epics.nt.NTTable;
import org.epics.nt.NTURI;
import org.epics.pvaccess.server.rpc.RPCResponseCallback;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.Status.StatusType;

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
	public void request(PVStructure args, RPCResponseCallback callback, ConfigService configService) {
		String id = null;
		LinkedHashMap<String, String> applianceInfoMap;
		NTURI uri = NTURI.wrap(args);
		List<String> queryName = Arrays.asList(uri.getQueryNames());
		if (queryName.contains("id")) {
			id = uri.getQueryField(PVString.class, "id").get();
		}

		ApplianceInfo applianceInfo = null;
		if (id == null || id.equals("")) {
			applianceInfo = configService.getMyApplianceInfo();
			logger.debug("No id specified, returning the id of this appliance " + applianceInfo.getIdentity());
		} else {
			logger.debug("Getting Appliance info for appliance with identity " + id);
			applianceInfo = configService.getAppliance(id);
		}

		if (applianceInfo == null) {
			logger.warn("Cannot find appliance info for " + id);
			callback.requestDone(StatusFactory.getStatusCreate().createStatus(StatusType.ERROR,
					"Cannot find appliance info for " + id, null), null);
			return;
		} else {
			applianceInfoMap = new LinkedHashMap<String, String>();
			applianceInfoMap.put("identity", applianceInfo.getIdentity());
			applianceInfoMap.put("mgmtURL", applianceInfo.getMgmtURL());
			applianceInfoMap.put("engineURL", applianceInfo.getEngineURL());
			applianceInfoMap.put("retrievalURL", applianceInfo.getRetrievalURL());
			applianceInfoMap.put("etlURL", applianceInfo.getEtlURL());
		}

		NTTable ntTable = NTTable.createBuilder().addColumn("Key", ScalarType.pvString)
				.addColumn("Value", ScalarType.pvString).create();
		ntTable.getColumn(PVStringArray.class, "Key").put(0, applianceInfoMap.size(),
				applianceInfoMap.keySet().toArray(new String[applianceInfoMap.size()]), 0);
		ntTable.getColumn(PVStringArray.class, "Value").put(0, applianceInfoMap.size(),
				applianceInfoMap.values().toArray(new String[applianceInfoMap.size()]), 0);
		callback.requestDone(StatusFactory.getStatusCreate().getStatusOK(), ntTable.getPVStructure());
	}
}
