package org.epics.archiverappliance.mgmt.pva.actions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.bpl.PVsMatchingParameter;
import org.epics.nt.NTScalarArray;
import org.epics.nt.NTURI;
import org.epics.pvaccess.server.rpc.RPCResponseCallback;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;

/**
 * Based on {@link GetAllPVs}
 * @author Kunal Shroff, mshankar
 *
 */
public class PvaGetAllPVs implements PvaAction {
	private static Logger logger = Logger.getLogger(PvaGetAllPVs.class.getName());

	public static final String NAME = "getAllPVs";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public void request(PVStructure args, RPCResponseCallback callback, ConfigService configService) {
		logger.debug("Getting all pvs for cluster");
		Map<String, String> searchParameters = new HashMap<String, String>();
		int defaultLimit = 500;
		NTURI uri = NTURI.wrap(args);
		List<String> queryName = Arrays.asList(uri.getQueryNames());
		if (queryName.contains("limit")) {
			searchParameters.put("limit", uri.getQueryField(PVString.class, "limit").get());
		}
		LinkedList<String> pvNames = PVsMatchingParameter.getMatchingPVs(searchParameters, configService, false,
				defaultLimit);
		NTScalarArray ntScalarArray = NTScalarArray.createBuilder().value(ScalarType.pvString).create();
		ntScalarArray.getValue(PVStringArray.class).put(0, pvNames.size(), pvNames.toArray(new String[pvNames.size()]), 0);
		callback.requestDone(StatusFactory.getStatusCreate().getStatusOK(), ntScalarArray.getPVStructure());
	}

}
