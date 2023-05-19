package org.epics.archiverappliance.mgmt.pva.actions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.bpl.GetAllPVs;
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
 * Get all the PVs in the cluster. Note this call can return millions of PVs
 * 
 * Additional parameters which can be packaged as uri query parameters
 * pv - An optional argument that can contain a
 * <a href="http://en.wikipedia.org/wiki/Glob_%28programming%29">GLOB</a>
 * wildcard. We will return PVs that match this GLOB. For example, if
 * <code>pv=KLYS*</code>, the server will return all PVs that start with the
 * string <code>KLYS</code>. If both pv and regex are unspecified, we match
 * against all PVs.
 * 
 * regex - An optional argument that can contain a <a href=
 * "http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">Java
 * regex</a> wildcard. We will return PVs that match this regex. For example, if
 * <code>pv=KLYS*</code>, the server will return all PVs that start with the
 * string <code>KLYS</code>.
 * 
 * limit - An optional argument that specifies the number of matched PV's that
 * are retured. If unspecified, we return 500 PV names. To get all the PV names,
 * (potentially in the millions), set limit to &ndash;1.
 * 
 * example
 *
 * request:
 * epics:nt/NTURI:1.0
 * 		string scheme pva
 * 		string path getAllPVs
 *
 * response
 * epics:nt/NTScalarArray:1.0 
 *     string[] value ["pv1", "pv2", ....]
 *
 * Based on {@link GetAllPVs}
 * 
 * @author Kunal Shroff, mshankar
 *
 */
public class PvaGetAllPVs implements PvaAction {
	private static Logger logger = LogManager.getLogger(PvaGetAllPVs.class.getName());

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
		LinkedList<String> pvNames = PVsMatchingParameter.getMatchingPVs(searchParameters, configService, false, defaultLimit);
		NTScalarArray ntScalarArray = NTScalarArray.createBuilder().value(ScalarType.pvString).create();
		ntScalarArray.getValue(PVStringArray.class).put(0, pvNames.size(), pvNames.toArray(new String[pvNames.size()]), 0);
		callback.requestDone(StatusFactory.getStatusCreate().getStatusOK(), ntScalarArray.getPVStructure());
	}

}
