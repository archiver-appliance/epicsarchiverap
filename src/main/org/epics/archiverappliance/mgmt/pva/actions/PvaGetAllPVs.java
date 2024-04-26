package org.epics.archiverappliance.mgmt.pva.actions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.bpl.GetAllPVs;
import org.epics.archiverappliance.mgmt.bpl.PVsMatchingParameter;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Get all the PVs in the cluster. Note this call can return millions of PVs
 * <p>
 * Additional parameters which can be packaged as uri query parameters
 * pv - An optional argument that can contain a
 * <a href="http://en.wikipedia.org/wiki/Glob_%28programming%29">GLOB</a>
 * wildcard. We will return PVs that match this GLOB. For example, if
 * <code>pv=KLYS*</code>, the server will return all PVs that start with the
 * string <code>KLYS</code>. If both pv and regex are unspecified, we match
 * against all PVs.
 * <p>
 * regex - An optional argument that can contain a <a href=
 * "http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">Java
 * regex</a> wildcard. We will return PVs that match this regex. For example, if
 * <code>pv=KLYS*</code>, the server will return all PVs that start with the
 * string <code>KLYS</code>.
 * <p>
 * limit - An optional argument that specifies the number of matched PV's that
 * are retured. If unspecified, we return 500 PV names. To get all the PV names,
 * (potentially in the millions), set limit to &ndash;1.
 * <p>
 * example
 * <p>
 * request:
 * epics:nt/NTURI:1.0
 * <ul>
 * 		<li>string scheme pva
 * 		<li>string path getAllPVs
 * </ul>
 * <p>
 *
 * response
 * epics:nt/NTScalarArray:1.0
 * <ul>
 *     <li>string[] value ["pv1", "pv2", ....]
 * </ul>
 *
 * Based on {@link GetAllPVs}
 *
 * @author Kunal Shroff, mshankar
 *
 */
public class PvaGetAllPVs implements PvaAction {
    private static final Logger logger = LogManager.getLogger(PvaGetAllPVs.class.getName());

    public static final String NAME = "getAllPVs";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public PVAStructure request(PVAStructure args, ConfigService configService) throws PvaActionException {
        logger.debug("Getting all pvs for cluster");
        Map<String, String> searchParameters = new HashMap<String, String>();
        int defaultLimit = 500;
        PVAURI uri = PVAURI.fromStructure(args);
        PVAScalar<PVAStringArray> ntScalarArray = null;
        try {
            Map<String, String> queryName = uri.getQuery();
            if (queryName.containsKey("limit")) {
                searchParameters.put("limit", queryName.get("limit"));
            }
            LinkedList<String> pvNames =
                    PVsMatchingParameter.getMatchingPVs(searchParameters, configService, false, defaultLimit);
            ntScalarArray = PVAScalar.stringArrayScalarBuilder(pvNames.toArray(new String[pvNames.size()]))
                    .name("result")
                    .build();
        } catch (PVAScalarDescriptionNameException | NotValueException | PVAScalarValueNameException e) {
            throw new ResponseConstructionException(e);
        }
        return ntScalarArray;
    }
}
