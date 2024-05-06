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
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.mgmt.bpl.GetPVStatusAction;
import org.epics.archiverappliance.mgmt.bpl.PVsMatchingParameter;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.MustBeArrayException;
import org.epics.pva.data.nt.PVATable;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Get the status of a PV.
 * <p>
 * pv - The name(s) of the pv for which status is to be determined.
 * <p>
 * example request
 * <p>
 * epics:nt/NTTable:1.0
 * <ul>
 *   <li>string[] labels [pv]</li>
 *   <li>structure value
 *      <ul>
 *      <li>string[] pv [test_0,test_1,test_10,test_100...]</li>
 *      </ul>
 * </ul>
 * <p>
 * example result:
 * <p>
 * epics:nt/NTTable:1.0
 * <ul>
 *    <li>string[] labels [pv,status,appliance,connectionState,lastEvent,samplingPeriod,isMonitored,connectionFirstEstablished,connectionLossRegainCount,connectionLastRestablished]</li>
 *    <li>structure value
 *       <ul>
 *         <li>string[] pv [test_0,test_1,test_10,test_100...]</li>
 *         <li>string[] status [Being archived,Initial sampling,Being archived,Being archived,...]</li>
 *         <li>string[] connectionState [true,null,true,true,null,true,null,true,null,...]</li>
 *         <li>string[] lastEvent [Feb/01/2018 13:18:36 -05:00,null,Feb/01/2018 13:18:37 -05:00,Feb/01/2018 13:18:37 -05:00,...]</li>
 *         <li>string[] samplingPeriod [1.0,null,1.0,1.0,null,1.0,null,...]</li>
 *         <li>string[] isMonitored [true,null,true,true,null,true,...]</li>
 *         <li>string[] connectionFirstEstablished [Feb/01/2018 13:17:10 -05:00,null,...]</li>
 *         <li>string[] connectionLossRegainCount [0,null,0,0,null,0,null,...]</li>
 *         <li>string[] connectionLastRestablished [Never,null,Never,Never,null,...]</li>
 *       </ul>
 *    </li>
 *  </ul>
 * @author mshankar, shroffk
 *
 */
public class PvaGetPVStatus implements PvaAction {
    private static final Logger logger = LogManager.getLogger(PvaGetPVStatus.class);

    public static final String NAME = "PVStatus";

    /**
     * There is a 1-many mapping between the name the user asks for and the internals.
     * When sending the data back, we need to take the "real" information and create an entry for each user request.
     * This method maintains this list.
     * @param pvNameFromRequest
     * @param pvName
     * @param realName2NameFromRequest
     */
    private static void addInverseNameMapping(
            String pvNameFromRequest, String pvName, HashMap<String, LinkedList<String>> realName2NameFromRequest) {
        if (!pvName.equals(pvNameFromRequest)) {
            if (!realName2NameFromRequest.containsKey(pvName)) {
                realName2NameFromRequest.put(pvName, new LinkedList<String>());
            }
            realName2NameFromRequest.get(pvName).add(pvNameFromRequest);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PVAStructure request(PVAStructure args, ConfigService configService) throws PvaActionException {
        Map<String, String> requestParameters = new HashMap<>();
        PVATable table = PVATable.fromStructure(args);
        List<String> pvs = List.of();
        if (table != null) {
            pvs = NTUtil.extractStringList(table.getColumn("pv"));
        } else {
            throw new IllegalArgumentException("Only supports request args of type NTURI or NTTable ");
        }
        LinkedList<String> pvNames = PVsMatchingParameter.getMatchingPVs(pvs, null, -1, configService, true);

        HashMap<String, Map<String, String>> pvStatuses = new HashMap<String, Map<String, String>>();
        HashMap<String, LinkedList<String>> pvNamesToAskEngineForStatus = new HashMap<String, LinkedList<String>>();
        HashMap<String, PVTypeInfo> typeInfosForEngineRequests = new HashMap<String, PVTypeInfo>();
        HashMap<String, LinkedList<String>> realName2NameFromRequest = new HashMap<String, LinkedList<String>>();

        GetPVStatusAction.getPVStatuses(
                configService,
                pvNames,
                pvStatuses,
                pvNamesToAskEngineForStatus,
                typeInfosForEngineRequests,
                realName2NameFromRequest);

        LinkedList<String> pv = new LinkedList<>();
        LinkedList<String> status = new LinkedList<>();
        LinkedList<String> appliance = new LinkedList<>();
        LinkedList<String> connectionState = new LinkedList<>();
        LinkedList<String> lastEvent = new LinkedList<>();
        LinkedList<String> samplingPeriod = new LinkedList<>();
        LinkedList<String> isMonitored = new LinkedList<>();
        LinkedList<String> connectionFirstEstablished = new LinkedList<>();
        LinkedList<String> connectionLossRegainCount = new LinkedList<>();
        LinkedList<String> connectionLastRestablished = new LinkedList<>();
        pvStatuses.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEachOrdered(entry -> {
            pv.add(entry.getKey());
            status.add(entry.getValue().get("status"));
            appliance.add(entry.getValue().get("appliance"));
            connectionState.add(entry.getValue().get("connectionState"));
            lastEvent.add(entry.getValue().get("lastEvent"));
            samplingPeriod.add(entry.getValue().get("samplingPeriod"));
            isMonitored.add(entry.getValue().get("isMonitored"));
            connectionFirstEstablished.add(entry.getValue().get("connectionFirstEstablished"));
            connectionLossRegainCount.add(entry.getValue().get("connectionLossRegainCount"));
            connectionLastRestablished.add(entry.getValue().get("connectionLastRestablished"));
        });
        try {
            return PVATable.PVATableBuilder.aPVATable()
                    .name("result")
                    .addColumn(new PVAStringArray("pv", pv.toArray(new String[pv.size()])))
                    .addColumn(new PVAStringArray("status", status.toArray(new String[status.size()])))
                    .addColumn(new PVAStringArray("appliance", appliance.toArray(new String[appliance.size()])))
                    .addColumn(new PVAStringArray(
                            "connectionState", connectionState.toArray(new String[connectionState.size()])))
                    .addColumn(new PVAStringArray("lastEvent", lastEvent.toArray(new String[lastEvent.size()])))
                    .addColumn(new PVAStringArray(
                            "samplingPeriod", samplingPeriod.toArray(new String[samplingPeriod.size()])))
                    .addColumn(new PVAStringArray("isMonitored", isMonitored.toArray(new String[isMonitored.size()])))
                    .addColumn(new PVAStringArray(
                            "connectionFirstEstablished",
                            connectionFirstEstablished.toArray(new String[connectionFirstEstablished.size()])))
                    .addColumn(new PVAStringArray(
                            "connectionLossRegainCount",
                            connectionLossRegainCount.toArray(new String[connectionLossRegainCount.size()])))
                    .addColumn(new PVAStringArray(
                            "connectionLastRestablished",
                            connectionLastRestablished.toArray(new String[connectionLastRestablished.size()])))
                    .build();
        } catch (MustBeArrayException e) {
            throw new ResponseConstructionException(e);
        }
    }
}
