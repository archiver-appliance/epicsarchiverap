/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext.CommandThreadChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Details of a PV
 * @author mshankar
 *
 */
public class PVDetails implements org.epics.archiverappliance.common.reports.PVDetails {
    private static final Logger logger = LogManager.getLogger(PVDetails.class);

    @Override
    public LinkedList<Map<String, String>> pvDetails(ConfigService configService, String pvName) throws Exception {

        try {
            logger.info("Getting the detailed status for PV " + pvName);
            PVTypeInfo typeInfoForPV = configService.getTypeInfoForPV(pvName);
            if (typeInfoForPV == null) {
                logger.error("Unable to find typeinfo for PV " + pvName);
                throw new IOException("Unable to find typeinfo for PV " + pvName);
            }

            if (typeInfoForPV.isPaused()) {
                LinkedList<Map<String, String>> statuses = new LinkedList<Map<String, String>>();
                List<CommandThreadChannel> immortalChannelsForPV =
                        configService.getEngineContext().getAllChannelsForPV(pvName);
                if (immortalChannelsForPV.isEmpty()) {
                    addDetailedStatus(statuses, "Open channels", "0");
                } else {
                    for (CommandThreadChannel immortalChanel : immortalChannelsForPV) {
                        addDetailedStatus(
                                statuses,
                                "Channel still hanging around",
                                immortalChanel.getChannel().getName());
                    }
                }
                return statuses;
            }

            ArchDBRTypes dbrType = typeInfoForPV.getDBRType();
            PVMetrics metrics = ArchiveEngine.getMetricsforPV(pvName, configService);
            if (metrics != null) {
                LinkedList<Map<String, String>> statuses = metrics.getDetailedStatus();
                ArchiveEngine.getLowLevelStateInfo(pvName, configService, statuses);

                List<CommandThreadChannel> immortalChannelsForPV =
                        configService.getEngineContext().getAllChannelsForPV(pvName);
                if (immortalChannelsForPV.isEmpty()) {
                    addDetailedStatus(statuses, "Open channels", "0");
                } else {
                    for (CommandThreadChannel immortalChanel : immortalChannelsForPV) {
                        addDetailedStatus(
                                statuses,
                                "Open channels",
                                immortalChanel.getChannel().getName());
                        statuses.addAll(immortalChanel.getCommandThread().getCommandThreadDetails());
                    }
                }                

                if (dbrType.isV3Type()) {
                    ArchiveChannel channel =
                            configService.getEngineContext().getChannelList().get(pvName);
                    if (channel != null) {
                        int metaFieldCount = channel.getMetaChannelCount();
                        int connectedMetaFieldCount = channel.getConnectedMetaChannelCount();
                        addDetailedStatus(statuses, "Channels for the extra fields", "" + metaFieldCount);
                        addDetailedStatus(
                                statuses, "Connected channels for the extra fields", "" + connectedMetaFieldCount);
                        addDetailedStatus(
                                statuses,
                                "Sample buffer capacity",
                                "" + channel.getSampleBuffer().getCapacity());
                        addDetailedStatus(
                                statuses,
                                "Time elapsed since search request (s)",
                                "" + channel.getSecondsElapsedSinceSearchRequest());
                    }
                }
                return statuses;
            } else {
                logger.error("No status for PV " + pvName + " in this engine.");
                throw new IOException("No status for PV " + pvName + " in this engine.");
            }
        } catch (Exception ex) {
            logger.error("Exception getting details for PV " + pvName, ex);
            throw new IOException(ex);
        }
    }

    private static void addDetailedStatus(LinkedList<Map<String, String>> statuses, String name, String value) {
        Map<String, String> obj = new LinkedHashMap<String, String>();
        obj.put("name", name);
        obj.put("value", value);
        obj.put("source", "pv");
        statuses.add(obj);
    }
}
