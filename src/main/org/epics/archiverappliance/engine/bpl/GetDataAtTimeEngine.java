/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.mgmt.bpl.PVsMatchingParameter;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * PV for getting the data for multiple PV's from the engine's buffers at a particular time.
 * We loop thru all the values and find the latest value in the engine that is older or equal to the specified time.
 * If no such sample is present, we do not add the PV to the JSON response.
 * @author mshankar
 *
 */
public class GetDataAtTimeEngine implements BPLAction {
    private static final Logger logger = LogManager.getLogger(GetDataAtTimeEngine.class);

    /**
     * Evaluate a new event from an event stream to see if it is applicable as a source of data for getDataForTime.
     * We mostly want to find the latest event before or at the requested timestamp.
     * @param atTime
     * @param newEventToConsider
     * @param alreadyExistingEvent
     * @param fieldIsEmbeddedInStream
     * @param fieldName
     * @return
     */
    private static DBRTimeEvent evaluatePotentialEvent(
            Instant atTime,
            DBRTimeEvent newEventToConsider,
            DBRTimeEvent alreadyExistingEvent,
            boolean fieldIsEmbeddedInStream,
            String fieldName) {
        if (newEventToConsider != null && newEventToConsider.getEventTimeStamp().isBefore(atTime)
                || newEventToConsider.getEventTimeStamp().equals(atTime)) {
            if (alreadyExistingEvent != null) {
                if (newEventToConsider.getEventTimeStamp().isAfter(alreadyExistingEvent.getEventTimeStamp())) {
                    if (fieldIsEmbeddedInStream) {
                        if (newEventToConsider.hasFieldValues()
                                && newEventToConsider.getFieldValue(fieldName) != null) {
                            return newEventToConsider;
                        }
                    } else {
                        return newEventToConsider;
                    }
                }
            } else {
                if (fieldIsEmbeddedInStream) {
                    if (newEventToConsider.hasFieldValues() && newEventToConsider.getFieldValue(fieldName) != null) {
                        return newEventToConsider;
                    }
                } else {
                    return newEventToConsider;
                }
            }
        }
        return alreadyExistingEvent;
    }

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        List<String> pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req);
        logger.debug("Getting data at time for PVs " + pvNames.size());

        String timeStr = req.getParameter("at");
        Instant atTime = TimeUtils.now();
        if (timeStr != null) {
            atTime = TimeUtils.convertFromISO8601String(timeStr);
        }

        EngineContext engineContext = configService.getEngineContext();
        HashMap<String, HashMap<String, Object>> values = new HashMap<String, HashMap<String, Object>>();
        for (String pvName : pvNames) {
            String nameFromUser = pvName;
            String fieldName = PVNames.getFieldName(pvName);
            boolean fieldIsEmbeddedInStream = false;

            PVTypeInfo rootTypeInfo = PVNames.determineAppropriatePVTypeInfo(pvName, configService);
            if (rootTypeInfo == null) continue;
            pvName = rootTypeInfo.getPvName();

            if (fieldName != null
                    && Arrays.asList(rootTypeInfo.getArchiveFields()).contains(fieldName)) {
                fieldIsEmbeddedInStream = true;
            }

            if (engineContext.getChannelList().containsKey(pvName)) {
                ArchiveChannel archiveChannel = engineContext.getChannelList().get(pvName);
                ArrayListEventStream st = archiveChannel.getPVData();
                DBRTimeEvent potentialEvent = null;
                for (Event ev : st) {
                    potentialEvent = evaluatePotentialEvent(
                            atTime, (DBRTimeEvent) ev, potentialEvent, fieldIsEmbeddedInStream, fieldName);
                }
                if (archiveChannel.getLastArchivedValue() != null) {
                    potentialEvent = evaluatePotentialEvent(
                            atTime,
                            archiveChannel.getLastArchivedValue(),
                            potentialEvent,
                            fieldIsEmbeddedInStream,
                            fieldName);
                }

                if (potentialEvent != null) {
                    HashMap<String, Object> evnt = new HashMap<String, Object>();
                    evnt.put("secs", potentialEvent.getEpochSeconds());
                    evnt.put("nanos", potentialEvent.getEventTimeStamp().getNano());
                    evnt.put("severity", potentialEvent.getSeverity());
                    evnt.put("status", potentialEvent.getStatus());
                    if (fieldIsEmbeddedInStream) {
                        evnt.put(
                                "val",
                                JSONValue.parse(potentialEvent.getFields().get(fieldName)));
                    } else {
                        evnt.put(
                                "val",
                                JSONValue.parse(potentialEvent.getSampleValue().toJSONString()));
                    }
                    values.put(nameFromUser, evnt);
                }
            }
        }

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            JSONObject.writeJSONString(values, out);
        }
    }
}
