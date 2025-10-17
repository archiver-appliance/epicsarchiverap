/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.cahdlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.retrieval.channelarchiver.ChannelArchiverReadOnlyPlugin;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Compares a specified period with data obtained from the channel archiver
 * @author mshankar
 *
 */
public class CompareWithChannelArchiver implements BPLAction {
    private static Logger logger = LogManager.getLogger(CompareWithChannelArchiver.class.getName());

    private static void addEventToEventList(
            LinkedList<HashMap<String, String>> retVals, DBRTimeEvent event, String src) {
        HashMap<String, String> eventData = new HashMap<String, String>();
        retVals.add(eventData);
        eventData.put("ts", TimeUtils.convertToHumanReadableString(event.getEpochSeconds()));
        eventData.put("nanos", Integer.toString(event.getEventTimeStamp().getNano()));
        eventData.put("stat", Integer.toString(event.getStatus()));
        eventData.put("sevr", Integer.toString(event.getSeverity()));
        eventData.put("src", src);
    }

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String pv = req.getParameter("pv");
        String channelArchiverServerURL = req.getParameter("serverURL");
        String channelArchiverKey = req.getParameter("archiveKey");
        String startTimeStr = req.getParameter("from");
        String endTimeStr = req.getParameter("to");
        String limitStr = req.getParameter("limit");

        if (pv == null
                || pv.equals("")
                || channelArchiverServerURL == null
                || channelArchiverServerURL.equals("")
                || channelArchiverKey == null
                || channelArchiverKey.equals("")) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        logger.info("Comparing data for " + pv + " with server " + channelArchiverServerURL + " using archive key "
                + channelArchiverKey);

        // ISO datetimes are of the form "2011-02-02T08:00:00.000Z"
        Instant end = TimeUtils.now();
        if (endTimeStr != null) {
            end = TimeUtils.convertFromISO8601String(endTimeStr);
        }

        // We get one day by default
        Instant start = TimeUtils.minusDays(end, 1);
        if (startTimeStr != null) {
            start = TimeUtils.convertFromISO8601String(startTimeStr);
        }

        int limit = 100;
        if (limitStr != null && !limitStr.equals("")) {
            limit = Integer.parseInt(limitStr);
        }

        LinkedList<HashMap<String, String>> retVals = new LinkedList<HashMap<String, String>>();

        RawDataRetrievalAsEventStream archClient = new RawDataRetrievalAsEventStream(
                configService.getMyApplianceInfo().getRetrievalURL() + "/data/getData.raw");
        ChannelArchiverReadOnlyPlugin caClient = new ChannelArchiverReadOnlyPlugin();
        caClient.initialize(
                "rtree://localhost"
                        + "?serverURL=" + URLEncoder.encode(channelArchiverServerURL, "UTF-8")
                        + "&archiveKey=" + channelArchiverKey,
                configService);

        try (BasicContext context = new BasicContext()) {
            try (EventStream archEventStream = archClient.getDataForPVS(new String[] {pv}, start, end, null)) {
                Iterator<Event> archEvents = archEventStream.iterator();
                try (EventStream caEventStream = new CurrentThreadWorkerEventStream(
                        pv, caClient.getDataForPV(context, pv, start, end, new DefaultRawPostProcessor()))) {
                    int eventCount = 0;
                    Iterator<Event> caEvents = caEventStream.iterator();
                    DBRTimeEvent archEvent = null;
                    DBRTimeEvent caEvent = null;
                    if (archEvents.hasNext()) archEvent = (DBRTimeEvent) archEvents.next();
                    if (caEvents.hasNext()) caEvent = (DBRTimeEvent) caEvents.next();
                    // We continue as long as there are events in either of the streams or as long as one of these items
                    // is not null
                    while ((archEvent != null || caEvent != null) && (eventCount < limit)) {
                        if (archEvent == null) {
                            logger.debug("We ran out of arch events as archEvent is null; moving to next CA event");
                            addEventToEventList(retVals, caEvent, "CA");
                            caEvent = caEvents.hasNext() ? ((DBRTimeEvent) caEvents.next()) : null;
                        } else if (caEvent == null) {
                            logger.debug("We ran out of CA events as caEvent is null; moving to next arch event");
                            addEventToEventList(retVals, archEvent, "arch");
                            archEvent = archEvents.hasNext() ? ((DBRTimeEvent) archEvents.next()) : null;
                        } else {
                            Instant archTs = archEvent.getEventTimeStamp();
                            Instant caTs = caEvent.getEventTimeStamp();

                            if (archTs.isAfter(caTs)) {
                                logger.debug("Arch event is after caEvent; moving to next CA event");
                                addEventToEventList(retVals, caEvent, "CA");
                                caEvent = caEvents.hasNext() ? ((DBRTimeEvent) caEvents.next()) : null;
                            } else if (archTs.isBefore(caTs)) {
                                logger.debug("Arch event is before caEvent; moving to next arch event");
                                addEventToEventList(retVals, archEvent, "arch");
                                archEvent = archEvents.hasNext() ? ((DBRTimeEvent) archEvents.next()) : null;
                            } else {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Skipping events with the same time stamp from both sources");
                                }
                                assert (archTs.equals(caTs));
                                caEvent = caEvents.hasNext() ? ((DBRTimeEvent) caEvents.next()) : null;
                                archEvent = archEvents.hasNext() ? ((DBRTimeEvent) archEvents.next()) : null;
                            }
                        }
                        eventCount++;
                    }
                }
            }
        }

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(retVals));
        }
    }
}
