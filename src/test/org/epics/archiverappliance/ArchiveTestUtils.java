package org.epics.archiverappliance;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.test.MemBufWriter;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Shared waiting and retrieval utilities for integration tests.
 * General-purpose helpers that do not depend on PVA/V4 specifics.
 */
public class ArchiveTestUtils {

    private static final Logger logger = LogManager.getLogger(ArchiveTestUtils.class);

    // -------------------------------------------------------------------------
    // Engine-level condition helpers
    // -------------------------------------------------------------------------

    /**
     * Waits up to 15 seconds for a PV to appear in the channel list and report
     * {@code isConnected() == true} via {@link ArchiveEngine#getMetricsforPV}.
     */
    public static void waitForPVConnected(String pvName, ConfigService configService) {
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> {
            var m = ArchiveEngine.getMetricsforPV(pvName, configService);
            return m != null && m.isConnected();
        });
    }

    /**
     * Waits up to 10 seconds for a PV channel to appear in the engine channel list.
     */
    public static void waitForChannelInList(String pvName, ConfigService configService) {
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> configService.getEngineContext().getChannelList().get(pvName) != null);
    }

    /**
     * Waits up to 10 seconds for a PV channel to be removed from the engine channel list.
     */
    public static void waitForChannelRemovedFromList(String pvName, ConfigService configService) {
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> configService.getEngineContext().getChannelList().get(pvName) == null);
    }

    // -------------------------------------------------------------------------
    // Retrieval helpers
    // -------------------------------------------------------------------------

    /**
     * Polls the raw retrieval API until at least one event is available for the given PV.
     * Fails the test if no data appears within 5 minutes.
     */
    public static void waitForData(String pvName, String retrievalURL) {
        waitForData(pvName, 1, retrievalURL);
    }

    /**
     * Polls the raw retrieval API until at least {@code minEvents} events are available for the given PV.
     * Fails the test if the condition is not met within 5 minutes.
     */
    public static void waitForData(String pvName, int minEvents, String retrievalURL) {
        Awaitility.await()
                .pollInterval(5, TimeUnit.SECONDS)
                .atMost(5, TimeUnit.MINUTES)
                .until(() -> {
                    RawDataRetrievalAsEventStream raw = new RawDataRetrievalAsEventStream(retrievalURL);
                    java.time.Instant end = TimeUtils.plusDays(TimeUtils.now(), 1);
                    java.time.Instant start = TimeUtils.minusDays(end, 8);
                    try (EventStream stream = raw.getDataForPVS(new String[] {pvName}, start, end, null)) {
                        if (stream == null) return false;
                        int count = 0;
                        for (Event ignored : stream) {
                            if (++count >= minEvents) return true;
                        }
                        return false;
                    } catch (IOException e) {
                        return false;
                    }
                });
    }

    // -------------------------------------------------------------------------
    // Management API helpers
    // -------------------------------------------------------------------------

    public static void waitForStatusChange(String pvName, String expectedStatus, int maxTries, String mgmtUrl) {
        waitForStatusChange(pvName, expectedStatus, maxTries, mgmtUrl, 5);
    }

    public static void waitForStatusChange(
            String pvName, String expectedStatus, int maxTries, String mgmtUrl, long waitPeriodSeconds) {
        Awaitility.await()
                .pollInterval(waitPeriodSeconds, TimeUnit.SECONDS)
                .atMost(maxTries * waitPeriodSeconds, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expectedStatus, getCurrentStatus(pvName, mgmtUrl)));
    }

    private static String getCurrentStatus(String pvName, String mgmtUrl) {
        String statusPVURL = mgmtUrl + "getPVStatus?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        JSONArray pvStatus = GetUrlContent.getURLContentAsJSONArray(statusPVURL);
        String status = ((JSONObject) pvStatus.get(0)).get("status").toString();
        logger.debug("status is " + status);
        return status;
    }

    public static void waitForPVDetail(
            String pvName,
            String detailName,
            String expectedValue,
            int maxTries,
            String mgmtUrl,
            long waitPeriodSeconds) {
        Awaitility.await()
                .pollInterval(waitPeriodSeconds, TimeUnit.SECONDS)
                .atMost(maxTries * waitPeriodSeconds, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expectedValue, getPVDetail(pvName, mgmtUrl, detailName)));
    }

    public static String getPVDetail(String pvName, String mgmtUrl, String detailName) {
        String pvDetailsURL = mgmtUrl + "getPVDetails?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        List<Map<String, String>> pvDetails =
                (List<Map<String, String>>) GetUrlContent.getURLContentAsJSONArray(pvDetailsURL);
        for (Map<String, String> pvDetail : pvDetails) {
            if (pvDetail.get("name").equals(detailName)) {
                return pvDetail.get("value");
            }
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Writer helpers
    // -------------------------------------------------------------------------

    public static Map<Instant, SampleValue> getReceivedValues(MemBufWriter writer, ConfigService configService)
            throws Exception {
        return getReceivedEvents(writer, configService).entrySet().stream()
                .map((e) -> Map.entry(e.getKey(), e.getValue().getSampleValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static HashMap<Instant, Event> getReceivedEvents(MemBufWriter writer, ConfigService configService)
            throws Exception {
        double secondsToBuffer = configService.getEngineContext().getWritePeriod();
        Thread.sleep((long) secondsToBuffer * 1000);

        HashMap<Instant, Event> actualValues = new HashMap<>();
        try {
            for (Event event : writer.getCollectedSamples()) {
                actualValues.put(event.getEventTimeStamp(), event);
            }
        } catch (IOException e) {
            Assertions.fail(e.getMessage());
        }
        return actualValues;
    }
}
