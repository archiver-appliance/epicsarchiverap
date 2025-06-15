package org.epics.archiverappliance.engine.V4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.test.MemBufWriter;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.pva.actions.NTUtil;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetPVStatus;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.data.Hexdump;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.PVATypeRegistry;
import org.epics.pva.data.nt.MustBeArrayException;
import org.epics.pva.data.nt.PVATable;
import org.epics.pva.data.nt.PVATimeStamp;
import org.epics.pva.server.ServerPV;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class PVAccessUtil {

    private static final Logger logger = LogManager.getLogger(PVAccessUtil.class);

    public static Map<Instant, SampleValue> getReceivedValues(MemBufWriter writer, ConfigService configService)
            throws Exception {

        return getReceivedEvents(writer, configService).entrySet().stream()
                .map((e) -> Map.entry(e.getKey(), e.getValue().getSampleValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static HashMap<Instant, Event> getReceivedEvents(MemBufWriter writer, ConfigService configService)
            throws Exception {
        double secondsToBuffer = configService.getEngineContext().getWritePeriod();
        // Need to wait for the writer to write all the received data.
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

    public static Map.Entry<Instant, PVAStructure> updateStructure(PVAStructure pvaStructure, ServerPV serverPV) {
        try {
            ((PVAStructure) pvaStructure.get("structure"))
                    .get("level 1")
                    .setValue(new PVAString("level 1", "level 1 0 new"));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        Instant instant = Instant.now();
        ((PVATimeStamp) pvaStructure.get("timeStamp")).set(instant);
        try {
            serverPV.update(pvaStructure);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        return Map.entry(instant, pvaStructure);
    }

    public static ArchiveChannel startArchivingPV(
            String pvName, MemBufWriter writer, ConfigService configService, ArchDBRTypes type)
            throws InterruptedException {
        return startArchivingPV(pvName, writer, configService, type, true, new String[0]);
    }

    public static ArchiveChannel startArchivingPV(
            String pvName,
            MemBufWriter writer,
            ConfigService configService,
            ArchDBRTypes type,
            boolean wait,
            String[] metaFields)
            throws InterruptedException {

        long samplingPeriodMilliSeconds = 100;
        float samplingPeriod = (float) samplingPeriodMilliSeconds / (float) 1000.0;
        try {
            ArchiveEngine.archivePV(
                    pvName,
                    samplingPeriod,
                    PolicyConfig.SamplingMethod.MONITOR,
                    writer,
                    configService,
                    type,
                    null,
                    metaFields,
                    true,
                    false);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        ArchiveChannel pvChannel =
                configService.getEngineContext().getChannelList().get(pvName);
        try {
            pvChannel.startUpMetaChannels();
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        if (wait) {

            waitForIsConnected(pvChannel);

            // Update no fields

            Thread.sleep(samplingPeriodMilliSeconds + 1000);
        }
        return pvChannel;
    }

    public static void waitForIsConnected(ArchiveChannel pvChannel) {
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        Assertions.assertTrue(!pvChannel.metaChannelsNeedStartingUp() && pvChannel.isConnected()));
    }

    /**
     * to string formats as "type_name value actual_value"
     */
    public static String formatInput(PVAData value) {
        String dataString = value.toString();
        var firstValueSubString = dataString.indexOf("value", 0);

        return dataString.substring(firstValueSubString + 5).replaceAll(" ", "");
    }

    public static void waitForStatusChange(String pvName, String expectedStatus, int maxTries, String mgmtUrl) {
        waitForStatusChange(pvName, expectedStatus, maxTries, mgmtUrl, 5);
    }

    public static void waitForStatusChange(
            String pvName, String expectedStatus, int maxTries, String mgmtUrl, long waitPeriodSeconds) {
        Awaitility.await()
                .pollInterval(waitPeriodSeconds, TimeUnit.SECONDS)
                .atMost(maxTries * waitPeriodSeconds, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expectedStatus, getCurentStatus(pvName, mgmtUrl)));
    }

    private static String getCurentStatus(String pvName, String mgmtUrl) {
        String curentStatus;
        // Check archiving
        String statusPVURL = mgmtUrl + "getPVStatus?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        JSONArray pvStatus = GetUrlContent.getURLContentAsJSONArray(statusPVURL);
        curentStatus = ((JSONObject) pvStatus.get(0)).get("status").toString();
        logger.debug("status is " + curentStatus);
        return curentStatus;
    }

    public static void waitForPVDetail(
            String pvName, String detailName, String expectedValue, int maxTries, String mgmtUrl, long waitPeriodSeconds) {
        Awaitility.await()
                .pollInterval(waitPeriodSeconds, TimeUnit.SECONDS)
                .atMost(maxTries * waitPeriodSeconds, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expectedValue, getPVDetail(pvName, mgmtUrl, detailName)));
    }

    public static String getPVDetail(String pvName, String mgmtUrl, String detailName) {
        String pvDetailsURL = mgmtUrl + "getPVDetails?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        List<Map<String, String>> pvDetails = (List<Map<String, String>>) GetUrlContent.getURLContentAsJSONArray(pvDetailsURL);
        for(Map<String, String> pvDetail : pvDetails){
            if(pvDetail.get("name").equals(detailName)) {
                return pvDetail.get("value");
            }
        }
        return null;
    }


    /**
     * Bytes to string method for debugging the byte buffers.
     *
     * @param data Input bytes
     * @return String representation as hexbytes and ascii conversion
     */
    public static String bytesToString(final ByteBuffer data) {
        ByteBuffer buffer = data.duplicate();

        return Hexdump.toHexdump(buffer);
    }

    public static PVAData fromGenericSampleValueToPVAData(SampleValue sampleValue, PVATypeRegistry types)
            throws Exception {
        ByteBuffer bytes = sampleValue.getValueAsBytes();
        var val = types.decodeType("struct name", bytes);
        val.decode(types, bytes);
        return val;
    }

    public static Map<Instant, PVAData> convertBytesToPVAStructure(Map<Instant, SampleValue> actualValues) {
        PVATypeRegistry types = new PVATypeRegistry();
        return actualValues.entrySet().stream()
                .map((e) -> {
                    try {
                        return Map.entry(e.getKey(), fromGenericSampleValueToPVAData(e.getValue(), types));
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static PVAStructure getCurrentStatus(List<String> pvNames, PVAChannel pvaChannel)
            throws ExecutionException, InterruptedException, TimeoutException, MustBeArrayException {

        PVATable archivePvStatusReqTable = PVATable.PVATableBuilder.aPVATable()
                .name(PvaGetPVStatus.NAME)
                .descriptor(PvaGetPVStatus.NAME)
                .addColumn(new PVAStringArray("pv", pvNames.toArray(new String[0])))
                .build();
        return pvaChannel.invoke(archivePvStatusReqTable).get(30, TimeUnit.SECONDS);
    }

    public static HashMap<String, String> getStatuses(List<String> pvNamesAll, PVAChannel pvaChannel)
            throws ExecutionException, InterruptedException, TimeoutException, MustBeArrayException {
        var statuses = NTUtil.extractStringArray(
                PVATable.fromStructure(getCurrentStatus(pvNamesAll, pvaChannel)).getColumn("status"));
        var pvs = NTUtil.extractStringArray(
                PVATable.fromStructure(getCurrentStatus(pvNamesAll, pvaChannel)).getColumn("pv"));
        var result = new HashMap<String, String>();
        for (int i = 0; i < pvs.length; i++) {
            result.put(pvs[i], statuses[i]);
        }
        return result;
    }
}
