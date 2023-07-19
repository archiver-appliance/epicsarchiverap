package org.epics.archiverappliance.retrieval.pva;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.epics.pva.data.PVAAnyArray;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVADouble;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.PVAStructureArray;
import org.epics.pva.data.PVAny;
import org.epics.pva.data.nt.MustBeArrayException;
import org.epics.pva.data.nt.PVATable;
import org.epics.pva.data.nt.PVATimeStamp;
import org.epics.pva.data.nt.PVAURI;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.ServerPV;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.awaitility.pollinterval.FibonacciPollInterval.fibonacci;
import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.epics.archiverappliance.retrieval.pva.PvaDataRetrievalService.PVA_DATA_SERVICE;

@Tag("integration")
@Tag("localEpics")
public class PvaGetPVDataTest {

    private static final Logger logger = LogManager.getLogger(PvaGetPVDataTest.class.getName());
    static TomcatSetup tomcatSetup = new TomcatSetup();
    private static PVAServer pvaServer;
    private static PVAClient pvaClient;
    private static PVAChannel pvaMgmtChannel;
    private static PVAChannel pvaRetrievalChannel;

    @BeforeAll
    public static void setup() throws Exception {

        tomcatSetup.setUpWebApps(PvaGetPVDataTest.class.getSimpleName());

        pvaServer = new PVAServer();
        pvaClient = new PVAClient();
        pvaMgmtChannel = pvaClient.getChannel(PVA_MGMT_SERVICE);
        pvaMgmtChannel.connect().get(5, TimeUnit.SECONDS);
        pvaRetrievalChannel = pvaClient.getChannel(PVA_DATA_SERVICE);
        pvaRetrievalChannel.connect().get(5, TimeUnit.SECONDS);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        pvaMgmtChannel.close();
        pvaRetrievalChannel.close();
        tomcatSetup.tearDown();
        pvaServer.close();
        pvaClient.close();
    }

    /**
     * to string formats as "value type_name actual_value"
     */
    String formatInput(PVAData value) {
        String[] splitStrings = value.toString().split(" ");
        String[] subArray = ArrayUtils.subarray(splitStrings, 2, splitStrings.length);

        return String.join("", subArray);
    }

    private static void archivePVsViaPVAccess(List<String> pvNames) throws MustBeArrayException, InterruptedException, ExecutionException, TimeoutException {
        PVATable archivePvStatusReqTable = PVATable.PVATableBuilder.aPVATable().name(PvaArchivePVAction.NAME)
                .descriptor(PvaArchivePVAction.NAME)
                .addColumn(new PVAStringArray("pv",  pvNames.stream().map(n -> "pva://" + n).toArray(String[]::new)))
                .build();
        pvaMgmtChannel.invoke(archivePvStatusReqTable).get(30, TimeUnit.SECONDS);
        Map<String, String> archivingStatus = new HashMap<>();
        for (String pvName: pvNames) {
            archivingStatus.put(pvName, "Being archived");
        }

        Awaitility.await()
                .pollInterval(fibonacci(TimeUnit.SECONDS))
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(() ->
                        Assertions.assertEquals(archivingStatus, PVAccessUtil.getStatuses(pvNames, pvaMgmtChannel))
                );
    }

    @Test
    public void testSingleRequest() throws Exception {

        // Create a pv

        String pvName = "PV:" + PvaGetPVDataTest.class.getSimpleName() + ":"
                + UUID.randomUUID();

        logger.info("Starting pvAccess test for pv " + pvName);
        var value = new PVADouble("value", 10.0);
        HashMap<Instant, String> expectedData = new HashMap<>();
        Instant instant = Instant.now();
        expectedData.put(instant, formatInput(value));
        PVATimeStamp timeStamp = new PVATimeStamp(instant);
        String struct_name = "epics:nt/NTScalar:1.0";
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAInt("status", 0), new PVAInt("severity", 0));
        PVAStructure data = new PVAStructure("demo", struct_name, value,
                timeStamp, alarm);
        ServerPV serverPV = pvaServer.createPV(pvName, data);

        Thread.sleep(1000);

        // Archive pv using pv access

        // Submit all the even named pv's to be archived
        logger.info("Submitting pv " + pvName + " to be archived.");

        List<String> pvNames = new ArrayList<>();
        pvNames.add(pvName);
        archivePVsViaPVAccess(pvNames);
        // Update a value

        Thread.sleep(100);
        instant = Instant.now();
        value.set(6.0);
        timeStamp.set(instant);
        expectedData.put(instant, formatInput(value));
        serverPV.update(data);

        // Get data from archiver via pv access
        // Wait for the writer to write the data
        Thread.sleep(30 * 1000);
        PVAAnyArray valuesAnyArray = getRetrievalPvaAnyArray(pvNames).get("value");

        PVAny[] allPVValues = valuesAnyArray.get();
        Map<Instant, String> actualData = new HashMap<>();
        for (PVAny any: allPVValues) {
            PVAStructureArray structureArray = any.get();
            for (PVAStructure structure: structureArray.get()) {
                actualData.put(PVATimeStamp.getTimeStamp(structure).instant(), formatInput(structure.get("value")));
            }
        }

        // Check data is expected
        Assertions.assertEquals(expectedData, actualData);
    }

    private static PVAStructure getRetrievalPvaAnyArray(List<String> pvNames) throws InterruptedException, ExecutionException, TimeoutException {
        logger.info("Retrieving data of pv " + pvNames);
        Map<String, String> query = new HashMap<>();
        query.put("pv", String.join(";", pvNames));
        PVAURI getPVDataURI = new PVAURI("name", "pva",null, PvaGetPVData.NAME,query );
        PVAStructure result = pvaRetrievalChannel.invoke(getPVDataURI).get(20, TimeUnit.SECONDS);
        logger.info("retrieved " + result);
        return result;
    }

    @Test
    public void testMultiRequest() throws Exception {

        // Create a set of pvs

        String randomModifier = String.valueOf(UUID.randomUUID());
        List<String> pvNames = IntStream.range(0, 10).mapToObj((i) -> "PV:" + i + ":" + PvaGetPVDataTest.class.getSimpleName() + ":"
                + randomModifier).toList();

        logger.info("Starting pvAccess test for pv " + pvNames);

        Instant instant = Instant.now();
        Map<String, HashMap<Instant, String>> expectedData = new HashMap<>();
        Instant finalInstant = instant;
        Map<String, PVAStructure> structures = pvNames.stream().map((pvName) -> {
            var value = new PVAString("value", pvName + "0");
            PVATimeStamp timeStamp = new PVATimeStamp(finalInstant);
            String struct_name = "epics:nt/NTScalar:1.0";
            var alarm = new PVAStructure("alarm", "alarm_t", new PVAInt("status", 0), new PVAInt("severity", 0));
            return new PVAStructure(pvName, struct_name, value,
                    timeStamp, alarm);
        }).collect(Collectors.toMap(PVAStructure::getName, (s) -> s));

        structures.forEach((key, value) -> {
            HashMap<Instant, String> initialMap = new HashMap<>();
            initialMap.put(finalInstant, formatInput(value.get("value")));
            expectedData.put(key,initialMap);
        });

        Map<String, ServerPV> serverPVS = structures.entrySet().stream().map((e) ->
        pvaServer.createPV(e.getKey(), e.getValue())).collect(Collectors.toMap(ServerPV::getName, (s) -> s));
        Thread.sleep(1000);

        // Archive pv using pv access

        // Submit all the even named pv's to be archived
        logger.info("Submitting pv " + pvNames + " to be archived.");

        archivePVsViaPVAccess(pvNames);
        // Update a value

        Thread.sleep(100);
        instant = Instant.now();
        Instant finalInstant1 = instant;
        structures.forEach((key, value) -> {
            PVAString stringValue = value.get("value");
            stringValue.set(key + "1");
            PVATimeStamp timeStamp = value.get("timeStamp");
            timeStamp.set(finalInstant1);
            HashMap<Instant, String> pvData = expectedData.get(key);
            pvData.put(finalInstant1, formatInput(stringValue));
            try {
                serverPVS.get(key).update(value);
            } catch (Exception e) {
                logger.error("Exception occurred in updating serverPV " + key, e);
            }
        });

        // Get data from archiver via pv access
        // Wait for the writer to write the data
        Thread.sleep(30 * 1000);
        PVAStructure result = getRetrievalPvaAnyArray(pvNames);
        PVAStringArray pvaLabels = result.get("labels");
        String[] labels = pvaLabels.get();
        PVAAnyArray pvaAnyArray = result.get("value");
        PVAny[] anyArray = pvaAnyArray.get();

        Map<String, Map<Instant, String>> actualData = new HashMap<>();
        for (int i = 0; i < labels.length; i++) {
            Map<Instant, String> pvDataResult = new HashMap<>();
            PVAStructureArray structureArray = anyArray[i].get();
            for (PVAStructure structure: structureArray.get()) {
                pvDataResult.put(PVATimeStamp.getTimeStamp(structure).instant(), formatInput(structure.get("value")));
            }
            actualData.put(labels[i], pvDataResult);
        }

        // Check data is expected
        Assertions.assertEquals(expectedData, actualData);
    }
}