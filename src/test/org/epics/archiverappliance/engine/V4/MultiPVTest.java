package org.epics.archiverappliance.engine.V4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.test.MemBufWriter;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVADoubleArray;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVATimeStamp;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.ServerPV;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.python.google.common.collect.Lists;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.formatInput;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.getReceivedValues;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.startArchivingPV;

public class MultiPVTest {

    // TODO Add enums

    private static final Logger logger = LogManager.getLogger(PVAccessTypesTest.class.getName());
    private static final String VALUE_STRING = "value";
    private static final ArchDBRTypes type = ArchDBRTypes.DBR_WAVEFORM_DOUBLE;

    /**
     * Generates a sequence of arrays of numbers up to maxInputNumber
     * <p>
     * For example with maxInputNumber: 10, multiplier: 2, nOfArrayGroups: 2
     * returns {{2, 4, 6, 8, 10}, {12, 14, 16, 18, 20}}
     *
     * @param numberOfPartitions Amount of partitions
     * @param multiplier         float multiplier to make values floats
     * @param partitionSize      Size of each partition
     * @return List of lists of fake data
     */
    private static List<List<Double>> fakeData(int numberOfPartitions, double multiplier, int partitionSize) {

        // If an array is shorter than the first input of the data
        // structure then the received array will have capacity
        // of the first one. Hence the elements will be filled
        // with the previous value.
        // So the size of each array must be the same.
        return Lists.partition(
                IntStream.range(0, numberOfPartitions * partitionSize)
                        .mapToDouble(i -> multiplier * i)
                        .boxed()
                        .collect(Collectors.toList()),
                partitionSize);
    }

    private static GeneratedServerPV getGenerateServerPV(PVAServer server, List<PVAData> dataSet) {
        String pvName = "PV:" + MultiPVTest.class.getSimpleName() + ":" + UUID.randomUUID();

        logger.info("Starting pvAccess test for type with pv " + pvName);
        PVAData value = dataSet.get(0).cloneData();
        HashMap<Instant, String> expectedData = new HashMap<>();
        Instant instant = Instant.now();
        expectedData.put(instant, formatInput(value));
        PVATimeStamp timeStamp = new PVATimeStamp(instant);
        String struct_name = "epics:nt/NTScalarArray:1.0";
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAInt("status", 0), new PVAInt("severity", 0));
        PVAStructure data = new PVAStructure("demo", struct_name, value, timeStamp, alarm);

        ServerPV serverPV = server.createPV(pvName, data);
        MemBufWriter writer = new MemBufWriter(pvName, type);
        return new GeneratedServerPV(pvName, expectedData, timeStamp, data, serverPV, writer);
    }

    /**
     * Test for a multiple pv of particular type.
     * <p>
     * Outline:
     * 1. Setup PVs in PVAServer
     * 2. Setup Archiver Storage
     * 3. Send PVs to be archived
     * 4. Wait for PVs to be setup
     * 5. Send data from inputData
     * 6. Convert Received Data into a hashmap
     * 7. Test received and sent data are the same
     *
     * @throws InterruptedException throw if interrupted
     */
    @Test
    public void multiPVTest() throws Exception {
        // Setup
        ConfigService configService = new ConfigServiceForTests(-1);
        PVAServer server = new PVAServer();

        // Create PVs

        var dataSet = fakeData(100, 1.1, 10).stream()
                .map((dArray) -> (PVAData) new PVADoubleArray(
                        VALUE_STRING,
                        dArray.stream().mapToDouble(Double::doubleValue).toArray()))
                .toList();
        List<GeneratedServerPV> generatedServerPVS = IntStream.rangeClosed(0, 20)
                .mapToObj((i) -> getGenerateServerPV(server, dataSet))
                .toList();

        // Start archiving all PVs
        generatedServerPVS.forEach((generatedServerPV) -> {
            try {
                startArchivingPV(generatedServerPV.pvName(), generatedServerPV.writer(), configService, type);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        long samplingPeriodMilliSeconds = 100;

        for (PVAData input : dataSet.subList(1, dataSet.size())) {
            Thread.sleep(samplingPeriodMilliSeconds);
            Instant instant = Instant.now();
            // Update all the PVs for each data
            generatedServerPVS.forEach((generatedServerPV) -> {
                PVAData newValue = generatedServerPV.data().get("value");
                try {
                    newValue.setValue(input);
                } catch (Exception e) {
                    Assertions.fail(e.getMessage());
                }
                generatedServerPV.expectedData().put(instant, formatInput(newValue));
                generatedServerPV.timeStamp().set(instant);
                try {
                    generatedServerPV.serverPV().update(generatedServerPV.data());
                } catch (Exception e) {
                    Assertions.fail(e.getMessage());
                }
            });
        }
        Thread.sleep(samplingPeriodMilliSeconds);

        generatedServerPVS.forEach((generatedServerPV) -> {
            Map<Instant, String> actualValues = null;
            try {
                actualValues = getReceivedValues(generatedServerPV.writer(), configService).entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey, (e) -> e.getValue().toString()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            Assertions.assertEquals(generatedServerPV.expectedData(), actualValues);
        });
        // Teardown
        configService.shutdownNow();
        server.close();
    }

    private record GeneratedServerPV(
            String pvName,
            HashMap<Instant, String> expectedData,
            PVATimeStamp timeStamp,
            PVAStructure data,
            ServerPV serverPV,
            MemBufWriter writer) {}
}
