package org.epics.archiverappliance.engine.V4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.test.MemBufWriter;
import org.epics.pva.data.*;
import org.epics.pva.data.nt.PVATimeStamp;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.ServerPV;
import org.junit.Test;
import org.python.google.common.collect.Lists;

import java.io.File;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Map.entry;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test for each pvAccess type that it can be archived successfully.
 */
public class PVAccessTypesTest {

    private static final Logger logger = LogManager.getLogger(PVAccessTypesTest.class.getName());
    private static final String VALUE_STRING = "value";

    /**
     * Generates a sequence of arrays of numbers up to maxInputNumber
     * <p>
     * For example with maxInputNumber: 10, multiplier: 2, nOfArrayGroups: 2
     * returns {{2, 4, 6, 8, 10}, {12, 14, 16, 18, 20}}
     * 
     * @param numberOfPartitions Amount of partitions
     * @param multiplier float multiplier to make values floats
     * @param partitionSize Size of each partition
     * @return List of lists of fake data
     */
    private static List<List<Double>> fakeData(int numberOfPartitions, double multiplier, int partitionSize) {

        // If an array is shorter than the first input of the data
        // structure then the received array will have capacity
        // of the first one. Hence the elements will be filled
        // with the previous value.
        // So the size of each array must be the same.
        return Lists.partition(
                IntStream.range(0, numberOfPartitions * partitionSize).mapToDouble(i -> multiplier * i)
                        .boxed()
                        .collect(Collectors.toList()),
                partitionSize);
    }

    /**
     * Provides the input data for the test cases.
     * <p>
     * First goes over every scalar type, converting the first array of the
     * generated fake data to the PVAccess type.
     * Seconds goes over every waveform type, converting each array of the
     * generated fake data to the PVAccess type.
     * 
     * @return input data
     */
    public static Map<ArchDBRTypes, List<PVAData>> data() {
        List<List<Double>> fakeData = fakeData(100, 1.1, 10);
        return Map.ofEntries(
                entry(ArchDBRTypes.DBR_SCALAR_STRING,
                        fakeData.get(0).stream()
                                .map((d) -> (PVAData) new PVAString(VALUE_STRING,
                                        d.toString()))
                                .toList()),
                entry(ArchDBRTypes.DBR_SCALAR_SHORT,
                        fakeData.get(0).stream().map(Double::shortValue)
                                .map((s) -> (PVAData) new PVAShort(VALUE_STRING, false,
                                        s))
                                .toList()),
                entry(ArchDBRTypes.DBR_SCALAR_FLOAT,
                        fakeData.get(0).stream().map(Double::floatValue)
                                .map((f) -> (PVAData) new PVAFloat(VALUE_STRING, f))
                                .toList()),
                entry(ArchDBRTypes.DBR_SCALAR_BYTE,
                        fakeData.get(0).stream().map(Double::byteValue)
                                .map((b) -> (PVAData) new PVAByte(VALUE_STRING, false,
                                        b))
                                .toList()),
                entry(ArchDBRTypes.DBR_SCALAR_INT,
                        fakeData.get(0).stream().map(Double::intValue)
                                .map((i) -> (PVAData) new PVAInt(VALUE_STRING, false,
                                        i))
                                .toList()),
                entry(ArchDBRTypes.DBR_SCALAR_DOUBLE,
                        fakeData.get(0).stream()
                                .map((d) -> (PVAData) new PVADouble(VALUE_STRING, d))
                                .toList()),
                entry(ArchDBRTypes.DBR_WAVEFORM_STRING,
                     fakeData.stream()
                             .map((dArray) -> (PVAData) new PVAStringArray(
                                     VALUE_STRING,
                                     dArray.stream().map(Object::toString)
                                             .toArray(String[]::new)))
                             .toList()),
                entry(ArchDBRTypes.DBR_WAVEFORM_SHORT,
                        fakeData.stream()
                                .map((dArray) -> {
                                    short[] array = new short[dArray.size()];
                                    int count = 0;
                                    for (Double d : dArray) {
                                        array[count] = d.shortValue();
                                        count++;
                                    }
                                    return (PVAData) new PVAShortArray(VALUE_STRING,
                                            false, array);
                                }).toList()),
                entry(ArchDBRTypes.DBR_WAVEFORM_FLOAT,
                        fakeData.stream()
                                .map((dArray) -> {
                                    float[] array = new float[dArray.size()];
                                    int count = 0;
                                    for (Double d : dArray) {
                                        array[count] = d.floatValue();
                                        count++;
                                    }
                                    return (PVAData) new PVAFloatArray(VALUE_STRING,
                                            array);
                                }).toList()),
                entry(ArchDBRTypes.DBR_WAVEFORM_BYTE,
                        fakeData.stream()
                                .map((dArray) -> {
                                    byte[] array = new byte[dArray.size()];
                                    int count = 0;
                                    for (Double d : dArray) {
                                        array[count] = d.byteValue();
                                        count++;
                                    }
                                    return (PVAData) new PVAByteArray(VALUE_STRING,
                                            false, array);
                                }).toList()),
                entry(ArchDBRTypes.DBR_WAVEFORM_INT,
                        fakeData.stream()
                                .map((dArray) -> (PVAData) new PVAIntArray(VALUE_STRING,
                                        false,
                                        dArray.stream().mapToInt(
                                                        Double::intValue)
                                                .toArray()))
                                .toList()),
                entry(ArchDBRTypes.DBR_WAVEFORM_DOUBLE,
                        fakeData.stream().map((dArray) -> (PVAData) new PVADoubleArray(
                                VALUE_STRING,
                                dArray.stream().mapToDouble(Double::doubleValue)
                                        .toArray()))
                                .toList()));
    }

    /**
     * Test for a single pv of particular type.
     * <p>
     * Outline:
     *  1. Setup PV in PVAServer
     *  2. Setup Archiver Storage
     *  3. Send PV to be archived
     *  4. Wait for PV to be setup
     *  5. Send data from inputData
     *  6. Convert Received Data into a hashmap
     *  7. Test received and sent data are the same
     * 
     * @param configService Archiver to send pv to 
     * @param server a pvAccess server
     * @param type Type of pv
     * @param inputData Input data to archive
     * @throws InterruptedException throw if interrupted
     */
    public void singlePVTest(ConfigService configService, PVAServer server, ArchDBRTypes type,
            List<PVAData> inputData) throws Exception {
        String pvName = "PV:" + inputData.get(0).getClass().getSimpleName() + ":"
                + UUID.randomUUID();

        logger.info("Starting pvAccess test for type " + type + " with pv " + pvName);
        PVAData value = inputData.get(0).cloneData();
        HashMap<Instant, String> expectedData = new HashMap<>();
        Instant instant = Instant.now();
        expectedData.put(instant, formatInput(value));
        PVATimeStamp timeStamp = new PVATimeStamp(instant);
        String struct_name = type.isWaveForm() ? "epics:nt/NTScalarArray:1.0" : "epics:nt/NTScalar:1.0";
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAInt("status", 0), new PVAInt("severity", 0));
        PVAStructure data = new PVAStructure("demo", struct_name, value,
                timeStamp, alarm);

        ServerPV serverPV = server.createPV(pvName, data);

        MemBufWriter writer = new MemBufWriter(pvName, type);
        startArchivingPV(pvName, writer, configService, type);
        long samplingPeriodMilliSeconds = 100;

        for (PVAData input : inputData.subList(1, inputData.size())) {
            Thread.sleep(samplingPeriodMilliSeconds);
            PVAData newValue = data.get("value");
            try {
                newValue.setValue(input);
            } catch (Exception e) {
                fail(e.getMessage());
            }
            instant = Instant.now();
            expectedData.put(instant, formatInput(newValue));
            timeStamp.set(instant);
            try {
                serverPV.update(data);
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }
        Thread.sleep(samplingPeriodMilliSeconds);

        Map<Instant, String> actualValues = getReceivedValues(writer, configService).entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey, (e) -> e.getValue().toString()));

        assertEquals(expectedData, actualValues);
    }

    /**
     * Test checks each type of pv from data() method
     * <p>
     * Sets up an archiver with ConfigServiceForTests
     * Sets up PVAServer for sending data
     * <p>
     * Run tests
     * <p>
     * Shuts down server and archiver
     * 
     * @throws Exception Generic exception
     */
    @Test
    public void testSinglePVs() throws Exception {

        // Setup
        ConfigService configService = new ConfigServiceForTests(new File("./bin"));
        PVAServer server = new PVAServer();
        var dataSet = data();
        for (var data : dataSet.entrySet()) {
            singlePVTest(configService, server, data.getKey(), data.getValue());
        }
        // Teardown
        configService.shutdownNow();
        server.close();
    }

}
