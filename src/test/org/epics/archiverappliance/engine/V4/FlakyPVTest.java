package org.epics.archiverappliance.engine.V4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.test.MemBufWriter;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVATimeStamp;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.ServerPV;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.formatInput;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.getReceivedValues;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.startArchivingPV;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.waitForIsConnected;

/**
 * Checks reconnects after connection drops
 */
public class FlakyPVTest {

    private static final Logger logger = LogManager.getLogger(FlakyPVTest.class.getName());

    private ConfigService configService;
    private PVAServer pvaServer;
    private static final long disconnectTime = ConfigServiceForTests.defaultSecondsDisconnect * 1000;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(new File("./bin"));
        pvaServer = new PVAServer();
    }

    @AfterEach
    public void tearDown() {
        configService.shutdownNow();
        pvaServer.close();
    }

    /**
     * Test that disconnecting a pv doesn't cause any issues.
     *
     * @throws Exception From dealing with pvaStructures
     */
    @Test
    public void testDisconnect() throws Exception {

        String pvName = "PV:" + FlakyPVTest.class.getSimpleName() + ":"
                + UUID.randomUUID();

        logger.info("Starting pvAccess test for pv " + pvName);

        HashMap<Instant, String> expectedData = new HashMap<>();
        Instant instant = Instant.now();
        var value = new PVAString("value", "value0");

        expectedData.put(instant, formatInput(value));
        PVATimeStamp timeStamp = new PVATimeStamp(instant);
        String struct_name = "epics:nt/NTScalar:1.0";
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAInt("status", 0), new PVAInt("severity", 0));
        PVAStructure data = new PVAStructure("demo", struct_name, value,
                timeStamp, alarm);

        ServerPV serverPV = pvaServer.createPV(pvName, data);

        var type = ArchDBRTypes.DBR_SCALAR_STRING;
        MemBufWriter writer = new MemBufWriter(pvName, type);
        ArchiveChannel archiveChannel = startArchivingPV(pvName, writer, configService, type);

        long samplingPeriodMilliSeconds = 100;

        Thread.sleep(samplingPeriodMilliSeconds);
        Instant instantFirstChange = Instant.now();
        timeStamp.set(instantFirstChange);
        value.set("value1");
        expectedData.put(instantFirstChange, formatInput(value));
        try {
            serverPV.update(data);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        // Disconnect the pv
        logger.info("Close pv " + pvName);
        serverPV.close();
        pvaServer.close();
        logger.info("Close pv " + pvName);
        Thread.sleep(disconnectTime);

        // Restart the pv
        logger.info("Restart pv " + pvName);
        pvaServer = new PVAServer();
        serverPV = pvaServer.createPV(pvName, data);

        waitForIsConnected(archiveChannel);
        Instant instantSecondChange = Instant.now();
        timeStamp.set(instantSecondChange);
        value.set("value2");
        expectedData.put(instantSecondChange, formatInput(value));
        try {
            serverPV.update(data);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        Thread.sleep(5000);

        Map<Instant, String> actualValues = getReceivedValues(writer, configService).entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey, (e) -> e.getValue().toString()));

        Assertions.assertEquals(expectedData, actualValues);
    }
    /**
     * Test that disconnecting a pv doesn't cause any issues.
     *
     * @throws Exception From dealing with pvaStructures
     */
    @Test
    public void testDestroyServer() throws Exception {

        String pvName = "PV:" + FlakyPVTest.class.getSimpleName() + ":"
                + UUID.randomUUID();

        logger.info("Starting pvAccess test for pv " + pvName);

        HashMap<Instant, String> expectedData = new HashMap<>();
        Instant instant = Instant.now();
        var value = new PVAString("value", "value0");

        expectedData.put(instant, formatInput(value));
        PVATimeStamp timeStamp = new PVATimeStamp(instant);
        String struct_name = "epics:nt/NTScalar:1.0";
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAInt("status", 0), new PVAInt("severity", 0));
        PVAStructure data = new PVAStructure("demo", struct_name, value,
                timeStamp, alarm);

        ServerPV serverPV = pvaServer.createPV(pvName, data);

        var type = ArchDBRTypes.DBR_SCALAR_STRING;
        MemBufWriter writer = new MemBufWriter(pvName, type);
        ArchiveChannel archiveChannel = startArchivingPV(pvName, writer, configService, type);

        long samplingPeriodMilliSeconds = 100;

        Thread.sleep(samplingPeriodMilliSeconds);
        Instant instantFirstChange = Instant.now();
        timeStamp.set(instantFirstChange);
        value.set("value1");
        expectedData.put(instantFirstChange, formatInput(value));
        try {
            serverPV.update(data);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        // close pv the pv
        pvaServer.close();
        logger.info("Server close pv " + pvName);
        Thread.sleep(disconnectTime);

        // Restart the pv
        pvaServer = new PVAServer();
        serverPV = pvaServer.createPV(pvName, data);
        logger.info("Restart pv " + pvName);

        waitForIsConnected(archiveChannel);
        Instant instantSecondChange = Instant.now();
        timeStamp.set(instantSecondChange);
        value.set("value2");
        expectedData.put(instantSecondChange, formatInput(value));
        try {
            serverPV.update(data);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        Thread.sleep(5000);

        Map<Instant, String> actualValues = getReceivedValues(writer, configService).entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey, (e) -> e.getValue().toString()));

        Assertions.assertEquals(expectedData, actualValues);
    }
    /**
     * Test that disconnecting a pv doesn't cause any issues.
     *
     * @throws Exception From dealing with pvaStructures
     */
    @Test
    public void testDestroyServerPV() throws Exception {

        String pvName = "PV:" + FlakyPVTest.class.getSimpleName() + ":"
                + UUID.randomUUID();

        logger.info("Starting pvAccess test for pv " + pvName);

        HashMap<Instant, String> expectedData = new HashMap<>();
        Instant instant = Instant.now();
        var value = new PVAString("value", "value0");

        expectedData.put(instant, formatInput(value));
        PVATimeStamp timeStamp = new PVATimeStamp(instant);
        String struct_name = "epics:nt/NTScalar:1.0";
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAInt("status", 0), new PVAInt("severity", 0));
        PVAStructure data = new PVAStructure("demo", struct_name, value,
                timeStamp, alarm);

        ServerPV serverPV = pvaServer.createPV(pvName, data);

        var type = ArchDBRTypes.DBR_SCALAR_STRING;
        MemBufWriter writer = new MemBufWriter(pvName, type);
        ArchiveChannel archiveChannel = startArchivingPV(pvName, writer, configService, type);

        long samplingPeriodMilliSeconds = 100;

        Thread.sleep(samplingPeriodMilliSeconds);
        Instant instantFirstChange = Instant.now();
        timeStamp.set(instantFirstChange);
        value.set("value1");
        expectedData.put(instantFirstChange, formatInput(value));
        try {
            serverPV.update(data);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        // close pv the pv
        serverPV.close();
        logger.info("ServerPV close pv " + pvName);
        Thread.sleep(disconnectTime);

        // Restart the pv
        pvaServer = new PVAServer();
        serverPV = pvaServer.createPV(pvName, data);
        logger.info("Restart pv " + pvName);

        waitForIsConnected(archiveChannel);
        Instant instantSecondChange = Instant.now();
        timeStamp.set(instantSecondChange);
        value.set("value2");
        expectedData.put(instantSecondChange, formatInput(value));
        try {
            serverPV.update(data);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        Thread.sleep(5000);

        Map<Instant, String> actualValues = getReceivedValues(writer, configService).entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey, (e) -> e.getValue().toString()));

        Assertions.assertEquals(expectedData, actualValues);
    }

    /**
     * Test that starting a pv after sending to archiver causes no problems.
     *
     * @throws Exception From dealing with pvaStructures
     */
    @Test
    public void testPVStartAfterArchive() throws Exception {

        String pvName = "PV:" + FlakyPVTest.class.getSimpleName() + ":"
                + UUID.randomUUID();

        logger.info("Starting pvAccess test for pv " + pvName);

        HashMap<Instant, String> expectedData = new HashMap<>();
        Instant instant = Instant.now();
        var value = new PVAString("value", "value0");

        PVATimeStamp timeStamp = new PVATimeStamp(instant);
        String struct_name = "epics:nt/NTScalar:1.0";
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAInt("status", 0), new PVAInt("severity", 0));
        PVAStructure data = new PVAStructure("demo", struct_name, value,
                timeStamp, alarm);


        var type = ArchDBRTypes.DBR_SCALAR_STRING;
        MemBufWriter writer = new MemBufWriter(pvName, type);
        ArchiveChannel archiveChannel = startArchivingPV(pvName, writer, configService, type, false, new String[0]);
        Thread.sleep(1000);

        try {
            var meta = configService.getEngineContext().getChannelList().get(pvName).getCurrentCopyOfMetaFields();
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail(e.getMessage());
        }

        long samplingPeriodMilliSeconds = 100;
        Instant instantFirstChange = Instant.now();
        timeStamp.set(instantFirstChange);
        value.set("value1");
        expectedData.put(instantFirstChange, formatInput(value));
        ServerPV serverPV = pvaServer.createPV(pvName, data);
        waitForIsConnected(archiveChannel);
        try {
            serverPV.update(data);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        Thread.sleep(samplingPeriodMilliSeconds);


        // Disconnect the pv
        serverPV.close();
        pvaServer.close();
        logger.info("Close pv " + pvName);
        Thread.sleep(disconnectTime);

        // Restart the pv
        pvaServer = new PVAServer();
        serverPV = pvaServer.createPV(pvName, data);
        logger.info("Restart pv " + pvName);

        waitForIsConnected(archiveChannel);
        Instant instantSecondChange = Instant.now();
        timeStamp.set(instantSecondChange);
        value.set("value2");
        expectedData.put(instantSecondChange, formatInput(value));
        try {
            serverPV.update(data);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        Thread.sleep(5000);


        Map<Instant, String> actualValues = getReceivedValues(writer, configService).entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey, (e) -> e.getValue().toString()));

        Assertions.assertEquals(expectedData, actualValues);
    }

}
