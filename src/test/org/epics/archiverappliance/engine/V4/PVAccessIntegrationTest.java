package org.epics.archiverappliance.engine.V4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.pva.data.PVAByte;
import org.epics.pva.data.PVAByteArray;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAIntArray;
import org.epics.pva.data.PVAShort;
import org.epics.pva.data.PVAShortArray;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.PVAStructureArray;
import org.epics.pva.data.PVATypeRegistry;
import org.epics.pva.data.nt.PVAAlarm;
import org.epics.pva.data.nt.PVAEnum;
import org.epics.pva.data.nt.PVATimeStamp;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.ServerPV;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.fromGenericSampleValueToPVAData;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.waitForStatusChange;

/**
 * A basic integration test of using pvAccess to archive a pv
 */
@Tag("integration")
public class PVAccessIntegrationTest {

    private static final Logger logger = LogManager.getLogger(PVAccessIntegrationTest.class.getName());
    private static final TomcatSetup tomcatSetup = new TomcatSetup();
    private static final PVAServer pvaServer;

    static {
        try {
            pvaServer = new PVAServer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    public static void setUp() throws Exception {

        tomcatSetup.setUpWebApps(PVAccessIntegrationTest.class.getSimpleName());
    }

    @AfterAll
    public static void tearDown() throws Exception {
        tomcatSetup.tearDown();
        pvaServer.close();
    }

    @Test
    public void testPVAccessGenericJsonApi() throws Exception {

        String pvName = "PV:" + PVAccessIntegrationTest.class.getSimpleName() + ":testPVAccessGenericJsonApi:" + UUID.randomUUID();

        var level1 = new PVAString("level 1", "level 1 0");
        var level2 = new PVAInt("level 2", 16);
        var value = new PVAStructure("value", "structure_name", level1, level2);

        var level11 = new PVAString("level 11", "level 1 1");
        var value2 = new PVAStructure("value", "structure_name", level11, level2);
        PVATypeRegistry types = new PVATypeRegistry();

        testPVData(ArchDBRTypes.DBR_V4_GENERIC_BYTES,
                List.of(value, value2), (sampleValue) -> {
                    try {
                        PVAStructure fullValue = (PVAStructure) fromGenericSampleValueToPVAData(sampleValue, types);
                        return fullValue.get("value");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, "epics:nt/NTScalar:1.0", pvName);

    }

    @Test
    public void testPVAccessEnumJsonApi() throws Exception {
        String pvName = "PV:" + PVAccessIntegrationTest.class.getSimpleName() + ":testPVAccessEnumJsonApi:" + UUID.randomUUID();

        String[] choices = new String[]{"one", "two", "three"};

        var value = new PVAEnum("value", 0, choices);
        var value2 = new PVAEnum("value", 1, choices);

        testPVData(ArchDBRTypes.DBR_SCALAR_ENUM,
                List.of(value, value2), (sampleValue) -> {
                    return new PVAEnum("value", sampleValue.getValue().intValue(), choices);
                }, "epics:nt/NTScalar:1.0", pvName);

    }

    @Test
    public void testPVAccessEnumWaveformJsonApi() throws Exception {
        String pvName = "PV:" + PVAccessIntegrationTest.class.getSimpleName() + ":testPVAccessEnumWaveformJsonApi:" + UUID.randomUUID();

        String[] choices = new String[]{"one", "two", "three"};
        var enumStructure = new PVAEnum("enum_t[]", 0, choices);
        var enum1 = new PVAEnum("enum1", 0, choices);
        var enum2 = new PVAEnum("enum2", 0, choices);
        var value = new PVAStructureArray("value", enumStructure, enum1, enum2);

        var value2 = new PVAStructureArray("value", enumStructure,
                new PVAEnum("enum1", 1, choices),
                new PVAEnum("enum2", 2, choices));

        testPVData(ArchDBRTypes.DBR_WAVEFORM_ENUM, List.of(value, value2), (sampleValue) -> {
            var values = sampleValue.getValues();
            return new PVAStructureArray("value", enumStructure,
                    new PVAEnum("enum1", (Short) values.get(0), choices),
                    new PVAEnum("enum2", (Short) values.get(1), choices));
        }, "epics:nt/NTScalarArray:1.0", pvName);

    }

    @Test
    public void testPVAccessUnsignedByte() throws Exception {
        String pvName = "PV:" + PVAccessIntegrationTest.class.getSimpleName() + ":testPVAccessUnsignedByte:" + UUID.randomUUID();

        var value = new PVAByte("value", true, Integer.valueOf(1).byteValue());
        var value2 = new PVAByte("value", true, Integer.valueOf(255).byteValue());

        testPVData(ArchDBRTypes.DBR_SCALAR_BYTE,
                List.of(value, value2), (sampleValue) -> {
                    return new PVAByte("value", true, sampleValue.getValue().byteValue());
                }, "epics:nt/NTScalar:1.0", pvName);

    }
    @Test
    public void testPVAccessUnsignedBytes() throws Exception {
        String pvName = "PV:" + PVAccessIntegrationTest.class.getSimpleName() + ":testPVAccessUnsignedBytes:" + UUID.randomUUID();

        var value = new PVAByteArray("value", true, Integer.valueOf(1).byteValue(), Integer.valueOf(-1).byteValue());
        var value2 = new PVAByteArray("value", true, Integer.valueOf(255).byteValue(), Integer.valueOf(1).byteValue());

        testPVData(ArchDBRTypes.DBR_WAVEFORM_BYTE,
                List.of(value, value2), (sampleValue) -> {
                    var values = sampleValue.getValues();
                    return new PVAByteArray("value", true, ((Number) values.get(0)).byteValue(), ((Number) values.get(1)).byteValue());
                }, "epics:nt/NTScalarArray:1.0", pvName);

    }
    @Test
    public void testPVAccessUnsignedShort() throws Exception {
        String pvName = "PV:" + PVAccessIntegrationTest.class.getSimpleName() + ":testPVAccessUnsignedShort:" + UUID.randomUUID();

        var value = new PVAShort("value", true, Integer.valueOf(1).shortValue());
        var value2 = new PVAShort("value", true, Integer.valueOf(255).shortValue());

        testPVData(ArchDBRTypes.DBR_SCALAR_SHORT,
                List.of(value, value2), (sampleValue) -> {
                    return new PVAShort("value", true, sampleValue.getValue().shortValue());
                }, "epics:nt/NTScalar:1.0", pvName);

    }
    @Test
    public void testPVAccessUnsignedShorts() throws Exception {
        String pvName = "PV:" + PVAccessIntegrationTest.class.getSimpleName() + ":testPVAccessUnsignedShorts:" + UUID.randomUUID();

        var value = new PVAShortArray("value", true, Integer.valueOf(1).shortValue(), Integer.valueOf(-1).shortValue());
        var value2 = new PVAShortArray("value", true, Integer.valueOf(255).shortValue(), Integer.valueOf(1).shortValue());

        testPVData(ArchDBRTypes.DBR_WAVEFORM_SHORT,
                List.of(value, value2), (sampleValue) -> {
                    var values = sampleValue.getValues();
                    return new PVAShortArray("value", true, ((Number) values.get(0)).shortValue(), ((Number) values.get(1)).shortValue());
                }, "epics:nt/NTScalarArray:1.0", pvName);

    }

    @Test
    public void testPVAccessUnsignedInt() throws Exception {
        String pvName = "PV:" + PVAccessIntegrationTest.class.getSimpleName() + ":testPVAccessUnsignedInt:" + UUID.randomUUID();

        var value = new PVAInt("value", true, 1);
        var value2 = new PVAInt("value", true, 255);

        testPVData(ArchDBRTypes.DBR_SCALAR_INT,
                List.of(value, value2), (sampleValue) -> {
                    return new PVAInt("value", true, sampleValue.getValue().shortValue());
                }, "epics:nt/NTScalar:1.0", pvName);

    }
    @Test
    public void testPVAccessUnsignedInts() throws Exception {
        String pvName = "PV:" + PVAccessIntegrationTest.class.getSimpleName() + ":testPVAccessUnsignedInts:" + UUID.randomUUID();

        var value = new PVAIntArray("value", true, 1, -1);
        var value2 = new PVAIntArray("value", true, 255, 1);

        testPVData(ArchDBRTypes.DBR_WAVEFORM_INT,
                List.of(value, value2), (sampleValue) -> {
                    var values = sampleValue.getValues();
                    return new PVAIntArray("value", true, ((Number) values.get(0)).intValue(), ((Number) values.get(1)).intValue());
                }, "epics:nt/NTScalarArray:1.0", pvName);

    }

    public <PVA extends PVAData> void testPVData(ArchDBRTypes type,
                                                 List<PVA> inputPvaDataList, Function<SampleValue, PVA> expectedDataMapping,
                                                 String structName, String pvName)
            throws Exception {


        logger.info("Starting pvAccess test for pv " + pvName);

        Instant firstInstant = Instant.now();

        PVATimeStamp timeStamp = new PVATimeStamp(firstInstant);
        var value = inputPvaDataList.get(0).cloneData();

        var alarm = new PVAAlarm(PVAAlarm.AlarmSeverity.NO_ALARM, PVAAlarm.AlarmStatus.NO_STATUS, "alarm message");

        PVAStructure pvaStructure = new PVAStructure("struct name", structName, value, timeStamp, alarm);

        Map<Instant, PVA> expectedValues = new HashMap<>();
        expectedValues.put(firstInstant, inputPvaDataList.get(0));

        ServerPV serverPV = pvaServer.createPV(pvName, pvaStructure);

        String pvURLName = URLEncoder.encode(pvName, StandardCharsets.UTF_8);

        // Archive PV
        String mgmtUrl = "http://localhost:17665/mgmt/bpl/";
        String archivePVURL = mgmtUrl + "archivePV?pv=pva://";

        GetUrlContent.getURLContentAsJSONArray(archivePVURL + pvURLName);
        waitForStatusChange(pvName, "Being archived", 60, mgmtUrl, 10);

        Instant start = firstInstant;

        long samplingPeriodMilliSeconds = 100;

        Thread.sleep(samplingPeriodMilliSeconds);
        Instant instant = Instant.now();
        value.setValue(inputPvaDataList.get(1));
        timeStamp.set(instant);
        serverPV.update(pvaStructure);

        expectedValues.put(instant, inputPvaDataList.get(1));

        Thread.sleep(samplingPeriodMilliSeconds);
        double secondsToBuffer = 5.0;
        // Need to wait for the writer to write all the received data.
        Thread.sleep((long) secondsToBuffer * 1000);
        Instant end = Instant.now();

        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");

        EventStream stream = null;
        Map<Instant, PVA> actualValues = new HashMap<>();
        try {
            stream = rawDataRetrieval.getDataForPVS(new String[]{pvName}, start, end, desc -> logger.info("Getting data for PV " + desc.getPvName()));

            // Make sure we get the DBR type we expect
            Assertions.assertEquals(type, stream.getDescription().getArchDBRType());

            // We are making sure that the stream we get back has times in sequential order...
            for (Event e : stream) {
                actualValues.put(e.getEventTimeStamp(), expectedDataMapping.apply(e.getSampleValue()));
            }
        } finally {
            if (stream != null) try {
                stream.close();
            } catch (Throwable ignored) {
            }
        }

        Assertions.assertEquals(expectedValues, actualValues);
    }
}
