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
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVATimeStamp;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.ServerPV;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.convertBytesToPVAStructure;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.waitForStatusChange;

/**
 * Checks reconnects after connection drops as an integration test.
 */
@Tag("integration")
public class PVAFlakyIntegrationTest {

    private static final Logger logger = LogManager.getLogger(PVAFlakyIntegrationTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    private PVAServer pvaServer;

    @BeforeEach
    public void setUp() throws Exception {

        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
        pvaServer = new PVAServer();
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        pvaServer.close();
    }

    @Test
    public void testStartAfterArchive() throws Exception {

        String pvName = "PV:" + org.epics.archiverappliance.engine.V4.PVAccessIntegrationTest.class.getSimpleName() + ":" + UUID.randomUUID();

        logger.info("Starting pvAccess test for pv " + pvName);

        Instant firstInstant = Instant.now();

        PVATimeStamp timeStamp = new PVATimeStamp(firstInstant);
        String struct_name = "epics:nt/NTScalar:1.0";
        var level1 = new PVAString("level 1", "level 1 0");
        var level2 = new PVAInt("level 2", 16);
        var value = new PVAStructure("structure", "structure_name", level1, level2);
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAInt("status", 0), new PVAInt("severity", 0));

        PVAStructure pvaStructure = new PVAStructure("struct name", struct_name, value, timeStamp, alarm);

        String pvURLName = URLEncoder.encode(pvName, StandardCharsets.UTF_8);

        // Archive PV
        String mgmtUrl = "http://localhost:17665/mgmt/bpl/";
        String archivePVURL = mgmtUrl + "archivePV?pv=pva://";

        GetUrlContent.getURLContentAsJSONArray(archivePVURL + pvURLName);

        Instant start = firstInstant;

        long samplingPeriodMilliSeconds = 100;

        Map<Instant, PVAStructure> expectedValues = new HashMap<>();
        expectedValues.put(firstInstant, pvaStructure.cloneData());

        ServerPV serverPV = pvaServer.createPV(pvName, pvaStructure);
        waitForStatusChange(pvName, "Being archived", 60, mgmtUrl, 10);

        Thread.sleep(samplingPeriodMilliSeconds);
        level1.set("level 1 1");
        Instant instantFirstChange = Instant.now();
        timeStamp.set(instantFirstChange);
        serverPV.update(pvaStructure);

        expectedValues.put(instantFirstChange, pvaStructure.cloneData());

        // Disconnect the pv
        serverPV.close();
        pvaServer.close();
        logger.info("Close pv " + pvName);
        Thread.sleep(2 * 60 * 1000);

        // Restart the pv
        pvaServer = new PVAServer();
        serverPV = pvaServer.createPV(pvName, pvaStructure);
        logger.info("Restart pv " + pvName);

        Thread.sleep(samplingPeriodMilliSeconds);
        level1.set("level 1 2");
        Instant instantSecondChange = Instant.now();
        timeStamp.set(instantSecondChange);
        serverPV.update(pvaStructure);

        expectedValues.put(instantSecondChange, pvaStructure.cloneData());

        Thread.sleep(samplingPeriodMilliSeconds);
        double secondsToBuffer = 5.0;
        // Need to wait for the writer to write all the received data.
        Thread.sleep((long) secondsToBuffer * 1000);
        Instant end = Instant.now();

        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");

        EventStream stream = null;
        Map<Instant, SampleValue> actualValues = new HashMap<>();
        try {
            stream = rawDataRetrieval.getDataForPVS(new String[]{pvName}, start, end, desc -> logger.info("Getting data for PV " + desc.getPvName()));

            // Make sure we get the DBR type we expect
            Assertions.assertEquals(ArchDBRTypes.DBR_V4_GENERIC_BYTES, stream.getDescription().getArchDBRType());

            // We are making sure that the stream we get back has times in sequential order...
            for (Event e : stream) {
                actualValues.put(e.getEventTimeStamp(), e.getSampleValue());
            }
        } finally {
            if (stream != null) try {
                stream.close();
            } catch (Throwable ignored) {
            }
        }

        Assertions.assertEquals(expectedValues, convertBytesToPVAStructure(actualValues));
    }


}
