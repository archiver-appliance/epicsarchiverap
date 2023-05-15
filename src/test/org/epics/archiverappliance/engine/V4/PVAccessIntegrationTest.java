package org.epics.archiverappliance.engine.V4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.convertBytesToPVAStructure;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.waitForStatusChange;
import static org.junit.Assert.assertEquals;

/**
 * A basic integration test of using pvAccess to archive a pv
 */
@Category(IntegrationTests.class)
public class PVAccessIntegrationTest {


    private static final Logger logger = LogManager.getLogger(PVAccessIntegrationTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    private PVAServer pvaServer;

    @Before
    public void setUp() throws Exception {

        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
        pvaServer = new PVAServer();
    }

    @After
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        pvaServer.close();
    }

    @Test
    public void testPVAccessGenericJsonApi() throws Exception {

        String pvName = "PV:" + PVAccessIntegrationTest.class.getSimpleName() + ":" + UUID.randomUUID();

        logger.info("Starting pvAccess test for pv " + pvName);

        Instant firstInstant = Instant.now();

        PVATimeStamp timeStamp = new PVATimeStamp(firstInstant);
        String struct_name = "epics:nt/NTScalar:1.0";
        var level1 = new PVAString("level 1", "level 1 0");
        var level2 = new PVAInt("level 2", 16);
        var value = new PVAStructure("structure", "structure_name", level1, level2);
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAInt("status", 0), new PVAInt("severity", 0));

        PVAStructure pvaStructure = new PVAStructure("struct name", struct_name, value, timeStamp, alarm);

        Map<Instant, PVAStructure> expectedValues = new HashMap<>();
        expectedValues.put(firstInstant, pvaStructure.cloneData());

        ServerPV serverPV = pvaServer.createPV(pvName, pvaStructure);

        String pvURLName = URLEncoder.encode(pvName, StandardCharsets.UTF_8);

        // Archive PV
        String mgmtUrl = "http://localhost:17665/mgmt/bpl/";
        String archivePVURL = mgmtUrl + "archivePV?pv=pva://";

        GetUrlContent.getURLContentAsJSONArray(archivePVURL + pvURLName);
        waitForStatusChange(pvName, "Being archived", 60, mgmtUrl, logger, 10);

        Timestamp start = TimeUtils.convertFromInstant(firstInstant);

        long samplingPeriodMilliSeconds = 100;

        Thread.sleep(samplingPeriodMilliSeconds);
        level1.set("level 1 1");
        Instant instant = Instant.now();
        timeStamp.set(instant);
        serverPV.update(pvaStructure);

        expectedValues.put(instant, pvaStructure);

        Thread.sleep(samplingPeriodMilliSeconds);
        double secondsToBuffer = 5.0;
        // Need to wait for the writer to write all the received data.
        Thread.sleep((long) secondsToBuffer * 1000);
        Timestamp end = TimeUtils.convertFromInstant(Instant.now());

        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");

        EventStream stream = null;
        Map<Instant, SampleValue> actualValues = new HashMap<>();
        try {
            stream = rawDataRetrieval.getDataForPVS(new String[]{pvName}, start, end, desc -> logger.info("Getting data for PV " + desc.getPvName()));

            // Make sure we get the DBR type we expect
            assertEquals(stream.getDescription().getArchDBRType(), ArchDBRTypes.DBR_V4_GENERIC_BYTES);

            // We are making sure that the stream we get back has times in sequential order...
            for (Event e : stream) {
                actualValues.put(e.getEventTimeStamp().toInstant(), e.getSampleValue());
            }
        } finally {
            if (stream != null) try {
                stream.close();
            } catch (Throwable ignored) {
            }
        }

        assertEquals(expectedValues, convertBytesToPVAStructure(actualValues));
    }
}
