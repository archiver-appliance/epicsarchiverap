package org.epics.archiverappliance.retrieval.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.epics.archiverappliance.config.PVNames.V3_PREFIX;
import static org.epics.archiverappliance.config.PVNames.V4_PREFIX;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.waitForStatusChange;

@Tag("integration")
@Tag("localEpics")
public class StripProtocolRetrievalTest {

    private static final Logger logger = LogManager.getLogger(StripProtocolRetrievalTest.class.getName());
    static TomcatSetup tomcatSetup = new TomcatSetup();
    static SIOCSetup siocSetup = new SIOCSetup();

    @BeforeAll
    public static void setUp() throws Exception {

        tomcatSetup.setUpWebApps(StripProtocolRetrievalTest.class.getSimpleName());
        siocSetup.startSIOCWithDefaultDB();
    }

    @AfterAll
    public static void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    @ParameterizedTest
    @ValueSource(strings = {V4_PREFIX, V3_PREFIX})
    public void testProtocol(String protocol) throws Exception {

        String pvName = "UnitTestNoNamingConvention:sine";

        logger.info("Starting pvAccess test for pv " + pvName);

        Instant firstInstant = Instant.now();

        String pvURLName = URLEncoder.encode(pvName, StandardCharsets.UTF_8);

        // Archive PV
        String mgmtUrl = "http://localhost:17665/mgmt/bpl/";
        String archivePVURL = mgmtUrl + "archivePV?pv=" + protocol;

        GetUrlContent.getURLContentAsJSONArray(archivePVURL + pvURLName);
        waitForStatusChange(pvName, "Being archived", 60, mgmtUrl, 10);

        long samplingPeriodMilliSeconds = 100;

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
            stream = rawDataRetrieval.getDataForPVS(
                    new String[] {protocol + pvName},
                    firstInstant,
                    end,
                    desc -> logger.info("Getting data for PV " + desc.getPvName()));

            // Make sure we get the DBR type we expect
            Assertions.assertEquals(
                    ArchDBRTypes.DBR_SCALAR_DOUBLE, stream.getDescription().getArchDBRType());

            // We are making sure that the stream we get back has times in sequential order...
            for (Event e : stream) {
                actualValues.put(e.getEventTimeStamp(), e.getSampleValue());
            }
        } finally {
            if (stream != null)
                try {
                    stream.close();
                } catch (Throwable ignored) {
                }
        }
        logger.info("Data was {}", actualValues);
        Assertions.assertTrue(actualValues.size() > secondsToBuffer);
    }
}
