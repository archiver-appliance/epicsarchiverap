package org.epics.archiverappliance.engine.V4;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.countRetrievedSamples;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.waitForStatusChange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
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
import java.util.concurrent.TimeUnit;

@Tag("integration")
@Tag("localEpics")
public class PVAEpicsIntegrationTest {

    private static final Logger logger = LogManager.getLogger(PVAEpicsIntegrationTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();

    @BeforeEach
    public void setUp() throws Exception {

        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
        siocSetup.startSIOCWithDefaultDB();
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    @Test
    public void testEpicsIocPVA() throws Exception {

        String pvName = "UnitTestNoNamingConvention:sine";

        logger.info("Starting pvAccess test for pv " + pvName);

        Instant firstInstant = Instant.now();

        String pvURLName = URLEncoder.encode(pvName, StandardCharsets.UTF_8);

        // Archive PV
        String mgmtUrl = "http://localhost:17665/mgmt/bpl/";
        String archivePVURL = mgmtUrl + "archivePV?pv=pva://";

        GetUrlContent.getURLContentAsJSONArray(archivePVURL + pvURLName);
        waitForStatusChange(pvName, "Being archived", 60, mgmtUrl, 10);

        Instant start = firstInstant;

        long samplingPeriodMilliSeconds = 100;

        Thread.sleep(samplingPeriodMilliSeconds);
        double secondsToBuffer = 5.0;

        logger.info("Stop the ioc");
        siocSetup.stopSIOC();
        Thread.sleep(61 * 1000);
        logger.info("Restart the ioc");
        siocSetup.startSIOCWithDefaultDB();
        Thread.sleep(samplingPeriodMilliSeconds);

        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");

        // Archiving resumes a few seconds after the IOC restarts; poll until enough of the
        // 1 Hz samples have been archived.
        Awaitility.await()
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .until(() -> countRetrievedSamples(rawDataRetrieval, pvName, start) > secondsToBuffer);

        // Verify the retrieved stream once, after the wait: right DBR type and enough samples.
        Map<Instant, SampleValue> actualValues = new HashMap<>();
        try (EventStream stream = rawDataRetrieval.getDataForPVS(
                new String[] {pvName},
                start,
                Instant.now(),
                desc -> logger.info("Getting data for PV " + desc.getPvName()))) {
            Assertions.assertNotNull(stream, "Got a null stream back from retrieval");
            // Make sure we get the DBR type we expect
            Assertions.assertEquals(
                    ArchDBRTypes.DBR_SCALAR_DOUBLE, stream.getDescription().getArchDBRType());
            for (Event e : stream) {
                actualValues.put(e.getEventTimeStamp(), e.getSampleValue());
            }
        }
        logger.info("Data was {}", actualValues);
        Assertions.assertTrue(actualValues.size() > secondsToBuffer);
    }
}
