package org.epics.archiverappliance.mgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigServiceForTests;
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

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.waitForStatusChange;

@Tag("integration")
public class DbdArchiveTest {
    private static final Logger logger = LogManager.getLogger(DbdArchiveTest.class.getName());
    private static final TomcatSetup tomcatSetup = new TomcatSetup();
    private SIOCSetup ioc = null;

    @BeforeEach
    public void setUp() throws Exception {
        ioc = new SIOCSetup();
        ioc.startSIOCWithDefaultDB();

        tomcatSetup.setUpWebApps(DbdArchiveTest.class.getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {

        tomcatSetup.tearDown();
        ioc.stopSIOC();
    }

    /**
     * test one pv with meta field. We must make sure the meta fields should be
     * archived too
     */
    @Test
    public void testArchiveFilterPV() {

        try {
            String pvName = "ArchUnitTest:manual";
            String filter = ".{'dbnd':{'abs':0.1}}";
            String fullPVName = pvName + filter;

            String pvURLName = URLEncoder.encode(fullPVName, StandardCharsets.UTF_8);

            Instant firstInstant = Instant.now();

            SIOCSetup.caput(pvName, 0);
            // Archive PV
            String mgmtUrl = "http://localhost:17665/mgmt/bpl/";
            String archivePVURL = mgmtUrl + "archivePV?pv=";

            GetUrlContent.getURLContentAsJSONArray(archivePVURL + pvURLName);
            waitForStatusChange(fullPVName, "Being archived", 60, mgmtUrl, 10);

            SIOCSetup.caput(pvName, 0);
            SIOCSetup.caput(pvName, 0.1);
            SIOCSetup.caput(pvName, 0.15);
            SIOCSetup.caput(pvName, 1.0);
            SIOCSetup.caput(pvName, 1.05);
            SIOCSetup.caput(pvName, 1.06);

            Instant end = Instant.now();
            RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                    "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
            EventStream stream = rawDataRetrieval.getDataForPVS(
                    new String[] {pvURLName},
                    firstInstant,
                    end,
                    desc -> logger.info("Getting data for PV " + desc.getPvName()));

            int totalEvents = 0;
            for (Event e : stream) {
                logger.info("event " + e.getSampleValue().toString());
                totalEvents++;
            }
            Assertions.assertEquals(3, totalEvents, "We should have some events in the current samples " + totalEvents);

        } catch (Exception e) {
            //
            Assertions.fail(e.getMessage());
            logger.error("Exception", e);
        }
    }
}
