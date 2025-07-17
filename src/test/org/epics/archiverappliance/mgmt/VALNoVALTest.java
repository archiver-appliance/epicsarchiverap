package org.epics.archiverappliance.mgmt;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * A common use case is where we archive the .VAL and ask for data either way.
 * This test archives two PVs; one with a .VAL and one without.
 * We then ask for data in all combinations and make sure we get said data.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class VALNoVALTest {
    private static Logger logger = LogManager.getLogger(VALNoVALTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();
    String folderSTS = ConfigServiceForTests.getDefaultShortTermFolder() + File.separator + "reshardSTS";
    String folderMTS = ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "reshardMTS";
    String folderLTS = ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "reshardLTS";

    @BeforeEach
    public void setUp() throws Exception {
        System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER", folderSTS);
        System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", folderMTS);
        System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER", folderLTS);

        FileUtils.deleteDirectory(new File(folderSTS));
        FileUtils.deleteDirectory(new File(folderMTS));
        FileUtils.deleteDirectory(new File(folderLTS));

        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();

        FileUtils.deleteDirectory(new File(folderSTS));
        FileUtils.deleteDirectory(new File(folderMTS));
        FileUtils.deleteDirectory(new File(folderLTS));
    }

    @Test
    public void testVALNoVALTest() throws Exception {
        String pvNameToArchive1 = "UnitTestNoNamingConvention:sine";
        String pvNameToArchive2 = "UnitTestNoNamingConvention:cosine.VAL";

        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
                mgmtURL + "/archivePV",
                GetUrlContent.from(List.of(
                        new JSONObject(Map.of("pv", pvNameToArchive1)),
                        new JSONObject(Map.of("pv", pvNameToArchive2)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive1, "Being archived", 10, mgmtURL, 15);
        PVAccessUtil.waitForStatusChange(pvNameToArchive2, "Being archived", 10, mgmtURL, 15);

        testRetrievalCountOnServer(pvNameToArchive1, 2);
        testRetrievalCountOnServer(pvNameToArchive1 + ".VAL", 2);
        testRetrievalCountOnServer(pvNameToArchive1, 2);
        testRetrievalCountOnServer(pvNameToArchive1 + ".VAL", 2);
    }

    /**
     * Get data for the PV from the server and make sure we have some data
     * @param pvName
     * @param expectedEventCount
     * @throws IOException
     */
    private void testRetrievalCountOnServer(String pvName, int expectedEventCount) throws IOException {
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant end = TimeUtils.plusDays(TimeUtils.now(), 3);
        Instant start = TimeUtils.minusDays(end, 6);
        try (EventStream stream = rawDataRetrieval.getDataForPVS(new String[] {pvName}, start, end, null)) {
            long previousEpochSeconds = 0;
            int eventCount = 0;

            // We are making sure that the stream we get back has times in sequential order...
            if (stream != null) {
                for (Event e : stream) {
                    long actualSeconds = e.getEpochSeconds();
                    Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
                    previousEpochSeconds = actualSeconds;
                    eventCount++;
                }
            }

            logger.info("Got " + eventCount + " event for pv " + pvName);
            Assertions.assertTrue(
                    eventCount > expectedEventCount,
                    "When asking for data using " + pvName + ", event count is incorrect We got " + eventCount
                            + " and we were expecting at least " + expectedEventCount);
        }
    }
}
