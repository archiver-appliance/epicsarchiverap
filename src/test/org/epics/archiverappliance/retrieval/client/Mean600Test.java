package org.epics.archiverappliance.retrieval.client;

import static edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin.PB_PLUGIN_IDENTIFIER;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Testing mean_600 and making sure we get expected results in the client.
 * Generate a few years worth of data for a PV and then get the results using mean_600 and raw results.
 *
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class Mean600Test {
    private static Logger logger = LogManager.getLogger(Mean600Test.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();
    private String pvName = "UnitTestNoNamingConvention:inactive1";
    private short currentYear = TimeUtils.getCurrentYear();
    private File ltsFolder = new File(System.getenv("ARCHAPPL_LONG_TERM_FOLDER") + "/UnitTestNoNamingConvention");
    StoragePlugin storageplugin;
    private ConfigServiceForTests configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        storageplugin = StoragePluginURLParser.parseStoragePlugin(
                PB_PLUGIN_IDENTIFIER
                        + "://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR",
                configService);
        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());

        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
        try (BasicContext context = new BasicContext()) {
            for (short y = 3; y >= 0; y--) {
                short year = (short) (currentYear - y);
                for (int day = 0; day < 366; day++) {
                    ArrayListEventStream testData = new ArrayListEventStream(
                            24 * 60 * 60,
                            new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));
                    int startofdayinseconds = day * 24 * 60 * 60;
                    for (int secondintoday = 0; secondintoday < 24 * 60 * 60; secondintoday++) {
                        // The value should be the secondsIntoYear integer divided by 600.
                        testData.add(new SimulationEvent(
                                startofdayinseconds + secondintoday,
                                year,
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                new ScalarValue<Double>(
                                        (double) (((int) (startofdayinseconds + secondintoday) / 600)))));
                    }
                    storageplugin.appendData(context, pvName, testData);
                }
            }
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();

        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
    }

    @Test
    public void testSimpleArchivePV() throws Exception {
        String pvNameToArchive = pvName;
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
                mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);

        // We have now archived this PV, get some data and validate we got the expected number of events
        checkRetrieval(pvName, 366 * 86400);
        checkRetrieval("mean_600(" + pvName + ")", 366 * 24 * 6);
    }

    private void checkRetrieval(String retrievalPVName, int expectedAtLeastEvents) throws IOException {
        long startTimeMillis = System.currentTimeMillis();
        RawDataRetrieval rawDataRetrieval = new RawDataRetrieval(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant now = TimeUtils.now();
        Instant start = TimeUtils.minusDays(now, 366);
        Instant end = now;
        int eventCount = 0;

        final HashMap<String, String> metaFields = new HashMap<String, String>();
        // Make sure we get the EGU as part of a regular VAL call.
        try (GenMsgIterator strm = rawDataRetrieval.getDataForPVs(
                Arrays.asList(retrievalPVName),
                TimeUtils.toSQLTimeStamp(start),
                TimeUtils.toSQLTimeStamp(end),
                false,
                null)) {
            PayloadInfo info = null;
            Assertions.assertTrue(strm != null, "We should get some data, we are getting a null stream back");
            info = strm.getPayLoadInfo();
            Assertions.assertTrue(info != null, "Stream has no payload info");
            mergeHeaders(info, metaFields);
            strm.onInfoChange(new InfoChangeHandler() {
                @Override
                public void handleInfoChange(PayloadInfo info) {
                    mergeHeaders(info, metaFields);
                }
            });

            long endTimeMillis = System.currentTimeMillis();

            for (@SuppressWarnings("unused") EpicsMessage dbrevent : strm) {
                eventCount++;
            }

            logger.info("Retrival for " + retrievalPVName + "=" + (endTimeMillis - startTimeMillis) + "(ms)");
        }

        Assertions.assertTrue(
                eventCount >= expectedAtLeastEvents,
                "Expecting " + expectedAtLeastEvents + "events. We got " + eventCount);
    }

    private static void mergeHeaders(PayloadInfo info, HashMap<String, String> headers) {
        int headerCount = info.getHeadersCount();
        for (int i = 0; i < headerCount; i++) {
            String headerName = info.getHeaders(i).getName();
            String headerValue = info.getHeaders(i).getVal();
            logger.info("Adding header " + headerName + " = " + headerValue);
            headers.put(headerName, headerValue);
        }
    }
}
