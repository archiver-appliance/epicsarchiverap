package org.epics.archiverappliance.retrieval.postprocessor;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.plain.FileBackedPBEventStream;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.retrieval.client.EpicsMessage;
import org.epics.archiverappliance.retrieval.client.GenMsgIterator;
import org.epics.archiverappliance.retrieval.client.InfoChangeHandler;
import org.epics.archiverappliance.retrieval.client.RawDataRetrieval;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import static org.epics.archiverappliance.config.ConfigServiceForTests.MGMT_URL;

/**
 * Michael DavidSaver supplied the data for this test.
 * We have raw data and data from another PV that applies the ADEL.
 * <ol>
 * <li>sig1-wo-adel.pb - the raw result of TST-CT{}Sig:1-I which has ADEL=MDEL=0.</li>
 * <li>sig2-w-adel.pb - TST-CT{}Sig:2-I with ADEL=MDEL=2</li>
 * <li>The time range in question is Dec/10/2014 14:12:42 through 14:12:55 EST.</li>
 * </ol>
 * @author mshankar
 *
 */
@Tag("integration")
public class DeadBandTest {
    private static Logger logger = LogManager.getLogger(DeadBandTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    private String pvName = "UnitTestNoNamingConvention:inactive1";
    private String ltsFolderName = System.getenv("ARCHAPPL_LONG_TERM_FOLDER");
    private File ltsFolder = new File(ltsFolderName);

    private static void mergeHeaders(PayloadInfo info, HashMap<String, String> headers) {
        int headerCount = info.getHeadersCount();
        for (int i = 0; i < headerCount; i++) {
            String headerName = info.getHeaders(i).getName();
            String headerValue = info.getHeaders(i).getVal();
            logger.debug("Adding header " + headerName + " = " + headerValue);
            headers.put(headerName, headerValue);
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());

        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();

        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
    }

    @Test
    public void testRetrievalPV1() throws Exception {
        File destFile = new File(ltsFolder + "/TST-CT{}Sig/1-I:2014.pb");
        String srcFile = "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/deadband/sig1-wo-adel.pb";
        destFile.getParentFile().mkdirs();
        FileUtils.copyFile(new File(srcFile), destFile);
        Assertions.assertTrue(destFile.exists(), destFile.getAbsolutePath() + "does not exist");

        // Load a sample PVTypeInfo from a prototype file.
        JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File(
                "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
        PVTypeInfo srcPVTypeInfo = new PVTypeInfo();
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        decoder.decode(srcPVTypeInfoJSON, srcPVTypeInfo);
        Assertions.assertTrue(
                srcPVTypeInfo.getPvName().equals(pvName),
                "Expecting PV typeInfo for " + pvName + "; instead it is " + srcPVTypeInfo.getPvName());
        String newPVName = "TST-CT{}Sig:1-I";
        PVTypeInfo newPVTypeInfo = new PVTypeInfo(newPVName, srcPVTypeInfo);
        newPVTypeInfo.setPaused(true);
        newPVTypeInfo.setChunkKey("TST-CT{}Sig/1-I:");
        JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
        GetUrlContent.postDataAndGetContentAsJSONObject(
                MGMT_URL + "/putPVTypeInfo?pv=" + URLEncoder.encode(newPVName, "UTF-8") + "&createnew=true",
                encoder.encode(newPVTypeInfo));

        logger.info("Sample file copied to " + destFile.getAbsolutePath());

        Instant start = TimeUtils.convertFromISO8601String("2014-12-10T19:10:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String("2014-12-10T19:15:55.000Z");

        checkRetrieval(newPVName, start, end, 37, true);
        try (FileBackedPBEventStream compareStream = new FileBackedPBEventStream(
                "TST-CT{}Sig:2-I",
                Paths.get("src/test/org/epics/archiverappliance/retrieval/postprocessor/data/deadband/sig2-w-adel.pb"),
                ArchDBRTypes.DBR_SCALAR_DOUBLE)) {
            compareStreams("deadBand_2.0(" + newPVName + ")", start, end, compareStream);
        }
    }

    private int checkRetrieval(
            String retrievalPVName, Instant start, Instant end, int expectedAtLeastEvents, boolean exactMatch)
            throws IOException {
        long startTimeMillis = System.currentTimeMillis();
        RawDataRetrieval rawDataRetrieval = new RawDataRetrieval(ConfigServiceForTests.RAW_RETRIEVAL_URL);
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
            Assertions.assertTrue(
                    strm != null,
                    "We should get some data for " + retrievalPVName + " , we are getting a null stream back");
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

        logger.info("For " + retrievalPVName + "we were expecting " + expectedAtLeastEvents + "events. We got "
                + eventCount);
        Assertions.assertTrue(
                eventCount >= expectedAtLeastEvents,
                "Expecting " + expectedAtLeastEvents + "events for " + retrievalPVName + ". We got " + eventCount);
        if (exactMatch) {
            Assertions.assertTrue(
                    eventCount == expectedAtLeastEvents,
                    "Expecting " + expectedAtLeastEvents + "events for " + retrievalPVName + ". We got " + eventCount);
        }
        return eventCount;
    }

    private void compareStreams(
            String retrievalPVName, Instant start, Instant end, FileBackedPBEventStream compareStream)
            throws IOException {
        long startTimeMillis = System.currentTimeMillis();
        RawDataRetrieval rawDataRetrieval = new RawDataRetrieval(ConfigServiceForTests.RAW_RETRIEVAL_URL);
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
            Assertions.assertTrue(
                    strm != null,
                    "We should get some data for " + retrievalPVName + " , we are getting a null stream back");
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

            Iterator<Event> compareIt = compareStream.iterator();

            for (EpicsMessage dbrEvent : strm) {
                Assertions.assertTrue(compareIt.hasNext(), "We seem to have run out of events at " + eventCount);
                Event compareEvent = compareIt.next();
                Assertions.assertTrue(
                        TimeUtils.fromSQLTimeStamp(dbrEvent.getTimestamp()).equals(compareEvent.getEventTimeStamp()),
                        "At event " + eventCount + ", from the operator we have an event at "
                                + TimeUtils.convertToISO8601String(TimeUtils.fromSQLTimeStamp(dbrEvent.getTimestamp()))
                                + " and from the compare stream, we have an event at "
                                + TimeUtils.convertToISO8601String(compareEvent.getEventTimeStamp()));

                eventCount++;
            }

            logger.info("Retrival for " + retrievalPVName + "=" + (endTimeMillis - startTimeMillis) + "(ms)");
        }
    }
}
