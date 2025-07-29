package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.plain.pb.PBCompressionMode;
import edu.stanford.slac.archiverappliance.plain.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.plain.PlainPBStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
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
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Generate known amount of data for a PV; corrupt known number of the values.
 * Retrieve data using mean_600 and raw and make sure we do not drop the stream entirely.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class PostProcessorWithPBErrorTest {
    private static final Logger logger = LogManager.getLogger(PostProcessorWithPBErrorTest.class.getName());
    private final String pvName = "UnitTestNoNamingConvention:inactive1";
    private final short currentYear = TimeUtils.getCurrentYear();
    private final String ltsFolderName = System.getenv("ARCHAPPL_LONG_TERM_FOLDER");
    private final File ltsFolder = new File(ltsFolderName + "/UnitTestNoNamingConvention");
    private final short dataGeneratedForYears = 5;
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();
    StoragePlugin storageplugin;
    private ConfigServiceForTests configService;

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
        configService = new ConfigServiceForTests(-1);
        storageplugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR",
                configService);
        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());

        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
        try (BasicContext context = new BasicContext()) {
            for (short y = dataGeneratedForYears; y > 0; y--) {
                short year = (short) (currentYear - y);
                for (int day = 0; day < 365; day++) {
                    ArrayListEventStream testData = new ArrayListEventStream(
                            PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                            new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
                    int startofdayinseconds = day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                    for (int secondintoday = 0;
                            secondintoday < PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
                            secondintoday += 60) {
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
    public void testRetrievalWithPostprocessingAndCorruption() throws Exception {
        String pvNameToArchive = pvName;
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
                mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);

        int totalCount = checkRetrieval(pvName, dataGeneratedForYears * 365 * 24 * 60, true);
        logger.info("*** -> Corrupting some data now");
        corruptSomeData();

        // We have now archived this PV, get some data and validate we got the expected number of events
        // We generated data for dataGeneratedForYears years; one sample every minute
        // We should get 365*24*60 events if things were ok.
        // However, we corrupted each file; so we should lose maybe 1000 events per file?
        checkRetrieval(pvName, totalCount - dataGeneratedForYears * 1000, false);
        checkRetrieval("mean_600(" + pvName + ")", totalCount / 10 - dataGeneratedForYears * 100, false);
        checkRetrieval("firstSample_600(" + pvName + ")", totalCount / 10 - dataGeneratedForYears * 100, false);
        checkRetrieval("lastSample_600(" + pvName + ")", totalCount / 10 - dataGeneratedForYears * 100, false);
    }

    private int checkRetrieval(String retrievalPVName, int expectedAtLeastEvents, boolean exactMatch)
            throws IOException {
        long startTimeMillis = System.currentTimeMillis();
        RawDataRetrieval rawDataRetrieval = new RawDataRetrieval(ConfigServiceForTests.RAW_RETRIEVAL_URL);
        Instant now = TimeUtils.now();
        Instant start = TimeUtils.minusDays(now, (dataGeneratedForYears + 1) * 366);
        int eventCount = 0;

        final HashMap<String, String> metaFields = new HashMap<String, String>();
        // Make sure we get the EGU as part of a regular VAL call.
        try (GenMsgIterator strm = rawDataRetrieval.getDataForPVs(
                Arrays.asList(retrievalPVName),
                TimeUtils.toSQLTimeStamp(start),
                TimeUtils.toSQLTimeStamp(now),
                false,
                null)) {
            PayloadInfo info = null;
            Assertions.assertNotNull(strm, "We should get some data, we are getting a null stream back");
            info = strm.getPayLoadInfo();
            Assertions.assertNotNull(info, "Stream has no payload info");
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
                "Expecting " + expectedAtLeastEvents + "events. We got " + eventCount);
        if (exactMatch) {
            Assertions.assertEquals(
                    eventCount,
                    expectedAtLeastEvents,
                    "Expecting " + expectedAtLeastEvents + "events. We got " + eventCount);
        }
        return eventCount;
    }

    private void corruptSomeData() throws Exception {
        try (BasicContext context = new BasicContext()) {
            Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    ltsFolderName,
                    pvName,
                    PlainPBStoragePlugin.pbFileExtension,
                    PartitionGranularity.PARTITION_DAY,
                    PBCompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
            Assertions.assertTrue(true);
            Assertions.assertTrue(paths.length > 0);
            // Corrupt each file
            for (Path path : paths) {
                try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
                    Random random = new Random();
                    // Seek to a well defined spot.
                    int bytesToOverwrite = 100;
                    long randomSpot = 512 + (long) ((channel.size() - 512) * 0.33);
                    channel.position(randomSpot - bytesToOverwrite);
                    ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
                    byte[] junk = new byte[bytesToOverwrite];
                    // Write some garbage
                    random.nextBytes(junk);
                    buf.put(junk);
                    buf.flip();
                    channel.write(buf);
                }
            }
        }
    }
}
