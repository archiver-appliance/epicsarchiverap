package org.epics.archiverappliance.retrieval.cluster;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONArray;
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
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.epics.archiverappliance.utils.ui.GetUrlContent.getURLContentAsJSONArray;

@Tag("integration")
public class ClusterSinglePVTest {
    private static final Logger logger = LogManager.getLogger(ClusterSinglePVTest.class.getName());
    private final TomcatSetup tomcatSetup = new TomcatSetup();
    private final String ltsFolder = System.getenv("ARCHAPPL_LONG_TERM_FOLDER");
    private final PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
    private final String prefixPvName = ClusterSinglePVTest.class.getSimpleName();
    private final String pvName = prefixPvName + ":dataretrieval";
    private final File ltsPVFolder = new File(ltsFolder + File.separator + prefixPvName);
    short year = TimeUtils.getCurrentYear();

    @BeforeEach
    public void setUp() throws Exception {
        tomcatSetup.setUpClusterWithWebApps(this.getClass().getSimpleName(), 2);

        if (ltsPVFolder.exists()) {
            FileUtils.deleteDirectory(ltsPVFolder);
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        if (ltsPVFolder.exists()) {
            FileUtils.deleteDirectory(ltsPVFolder);
        }
    }

    /**
     * Test to make sure that data is retrieved from across clusters.
     *
     * @throws Exception
     * @throws UnsupportedEncodingException
     */
    @Test
    public void singlePvsAcrossCluster() throws Exception {
        ConfigService configService = new ConfigServiceForTests(new File("./bin"));

        // Set up pbplugin so that data can be retrieved using the instance
        pbplugin.initialize("pb://localhost?name=LTS&rootFolder=" + ltsFolder + "&partitionGranularity=PARTITION_YEAR", configService);

        short currentYear = TimeUtils.getCurrentYear();
        String startString = currentYear + "-11-17T16:00:00.000Z";
        String endString = currentYear + "-11-17T16:01:00.000Z";

        Instant start = TimeUtils.convertFromISO8601String(startString);
        Instant end = TimeUtils.convertFromISO8601String(endString);

        // Generate an event stream to populate the PB files
        SimulationEventStream simstream = new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0), start, end, 1);
        try (BasicContext context = new BasicContext()) {
            pbplugin.appendData(context, pvName, simstream);
        }
        logger.info("Done generating data for PV in " + ltsPVFolder.getAbsolutePath());

        // Load a sample PVTypeInfo from a prototype file.
        JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream("src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json")));

        // Create target for decoded type info from JSON
        PVTypeInfo srcPVTypeInfo = new PVTypeInfo();

        // Decoder for PVTypeInfo
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);

        // Create type info from the data
        decoder.decode(srcPVTypeInfoJSON, srcPVTypeInfo);

        PVTypeInfo pvTypeInfo1 = new PVTypeInfo(pvName, srcPVTypeInfo);
        Assertions.assertEquals(pvTypeInfo1.getPvName(), pvName);

        JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);

        pvTypeInfo1.setPaused(true);
        pvTypeInfo1.setChunkKey(configService.getPVNameToKeyConverter().convertPVNameToKey(pvName));
        pvTypeInfo1.setCreationTime(TimeUtils.convertFromISO8601String("2013-11-11T14:49:58.523Z"));
        pvTypeInfo1.setModificationTime(TimeUtils.now());
        pvTypeInfo1.setApplianceIdentity("appliance1");
        GetUrlContent.postObjectAndGetContentAsJSONObject("http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8) + "&override=false&createnew=true", encoder.encode(pvTypeInfo1));

        logger.info("Added " + pvName + " to appliance0");

        try {
            Thread.sleep(5 * 1000);
        } catch (Exception ignored) {
        }

        Instant pluginstart = start.plusMillis(1000);

        Map<String, List<JSONObject>> pvToData = retrieveJsonResults(startString, endString, true);

        logger.info("Received response from server; now retrieving data using PBStoragePlugin Start: " + start + " End: " + end);

        try (BasicContext context = new BasicContext();
             EventStream pv1ResultsStream = new CurrentThreadWorkerEventStream(pvName, pbplugin.getDataForPV(context, pvName, pluginstart, end))) {
            compareDataAndInstants(pvName, pvToData.get(pvName), pv1ResultsStream);
        }
        Map<String, List<JSONObject>> pvToDataNoRedirect = retrieveJsonResults(startString, endString, false);

        logger.info("Received response from server; now retrieving data using PBStoragePlugin Start: " + start + " End: " + end);

        try (BasicContext context = new BasicContext();
             EventStream pv1ResultsStream = new CurrentThreadWorkerEventStream(pvName, pbplugin.getDataForPV(context, pvName, pluginstart, end))) {
            compareDataAndInstants(pvName, pvToDataNoRedirect.get(pvName), pv1ResultsStream);
        }
    }

    private Map<String, List<JSONObject>> retrieveJsonResults(String startString, String endString, boolean redirect) throws IOException {
        logger.info("Retrieving data using JSON/HTTP and comparing it to retrieval over PBStoragePlugin with redirect:{}", redirect);

        // Establish a connection with appliance0
        URL obj = new URL("http://localhost:17665/retrieval/data/getData.json?pv="
                + URLEncoder.encode(pvName, StandardCharsets.UTF_8) + "&from=" + URLEncoder.encode(startString, StandardCharsets.UTF_8)
                + "&to=" + URLEncoder.encode(endString, StandardCharsets.UTF_8));
        logger.info("Opening this URL: " + obj);
        JSONArray finalResult = getURLContentAsJSONArray(obj.toString(), true, redirect);

        Map<String, List<JSONObject>> pvToData = new HashMap<>();
        assert finalResult != null;
        int sizeOfResponse = finalResult.size();

        logger.info("Size of response: " + sizeOfResponse);

        logger.debug("First part: " + finalResult.get(0).toString());

        for (int i = 0; i < sizeOfResponse; i++) {
            logger.debug("Count on the for-loop: " + i);

            // Three explicit castings because Java
            String pvName = ((JSONObject) ((JSONObject) finalResult.get(i)).get("meta")).get("name").toString();

            logger.debug("Extracted PV name: " + pvName);

            // Data for PV
            JSONArray pvDataJson = (JSONArray) ((JSONObject) finalResult.get(i)).get("data");
            List<JSONObject> pvData = new ArrayList<>();
            for (Object o : pvDataJson) pvData.add((JSONObject) o);

            pvToData.put(pvName, pvData);

            logger.debug("Grabbed PV " + pvName + " with data " + pvData);
        }

        return pvToData;
    }

    private void compareDataAndInstants(String pvName, List<JSONObject> pvJsonData, EventStream pv1ResultsStream) throws NoSuchElementException {
        logger.info("Comparing data for pv " + pvName);
        int counter = 0;
        try {
            for (Event pluginEvent : pv1ResultsStream) {
                JSONObject jsonEvent = pvJsonData.get(counter++);

                // Get values
                double jsonValue = Double.parseDouble(jsonEvent.get("val").toString());
                double pluginValue = Double.parseDouble(pluginEvent.getSampleValue().toString());

                // Get seconds and nanoseconds for JSON
                String jsonSecondsPart = jsonEvent.get("secs").toString();
                String jsonNanosPart = jsonEvent.get("nanos").toString();
                String jsonInstant = jsonSecondsPart + ("000000000" + jsonNanosPart).substring(jsonNanosPart.length());

                // Get seconds and nanoseconds for plugin event
                String pluginSecondsPart = Long.toString(pluginEvent.getEpochSeconds());
                String pluginNanosPart = Integer.toString(pluginEvent.getEventTimeStamp().getNano());
                String pluginInstant = pluginSecondsPart + ("000000000" + pluginNanosPart).substring(pluginNanosPart.length());

                Assertions.assertEquals(jsonInstant, pluginInstant, "JSON timestamp, " + jsonInstant + ", and plugin event timestamp, " + pluginInstant + ", are unequal.");
                Assertions.assertEquals(jsonValue, pluginValue, 0.0, "JSON value, " + jsonValue + ", and plugin event value, " + pluginValue + ", are unequal.");
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IndexOutOfBoundsException("The data obtained from JSON and the plugin class for PV " + pvName + " are unequal in length.");
        }
    }
}
