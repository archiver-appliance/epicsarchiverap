package org.epics.archiverappliance.retrieval.saverestore;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.time.Instant;
import java.time.Period;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


/*
 * Test for issue 375.
 * We want to generate some semi-realistic data for a GetDataAtTime test with data spread across STS/MTS/LTS and then test GetDataAtTime under various scenarios.
 * We're generating data in all three storee; one sample an hour...
 */

public class GetDataAtTimeSpanningStoresTest {
    private static final Logger logger = LogManager.getLogger(GetDataAtTimeSpanningStoresTest.class.getName());
    String pvName = GetDataAtTimeSpanningStoresTest.class.getSimpleName();
    TomcatSetup tomcatSetup = new TomcatSetup();
    private ConfigServiceForTests configService;
    String folderSTS =
            ConfigServiceForTests.getDefaultShortTermFolder() + "/" + GetDataAtTimeSpanningStoresTest.class.getSimpleName() + "/sts";
    String folderMTS =
            ConfigServiceForTests.getDefaultPBTestFolder() + "/" + GetDataAtTimeSpanningStoresTest.class.getSimpleName() + "/mts";
    String folderLTS =
            ConfigServiceForTests.getDefaultPBTestFolder() + "/" + GetDataAtTimeSpanningStoresTest.class.getSimpleName() + "/lts";


    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        for(String fldr : new String[] { folderSTS, folderMTS, folderLTS  }) {
            if (new File(fldr).exists()) {
                FileUtils.deleteDirectory(new File(fldr));
            }
            assert new File(fldr).mkdirs();
        }
        System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER", folderSTS);
        System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", folderMTS);
        System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER", folderLTS);
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        for(String fldr : new String[] { folderSTS, folderMTS, folderLTS  }) {
            if (new File(fldr).exists()) {
                FileUtils.deleteDirectory(new File(fldr));
            }
        }
    }

    // The various gapBetweenSamplesInMins is to mix up the number of samples in a file
    @SuppressWarnings("unchecked")
    @ValueSource(ints = {60, 59, 13})
    @ParameterizedTest
    public void testGetDataAtTime(int gapBetweenSamplesInMins) throws Exception {
        PVTypeInfo theInfo = this.addPVToCluster(pvName);
        generateDataIntoPlugin(
            folderLTS,
            theInfo.getDataStores()[2],
            Instant.now().minus(Period.parse("P100D")),
            Instant.now().minus(Period.parse("P10D")),
            gapBetweenSamplesInMins);
        generateDataIntoPlugin(
            folderMTS,
            theInfo.getDataStores()[1],
            Instant.now().minus(Period.parse("P10D")),
            Instant.now().minus(Period.parse("P1D")),
            gapBetweenSamplesInMins);
        generateDataIntoPlugin(
            folderSTS,
            theInfo.getDataStores()[0],
            Instant.now().minus(Period.parse("P1D")),
            Instant.now(),
            gapBetweenSamplesInMins);

        ArrayListEventStream getData = this.getData(
            Instant.now().minus(Period.parse("P101D")),
            Instant.now());
        int samplesize = getData.size();
        Assertions.assertTrue(samplesize >= (100*24*60/gapBetweenSamplesInMins)/2,
            "We expected more than " + samplesize + " events");

        JSONArray pvs = new JSONArray();
        pvs.add(pvName);
        // We loop backwards in getData and make use we get the previous sample when we ask for getDataAtTime at that instant.
        for(int i = samplesize-1; i > 0; i--) {
            Instant atTime = getData.get(i).getEventTimeStamp().minusMillis(500);
            logger.debug("Checking getDataAtTime at time " + TimeUtils.convertToHumanReadableString(atTime));
            String getDataAtTimeURL = ConfigServiceForTests.GETDATAATTIME_URL + "?at=" + TimeUtils.convertToISO8601String(atTime);
            JSONObject resp = GetUrlContent.postDataAndGetContentAsJSONObject(getDataAtTimeURL, pvs);
            Assertions.assertTrue(resp.size() >= 1, "We expected at least one sample back, we got " + resp.size());
            Instant expectedTimeStamp = getData.get(i-1).getEventTimeStamp().with(ChronoField.MILLI_OF_SECOND, 0);
            Instant obtainedTimeStamp = Instant.ofEpochSecond((long)((JSONObject)resp.get(pvName)).get("secs"));
            Assertions.assertEquals(
                expectedTimeStamp,
                obtainedTimeStamp,
                "Expected " + 
                TimeUtils.convertToHumanReadableString(expectedTimeStamp) + 
                " but we got " + 
                TimeUtils.convertToHumanReadableString(obtainedTimeStamp) + 
                " for getDataAtTime at " + 
                TimeUtils.convertToHumanReadableString(atTime) + 
                " at index " + i
                );
        }
    }


    private PVTypeInfo addPVToCluster(String pvName) throws Exception {
        // Load a sample PVTypeInfo from a prototype file.
        JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File(
                "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
        PVTypeInfo srcPVTypeInfo = new PVTypeInfo();
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        decoder.decode(srcPVTypeInfoJSON, srcPVTypeInfo);
        PVTypeInfo newPVTypeInfo = new PVTypeInfo(pvName, srcPVTypeInfo);
        newPVTypeInfo.setPaused(true);
        newPVTypeInfo.setApplianceIdentity("appliance0");
        newPVTypeInfo.setChunkKey(pvName + ":");
        Assertions.assertTrue(
                newPVTypeInfo.getPvName().equals(pvName),
                "Expecting PV typeInfo for " + pvName + "; instead it is " + srcPVTypeInfo.getPvName());
        JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
        GetUrlContent.postDataAndGetContentAsJSONObject(
                "http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8")
                        + "&createnew=true",
                encoder.encode(newPVTypeInfo));
        return newPVTypeInfo;
    }

    private void generateDataIntoPlugin(String dataFolder, String pluginDesc, Instant startTime, Instant endTime, int gapBetweenSamplesInMins) throws IOException {
        if (new File(dataFolder).exists()) {
            FileUtils.deleteDirectory(new File(dataFolder));
        }
        assert new File(dataFolder).mkdirs();        

        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(pluginDesc, configService);
        Instant startOfData = Instant.ofEpochSecond(((startTime.getEpochSecond()/3600)+1)*3600);        
        try (BasicContext context = new BasicContext()) {
            ArrayListEventStream strm = new ArrayListEventStream(
                0,
                new RemotableEventStreamDesc(
                        ArchDBRTypes.DBR_SCALAR_DOUBLE,
                        pvName,
                        TimeUtils.convertToYearSecondTimestamp(startOfData).getYear()));
            for (Instant ts = startOfData; ts.isBefore(endTime); ts = ts.plus(gapBetweenSamplesInMins, ChronoUnit.MINUTES)) {
                strm.add(new POJOEvent(
                        ArchDBRTypes.DBR_SCALAR_DOUBLE,
                        ts,
                        new ScalarValue<Double>((double) ts.getEpochSecond()),
                        0,
                        0));
            }
            storagePlugin.appendData(context, pvName, strm);            
        }
    }

    private ArrayListEventStream getData(Instant start, Instant end) throws Exception {
        RawDataRetrievalAsEventStream rawDataRetrieval =
                new RawDataRetrievalAsEventStream(ConfigServiceForTests.RAW_RETRIEVAL_URL);

        ArrayListEventStream strm = new ArrayListEventStream(
            0,
            new RemotableEventStreamDesc(
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                pvName,
                TimeUtils.convertToYearSecondTimestamp(start).getYear()));
    
        try(EventStream stream = rawDataRetrieval.getDataForPVS(
            new String[] {pvName},
            start,
            end,
            desc -> logger.info("Getting data for PV " + desc.getPvName()))) {
                for(Event ev : stream) {
                    strm.add(ev);
                }

        }
        return strm;
    }
    
}
