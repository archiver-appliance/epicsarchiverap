/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import static edu.stanford.slac.archiverappliance.plain.pb.PBPlainFileHandler.PB_PLUGIN_IDENTIFIER;
import static org.epics.archiverappliance.utils.ui.URIUtils.pluginString;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;

/**
 * Test the getDataAtTime API when using the merge dedup plugin.
 * Generate data such that each failover cluster has one of known data on the morning or afternoon.
 * @author mshankar
 *
 */
@Tag("integration")
@Disabled(
        "This is a complex use case and we don't support this in the main code yet; keeping this test around just in case")
public class FailoverScoreAPITest {
    private static final Logger logger = LogManager.getLogger(FailoverScoreAPITest.class.getName());
    String pvName = "FailoverScoreAPITest";
    ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
    TomcatSetup tomcatSetup = new TomcatSetup();
    private ConfigServiceForTests configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        tomcatSetup.setUpFailoverWithWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    /**
     * Generate a months worth of data for the given appserver; one per day, a boolean indicating if the sample is in
     * the morning or afternoon.
     *
     * @param applURL       - The URL for the appliance.
     * @param applianceName - The name of the appliance
     * @param theMonth      - The month we generate data for. We generate a month's worth of MTS data.
     * @param morningp      - If true; data is generated for the morning else afternoon.
     * @throws Exception
     */
    private void generateMTSData(String applURL, String applianceName, Instant theMonth, boolean morningp)
            throws Exception {
        int genEventCount = 0;
        String folderName = "build/tomcats/tomcat_" + this.getClass().getSimpleName() + "/" + applianceName + "/mts";
        logger.info("Generating data into folder "
                + Paths.get(folderName).toAbsolutePath().toString());
        StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(
                pluginString(
                        PB_PLUGIN_IDENTIFIER,
                        "localhost",
                        "name=LTS&rootFolder=" + folderName + "&partitionGranularity=PARTITION_DAY"),
                configService);
        try (BasicContext context = new BasicContext()) {
            ArrayListEventStream strm = new ArrayListEventStream(
                    0,
                    new RemotableEventStreamDesc(
                            ArchDBRTypes.DBR_SCALAR_DOUBLE,
                            pvName,
                            TimeUtils.convertToYearSecondTimestamp(theMonth).getYear()));
            for (Instant s = TimeUtils.getPreviousPartitionLastSecond(theMonth, PartitionGranularity.PARTITION_DAY)
                            .plusSeconds(1);
                    s.isBefore(TimeUtils.now());
                    s = s.plusSeconds(PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk())) {

                POJOEvent pojoEvent = new POJOEvent(
                        ArchDBRTypes.DBR_SCALAR_DOUBLE,
                        s.plusSeconds(
                                morningp
                                        ? 10L * PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk()
                                        : 20L * PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk()),
                        new ScalarValue<Double>((double) (morningp ? 10 : 20)),
                        0,
                        0);
                logger.debug(
                        "Generating event at " + TimeUtils.convertToHumanReadableString(pojoEvent.getEventTimeStamp()));
                strm.add(pojoEvent);
                genEventCount++;
            }
            plugin.appendData(context, pvName, strm);
        }
        logger.info("Done generating data for appliance " + applianceName);

        JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(
                "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json")));
        PVTypeInfo destPVTypeInfo = new PVTypeInfo();
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
        decoder.decode(srcPVTypeInfoJSON, destPVTypeInfo);

        destPVTypeInfo.setPaused(true);
        destPVTypeInfo.setPvName(pvName);
        destPVTypeInfo.setApplianceIdentity(applianceName);
        destPVTypeInfo.setChunkKey(configService.getPVNameToKeyConverter().convertPVNameToKey(pvName));
        destPVTypeInfo.setCreationTime(TimeUtils.convertFromISO8601String("2020-11-11T14:49:58.523Z"));
        destPVTypeInfo.setModificationTime(TimeUtils.now());
        GetUrlContent.postDataAndGetContentAsJSONObject(
                applURL + "/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                        + "&override=true&createnew=true",
                encoder.encode(destPVTypeInfo));
        logger.info("Added " + pvName + " to the appliance " + applianceName);

        RawDataRetrievalAsEventStream rawDataRetrieval =
                new RawDataRetrievalAsEventStream(applURL + "/retrieval/data/getData.raw");
        long rtvlEventCount = 0;
        try (EventStream stream = rawDataRetrieval.getDataForPVS(
                new String[] {pvName},
                TimeUtils.minusDays(TimeUtils.now(), 90),
                TimeUtils.plusDays(TimeUtils.now(), 31),
                null)) {
            long lastEvEpoch = 0;
            if (stream != null) {
                for (Event e : stream) {
                    long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
                    if (lastEvEpoch != 0) {
                        Assertions.assertTrue(
                                (evEpoch - lastEvEpoch)
                                        == PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                                "We got events more than "
                                        + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk()
                                        + " seconds apart " + TimeUtils.convertToHumanReadableString(lastEvEpoch)
                                        + " and  " + TimeUtils.convertToHumanReadableString(evEpoch));
                    }
                    lastEvEpoch = evEpoch;
                    rtvlEventCount++;
                }
            } else {
                Assertions.fail("Stream is null when retrieving data.");
            }
        }
        Assertions.assertEquals(
                genEventCount,
                rtvlEventCount,
                "We expected event count  " + genEventCount + " but got  " + rtvlEventCount);
    }

    private void changeMTSForDest() throws Exception {
        JSONObject srcPVTypeInfoJSON =
                GetUrlContent.getURLContentAsJSONObject("http://localhost:17665/mgmt/bpl/getPVTypeInfo?pv="
                        + URLEncoder.encode(pvName, StandardCharsets.UTF_8));
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
        PVTypeInfo destPVTypeInfo = new PVTypeInfo();
        decoder.decode(srcPVTypeInfoJSON, destPVTypeInfo);
        String otherURL = "pbraw://localhost?name=MTS&rawURL="
                + URLEncoder.encode("http://localhost:17669/retrieval/data/getData.raw", StandardCharsets.UTF_8);
        destPVTypeInfo.getDataStores()[1] = "merge://localhost?name=MTS&dest="
                + URLEncoder.encode(destPVTypeInfo.getDataStores()[1], StandardCharsets.UTF_8)
                + "&other=" + URLEncoder.encode(otherURL, StandardCharsets.UTF_8);
        logger.info("Data store is " + destPVTypeInfo.getDataStores()[1]);

        GetUrlContent.postDataAndGetContentAsJSONObject(
                "http://localhost:17665/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                        + "&override=true&createnew=true",
                encoder.encode(destPVTypeInfo));
        logger.info("Changed " + pvName + " to a merge dedup plugin");
    }

    @SuppressWarnings("unchecked")
    private void testDataAtTime(long epochSecs, boolean morningp) throws Exception {
        String scoreURL = "http://localhost:17665/retrieval/data/getDataAtTime.json?at="
                + TimeUtils.convertToISO8601String(epochSecs);
        JSONArray array = new JSONArray();
        array.add(pvName);
        Map<String, Map<String, Object>> ret =
                (Map<String, Map<String, Object>>) GetUrlContent.postDataAndGetContentAsJSONObject(scoreURL, array);
        Assertions.assertTrue(
                ret.size() > 0,
                "We expected some data back from getDataAtTime at " + TimeUtils.convertToISO8601String(epochSecs));
        for (String retpvName : ret.keySet()) {
            Map<String, Object> val = ret.get(retpvName);
            if (retpvName.equals(pvName)) {
                logger.info("Asking for value at " + TimeUtils.convertToISO8601String(epochSecs) + " got value at "
                        + TimeUtils.convertToISO8601String((long) val.get("secs")));
                Assertions.assertEquals(
                        (double) val.get("val"),
                        (morningp ? 10 : 20),
                        "We expected a morning value for " + TimeUtils.convertToISO8601String(epochSecs)
                                + " instead we got " + TimeUtils.convertToISO8601String((long) val.get("secs")));
                return;
            }
        }

        Assertions.fail("We did not receive a value for PV ");
    }

    @Test
    public void testRetrieval() throws Exception {
        // Register the PV with both appliances and generate data.
        Instant lastMonth = TimeUtils.minusDays(TimeUtils.now(), 2 * 31);
        generateMTSData("http://localhost:17665", "dest_appliance", lastMonth, true);
        generateMTSData("http://localhost:17669", "other_appliance", lastMonth, false);

        changeMTSForDest();

        for (int i = 0; i < 20; i++) {
            long startOfDay = (TimeUtils.convertToEpochSeconds(TimeUtils.minusDays(TimeUtils.now(), -1 * (i - 25)))
                            / PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk())
                    * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
            for (int h = 0; h < 24; h++) {
                logger.info("Looking for value of PV at  "
                        + TimeUtils.convertToHumanReadableString(startOfDay
                                + (long) h * PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk()));
                testDataAtTime(
                        startOfDay + (long) h * PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk(),
                        (h >= 10 && h < 20));
            }
        }
    }
}
