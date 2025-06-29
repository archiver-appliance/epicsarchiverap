/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

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
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
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
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * A more complex test for testing ETL for failover.
 * "Other" generates even data for multiple months
 * "dest" generates odd data for multiple months.
 * We run ETL multiple times...
 * @author mshankar
 *
 */
@Tag("integration")
public class FailoverMultiStepETLTest {
    private static Logger logger = LogManager.getLogger(FailoverMultiStepETLTest.class.getName());
    private ConfigServiceForTests configService;
    String pvName = "FailoverMultiStepETLTest";
    ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
    TomcatSetup tomcatSetup = new TomcatSetup();
    long stepSeconds = 3600;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER", "../sts");
        System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", "../mts");
        System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER", "../lts");
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    /**
     * Generate a months of data for the other appliance.
     * @param applURL - The URL for the appliance.
     * @param applianceName - The name of the appliance
     * @param startTime - The query start time
     * @param endTime - The query end time
     * @param genEventCount - The expected event count.
     * @throws Exception
     */
    private long registerPVForOther(
            String applURL, String applianceName, Instant startTime, Instant endTime, long genEventCount)
            throws Exception {

        JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File(
                "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
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
                applURL + "/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, "UTF-8")
                        + "&override=true&createnew=true",
                encoder.encode(destPVTypeInfo));
        logger.info("Added " + pvName + " to the appliance " + applianceName);

        RawDataRetrievalAsEventStream rawDataRetrieval =
                new RawDataRetrievalAsEventStream(applURL + "/retrieval/data/getData.raw");
        long rtvlEventCount = 0;
        try (EventStream stream = rawDataRetrieval.getDataForPVS(new String[] {pvName}, startTime, endTime, null)) {
            long lastEvEpoch = 0;
            if (stream != null) {
                for (Event e : stream) {
                    long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
                    if (lastEvEpoch != 0) {
                        Assertions.assertTrue(
                                (evEpoch - lastEvEpoch) == stepSeconds,
                                "We got events more than " + stepSeconds + " seconds apart "
                                        + TimeUtils.convertToHumanReadableString(lastEvEpoch) + " and  "
                                        + TimeUtils.convertToHumanReadableString(evEpoch));
                    }
                    lastEvEpoch = evEpoch;
                    rtvlEventCount++;
                }
            } else {
                Assertions.fail("Stream is null when retrieving data.");
            }
        }
        Assertions.assertTrue(
                genEventCount == rtvlEventCount,
                "We expected event count  " + genEventCount + " but got  " + rtvlEventCount);
        return rtvlEventCount;
    }

    private int generateData(String applianceName, Instant ts, int startingOffset) throws IOException {
        int genEventCount = 0;
        StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=MTS&rootFolder=" + "build/tomcats/tomcat_"
                        + this.getClass().getSimpleName() + "/" + applianceName + "/mts"
                        + "&partitionGranularity=PARTITION_DAY",
                configService);
        try (BasicContext context = new BasicContext()) {
            ArrayListEventStream strm = new ArrayListEventStream(
                    0,
                    new RemotableEventStreamDesc(
                            ArchDBRTypes.DBR_SCALAR_DOUBLE,
                            pvName,
                            TimeUtils.convertToYearSecondTimestamp(ts).getYear()));

            for (Instant s = TimeUtils.getPreviousPartitionLastSecond(ts, PartitionGranularity.PARTITION_DAY)
                            .plusSeconds(1 + startingOffset); // We generate a months worth of data.
                    s.isBefore(TimeUtils.getNextPartitionFirstSecond(ts, PartitionGranularity.PARTITION_DAY));
                    s = s.plusSeconds(stepSeconds)) {
                POJOEvent genEvent = new POJOEvent(
                        ArchDBRTypes.DBR_SCALAR_DOUBLE, s, new ScalarValue<Double>((double) s.getEpochSecond()), 0, 0);
                strm.add(genEvent);
                genEventCount++;
            }
            plugin.appendData(context, pvName, strm);
        }
        return genEventCount;
    }

    private void changeMTSForDest() throws Exception {
        JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File(
                "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
        PVTypeInfo destPVTypeInfo = new PVTypeInfo();
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        decoder.decode(srcPVTypeInfoJSON, destPVTypeInfo);

        destPVTypeInfo.setPaused(false);
        destPVTypeInfo.setPvName(pvName);
        destPVTypeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
        destPVTypeInfo.setChunkKey(configService.getPVNameToKeyConverter().convertPVNameToKey(pvName));
        destPVTypeInfo.setCreationTime(TimeUtils.convertFromISO8601String("2020-11-11T14:49:58.523Z"));
        destPVTypeInfo.setModificationTime(TimeUtils.now());
        String otherURL = "pbraw://localhost?name=MTS&rawURL="
                + URLEncoder.encode("http://localhost:17665/retrieval/data/getData.raw", "UTF-8");
        destPVTypeInfo.getDataStores()[1] = "merge://localhost?name=MTS&dest="
                + URLEncoder.encode(destPVTypeInfo.getDataStores()[1], "UTF-8")
                + "&other=" + URLEncoder.encode(otherURL, "UTF-8");
        configService.updateTypeInfoForPV(pvName, destPVTypeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();
    }

    private long testMergedRetrieval(String applianceName, Instant startTime, Instant endTime, boolean expectContinous)
            throws Exception {
        long rtvlEventCount = 0;
        long lastEvEpoch = 0;
        StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=LTS&rootFolder=" + "build/tomcats/tomcat_"
                        + this.getClass().getSimpleName() + "/" + applianceName + "/lts"
                        + "&partitionGranularity=PARTITION_YEAR",
                configService);
        try (BasicContext context = new BasicContext()) {
            logger.info("Looking for data " + plugin.getDescription() + " from "
                    + TimeUtils.convertToHumanReadableString(startTime) + " and "
                    + TimeUtils.convertToHumanReadableString(endTime));
            List<Callable<EventStream>> callables =
                    plugin.getDataForPV(context, pvName, startTime, endTime, new DefaultRawPostProcessor());
            for (Callable<EventStream> callable : callables) {
                EventStream ev = callable.call();
                for (Event e : ev) {
                    long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
                    logger.debug("Current event " + TimeUtils.convertToHumanReadableString(evEpoch) + " Previous: "
                            + TimeUtils.convertToHumanReadableString(lastEvEpoch));
                    if (lastEvEpoch != 0) {
                        Assertions.assertTrue(
                                evEpoch > lastEvEpoch,
                                "We got events out of order " + TimeUtils.convertToHumanReadableString(lastEvEpoch)
                                        + " and  " + TimeUtils.convertToHumanReadableString(evEpoch)
                                        + " at event count " + rtvlEventCount);
                    }
                    lastEvEpoch = evEpoch;
                    rtvlEventCount++;
                }
            }
        }
        return rtvlEventCount;
    }

    @Test
    public void testETL() throws Exception {
        configService.getETLLookup().manualControlForUnitTests();
        // Register the PV with both appliances and generate data.
        Instant startTime = TimeUtils.minusDays(TimeUtils.now(), 365);
        Instant endTime = TimeUtils.now();

        long oCount = 0;
        for (Instant ts = startTime; ts.isBefore(endTime); ts = TimeUtils.plusDays(ts, 1)) {
            oCount = oCount + generateData(ConfigServiceForTests.TESTAPPLIANCE0, ts, 0);
        }
        registerPVForOther(
                "http://localhost:17665",
                ConfigServiceForTests.TESTAPPLIANCE0,
                TimeUtils.minusDays(TimeUtils.now(), 5 * 365),
                TimeUtils.plusDays(TimeUtils.now(), 10),
                oCount);

        System.getProperties()
                .put(
                        "ARCHAPPL_SHORT_TERM_FOLDER",
                        "build/tomcats/tomcat_" + this.getClass().getSimpleName() + "/" + "dest_appliance" + "/sts");
        System.getProperties()
                .put(
                        "ARCHAPPL_MEDIUM_TERM_FOLDER",
                        "build/tomcats/tomcat_" + this.getClass().getSimpleName() + "/" + "dest_appliance" + "/mts");
        System.getProperties()
                .put(
                        "ARCHAPPL_LONG_TERM_FOLDER",
                        "build/tomcats/tomcat_" + this.getClass().getSimpleName() + "/" + "dest_appliance" + "/lts");

        long dCount = 0;
        for (Instant ts = startTime; ts.isBefore(endTime); ts = TimeUtils.plusDays(ts, 1)) {
            dCount = dCount + generateData("dest_appliance", ts, 1);
        }
        testMergedRetrieval(
                "dest_appliance",
                TimeUtils.minusDays(TimeUtils.now(), 5 * 365),
                TimeUtils.plusDays(TimeUtils.now(), 10),
                false);
        long totCount = dCount + oCount;

        changeMTSForDest();
        long lastCount = 0;
        for (Instant ts = startTime; ts.isBefore(endTime); ts = TimeUtils.plusDays(ts, 1)) {
            Instant queryStart = TimeUtils.minusDays(TimeUtils.now(), 5 * 365), queryEnd = TimeUtils.plusDays(ts, 10);
            Instant timeETLruns = TimeUtils.getNextPartitionFirstSecond(ts, PartitionGranularity.PARTITION_DAY)
                    .plusSeconds(60);
            // Add 3 days to take care of the hold and gather
            timeETLruns = TimeUtils.plusDays(timeETLruns, 3);
            logger.info("Running ETL now as if it is " + TimeUtils.convertToHumanReadableString(timeETLruns));
            ETLExecutor.runETLs(configService, timeETLruns);
            long rCount = testMergedRetrieval("dest_appliance", queryStart, queryEnd, false);
            logger.info("Got " + rCount + " events between " + TimeUtils.convertToHumanReadableString(queryStart)
                    + " and " + TimeUtils.convertToHumanReadableString(queryEnd));
            Assertions.assertTrue(
                    (rCount >= lastCount),
                    "We expected more than what we got last time " + lastCount + " . This time we got " + rCount);
            lastCount = rCount;
        }
        Assertions.assertTrue(lastCount == totCount, "We expected event count  " + totCount + " but got  " + lastCount);
    }
}
