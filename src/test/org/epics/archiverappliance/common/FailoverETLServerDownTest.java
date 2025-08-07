/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import static edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin.PB_PLUGIN_IDENTIFIER;
import static org.epics.archiverappliance.config.ConfigServiceForTests.DATA_RETRIEVAL_URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Test basic failover - test the ETL side of things when the other server is down...
 * @author mshankar
 *
 */
class FailoverETLServerDownTest {
    private static final Logger logger = LogManager.getLogger(FailoverETLServerDownTest.class.getName());
    String pvName = FailoverETLServerDownTest.class.getSimpleName();
    long tCount = 0;
    long stepSeconds = 2;
    File testLTSFolder = new File(System.getenv("ARCHAPPL_LONG_TERM_FOLDER")
            + File.separator
            + FailoverETLServerDownTest.class.getSimpleName());
    File testMTSFolder = new File(System.getenv("ARCHAPPL_MEDIUM_TERM_FOLDER")
            + File.separator
            + FailoverETLServerDownTest.class.getSimpleName());
    private ConfigServiceForTests configService;

    @BeforeEach
    void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
    }

    @AfterEach
    void tearDown() {
        configService.shutdownNow();
    }

    private int generateData(Instant lastMonth) throws IOException {
        int genEventCount = 0;
        StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(
                PB_PLUGIN_IDENTIFIER + "://localhost?name=MTS&rootFolder=" + testMTSFolder
                        + "&partitionGranularity=PARTITION_DAY",
                configService);
        try (BasicContext context = new BasicContext()) {
            Instant start = TimeUtils.getPreviousPartitionLastSecond(lastMonth, PartitionGranularity.PARTITION_MONTH)
                    .plusSeconds(1 + 1);
            ArrayListEventStream strm = new ArrayListEventStream(
                    0,
                    new RemotableEventStreamDesc(
                            ArchDBRTypes.DBR_SCALAR_DOUBLE,
                            pvName,
                            TimeUtils.convertToYearSecondTimestamp(start).getYear()));
            for (Instant s = start; // We generate a months worth of data.
                    s.isBefore(TimeUtils.getNextPartitionFirstSecond(lastMonth, PartitionGranularity.PARTITION_MONTH));
                    s = s.plusSeconds(stepSeconds)) {

                strm.add(new POJOEvent(
                        ArchDBRTypes.DBR_SCALAR_DOUBLE, s, new ScalarValue<>((double) s.getEpochSecond()), 0, 0));
                genEventCount++;
                assert plugin != null;
            }
            assert plugin != null;
            plugin.appendData(context, pvName, strm);
        }
        logger.info("Done generating dest data");
        return genEventCount;
    }

    private void changeMTSForDest() throws Exception {
        JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(
                "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json")));
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
                + URLEncoder.encode(DATA_RETRIEVAL_URL + "/data/getData.raw", StandardCharsets.UTF_8);
        destPVTypeInfo.getDataStores()[1] = "merge://localhost?name=MTS&dest="
                + URLEncoder.encode(destPVTypeInfo.getDataStores()[1], StandardCharsets.UTF_8)
                + "&other=" + URLEncoder.encode(otherURL, StandardCharsets.UTF_8);
        configService.updateTypeInfoForPV(pvName, destPVTypeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();
    }

    private long testMergedRetrieval(String pluginURL, Instant startTime, Instant endTime) throws Exception {
        long rtvlEventCount = 0;
        long lastEvEpoch = 0;
        StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(pluginURL, configService);
        try (BasicContext context = new BasicContext()) {
            assert plugin != null;
            List<Callable<EventStream>> callables =
                    plugin.getDataForPV(context, pvName, startTime, endTime, new DefaultRawPostProcessor());
            for (Callable<EventStream> callable : callables) {
                EventStream ev = callable.call();
                logger.debug("Event Stream " + ev.getDescription());
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
        Instant lastMonth = TimeUtils.minusDays(TimeUtils.now(), 2 * 31);

        tCount = generateData(lastMonth);

        changeMTSForDest();
        Instant timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 365 * 10);
        logger.info("Running ETL now as if it is " + TimeUtils.convertToHumanReadableString(timeETLruns));
        ETLExecutor.runETLs(configService, timeETLruns);

        logger.info("Checking merged data after running ETL");
        long lCount = testMergedRetrieval(
                PB_PLUGIN_IDENTIFIER + "://localhost?name=LTS&rootFolder=" + testLTSFolder
                        + "&partitionGranularity=PARTITION_YEAR",
                TimeUtils.minusDays(TimeUtils.now(), 365 * 2),
                TimeUtils.plusDays(TimeUtils.now(), 365 * 2));
        Assertions.assertEquals(0, lCount, "We expected LTS to have failed " + lCount);
        long mCount = testMergedRetrieval(
                PB_PLUGIN_IDENTIFIER + "://localhost?name=MTS&rootFolder=" + testMTSFolder
                        + "&partitionGranularity=PARTITION_DAY",
                TimeUtils.minusDays(TimeUtils.now(), 365 * 2),
                TimeUtils.plusDays(TimeUtils.now(), 365 * 2));
        Assertions.assertEquals(
                mCount,
                tCount,
                "We expected MTS to have the same amount of data " + tCount + " instead we got " + mCount);
    }
}
