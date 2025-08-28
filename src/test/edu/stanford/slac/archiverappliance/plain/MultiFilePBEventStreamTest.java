/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

import static edu.stanford.slac.archiverappliance.plain.pb.PBPlainFileHandler.PB_PLUGIN_IDENTIFIER;
import static org.epics.archiverappliance.utils.ui.URIUtils.pluginString;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;

/**
 * Test EventStreams that span multiple PB files.
 * @author mshankar
 *
 */
@Tag("slow")
public class MultiFilePBEventStreamTest {
    private static final Logger logger = LogManager.getLogger(MultiFilePBEventStreamTest.class);
    String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "MultiFilePBEventStream/";
    File rootFolder = new File(rootFolderName);

    @BeforeEach
    public void setUp() throws Exception {
        if (rootFolder.exists()) {
            FileUtils.deleteDirectory(rootFolder);
        }
        rootFolder.mkdirs();
    }

    @AfterEach
    public void tearDown() throws Exception {
        // FileUtils.deleteDirectory(rootFolder);
    }

    @Test
    public void testMultiFileEventStream() throws Exception {
        // We generate a years worth of data into a PlainStoragePlugin with different granularity.
        // We then retrieve data and make sure that we get what we expect
        ConfigService configService = new ConfigServiceForTests(-1);

        for (PartitionGranularity granularity : PartitionGranularity.values()) {
            logger.debug("Generating sample data for granularity " + granularity);

            String pvName = "MultiYear" + granularity.toString();
            String configURL = pluginString(
                    PB_PLUGIN_IDENTIFIER,
                    "localhost",
                    "name=STS&rootFolder=" + rootFolderName + "&partitionGranularity=" + granularity);
            PlainStoragePlugin pbplugin = new PlainStoragePlugin(PlainStorageType.PB);
            pbplugin.initialize(configURL, configService);
            short currentYear = TimeUtils.getCurrentYear();
            ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
            try (BasicContext context = new BasicContext()) {
                for (int day = 0; day < 365; day++) {
                    ArrayListEventStream testData = new ArrayListEventStream(
                            24 * 60 * 60, new RemotableEventStreamDesc(type, pvName, currentYear));
                    int startofdayinseconds = day * 24 * 60 * 60;
                    for (int secondintoday = 0; secondintoday < 24 * 60 * 60; secondintoday++) {
                        testData.add(new SimulationEvent(
                                startofdayinseconds + secondintoday, currentYear, type, new ScalarValue<Double>((double)
                                        secondintoday)));
                    }
                    pbplugin.appendData(context, pvName, testData);
                }
            }
            logger.info("Done generating sample data for granularity " + granularity);
            Instant startTime = TimeUtils.convertFromISO8601String(currentYear + "-09-11T08:12:48.000Z");
            Instant endTime = TimeUtils.convertFromISO8601String(currentYear + "-10-04T22:53:31.000Z");
            long startEpochSeconds = TimeUtils.convertToEpochSeconds(startTime);
            long endEpochSeconds = TimeUtils.convertToEpochSeconds(endTime);
            long expectedEpochSeconds = startEpochSeconds;
            logger.info(
                    "Asking for data between {} and {}",
                    TimeUtils.convertToISO8601String(startTime),
                    TimeUtils.convertToISO8601String(endTime));
            try (BasicContext context = new BasicContext();
                    EventStream result = new CurrentThreadWorkerEventStream(
                            pvName, pbplugin.getDataForPV(context, pvName, startTime, endTime))) {
                long eventCount = 0;
                for (Event e : result) {
                    long currEpochSeconds = e.getEpochSeconds();
                    // The PlainStorage plugin will also yield the last event of the previous partition.
                    // We skip checking that as part of this test
                    if (currEpochSeconds < (startEpochSeconds - 1)) continue;
                    Assertions.assertEquals(
                            currEpochSeconds,
                            expectedEpochSeconds,
                            "Expected "
                                    + TimeUtils.convertToISO8601String(
                                            TimeUtils.convertFromEpochSeconds(expectedEpochSeconds, 0))
                                    + " Got "
                                    + TimeUtils.convertToISO8601String(
                                            TimeUtils.convertFromEpochSeconds(currEpochSeconds, 0))
                                    + " at eventCount "
                                    + eventCount);
                    Assertions.assertTrue(
                            currEpochSeconds <= endEpochSeconds,
                            "Less than "
                                    + TimeUtils.convertToISO8601String(
                                            TimeUtils.convertFromEpochSeconds(endEpochSeconds, 0))
                                    + " Got "
                                    + TimeUtils.convertToISO8601String(
                                            TimeUtils.convertFromEpochSeconds(currEpochSeconds, 0))
                                    + " at eventCount "
                                    + eventCount);
                    expectedEpochSeconds++;
                    eventCount++;
                }
            }
        }
    }
}
