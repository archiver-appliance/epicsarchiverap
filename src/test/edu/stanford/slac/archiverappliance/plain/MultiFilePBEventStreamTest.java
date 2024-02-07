/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.plain.pb.PBPlainFileHandler;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.time.Instant;
import java.time.ZonedDateTime;

/**
 * Test EventStreams that span multiple PB files.
 *
 * @author mshankar
 */
public class MultiFilePBEventStreamTest {
    private static final Logger logger = LogManager.getLogger(MultiFilePBEventStreamTest.class);
    static String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "MultiFilePBEventStream/";
    static File rootFolder = new File(rootFolderName);
    static ConfigService configService;
    long ratio = 10;

    @BeforeAll
    public static void setUp() throws Exception {
        if (rootFolder.exists()) {
            FileUtils.deleteDirectory(rootFolder);
        }
        rootFolder.mkdirs();
        configService = new ConfigServiceForTests(-1);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        FileUtils.deleteDirectory(rootFolder);
    }

    @ParameterizedTest
    @EnumSource(PartitionGranularity.class)
    public void testMultiFileEventStream(PartitionGranularity granularity) throws Exception {
        // We generate a ratio * seconds in granularity worth of data into a PlainStoragePlugin with different
        // granularity.
        // We then retrieve data and make sure that we get what we expect

        logger.debug("Generating sample data for granularity " + granularity);

        String pvName = "MultiYear" + granularity.toString();
        String configURL = PBPlainFileHandler.DEFAULT_PB_HANDLER.pluginIdentifier()
                + "://localhost?name=STS&rootFolder=" + rootFolderName + "&partitionGranularity=" + granularity;
        PlainStoragePlugin pbplugin = new PlainStoragePlugin();
        pbplugin.initialize(configURL, configService);
        ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
        Instant startTime = ZonedDateTime.now()
                .withMonth(9)
                .withDayOfMonth(11)
                .withHour(8)
                .withMinute(12)
                .withSecond(0)
                .withNano(0)
                .toInstant();
        ;
        Instant endTime = startTime.plusSeconds(granularity.getApproxSecondsPerChunk() * ratio);
        int periodSize = granularity.getApproxSecondsPerChunk() / 10;
        try (BasicContext context = new BasicContext()) {
            pbplugin.appendData(
                    context,
                    pvName,
                    new SimulationEventStream(
                            type,
                            (t, secondsIntoYear) -> new ScalarValue<Double>(1.0),
                            startTime,
                            endTime.plusSeconds(10),
                            periodSize));
        }
        logger.info("Done generating sample data for granularity " + granularity);
        Instant expectedTime = startTime;
        try (BasicContext context = new BasicContext();
                EventStream result = new CurrentThreadWorkerEventStream(
                        pvName, pbplugin.getDataForPV(context, pvName, startTime, endTime))) {
            long eventCount = 0;
            for (Event e : result) {
                Instant currTime = e.getEventTimeStamp();
                // The PlainPBStorage plugin will also yield the last event of the previous partition.
                // We skip checking that as part of this test
                if (currTime.isBefore(startTime.minusSeconds(1))) continue;
                Assertions.assertEquals(expectedTime, currTime);
                Assertions.assertTrue(
                        currTime.isBefore(endTime) || currTime.equals(endTime),
                        "Less than " + endTime + " Got " + currTime + " at eventCount " + eventCount);
                expectedTime = expectedTime.plusSeconds(periodSize);
                eventCount++;
            }
        }
    }
}
