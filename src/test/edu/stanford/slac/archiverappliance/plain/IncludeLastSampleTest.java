/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;

class IncludeLastSampleTest {
    static final String testSpecificFolder = "IncludeLastSampleTest";
    static final String pvNamePB = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":" + testSpecificFolder;
    private static final Logger logger = LogManager.getLogger(IncludeLastSampleTest.class.getName());
    private static final LinkedList<Instant> generatedTimeStamps = new LinkedList<Instant>();
    static File dataFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ArchUnitTest"
            + File.separator + testSpecificFolder);
    static PlainCommonSetup PBSetup = new PlainCommonSetup();
    static PlainStoragePlugin pbPlugin = new PlainStoragePlugin();
    private static final short currentYear = TimeUtils.getCurrentYear();

    @BeforeAll
    public static void setUp() throws Exception {
        PBSetup.setUpRootFolder(pbPlugin, testSpecificFolder, PartitionGranularity.PARTITION_DAY);
        logger.info("Data folder is " + dataFolder.getAbsolutePath());
        FileUtils.deleteDirectory(dataFolder);
        generateData();
    }

    private static void generateData() throws IOException {
        ArrayListEventStream strmPB = new ArrayListEventStream(
                0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvNamePB, currentYear));

        YearSecondTimestamp yts = new YearSecondTimestamp(currentYear, 1, 10);
        strmPB.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));

        yts = new YearSecondTimestamp(
                currentYear, 1 + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(), 20);
        strmPB.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));
        generatedTimeStamps.add(TimeUtils.convertFromYearSecondTimestamp(yts));

        yts = new YearSecondTimestamp(
                currentYear, 1 + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 2, 30);
        strmPB.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));
        generatedTimeStamps.add(TimeUtils.convertFromYearSecondTimestamp(yts));

        yts = new YearSecondTimestamp(
                currentYear, 1 + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 3, 40);
        strmPB.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));
        generatedTimeStamps.add(TimeUtils.convertFromYearSecondTimestamp(yts));

        try (BasicContext context = new BasicContext()) {
            pbPlugin.appendData(context, pvNamePB, strmPB);
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {
        FileUtils.deleteDirectory(dataFolder);
    }

    @Test
    void testRetrieval() {
        Instant start = TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(
                currentYear, 20 + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(), 1));
        Instant end = TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(
                currentYear, PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 10, 1));
        int eventCount = 0;
        LinkedList<Instant> actualTimestamps = new LinkedList<>();
        try (EventStream stream = new CurrentThreadWorkerEventStream(
                pvNamePB,
                pbPlugin.getDataForPV(new BasicContext(), pvNamePB, start, end, new DefaultRawPostProcessor()))) {

            for (Event e : stream) {
                actualTimestamps.add(e.getEventTimeStamp());
                eventCount++;
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
        Assertions.assertEquals(generatedTimeStamps, actualTimestamps);
        Assertions.assertEquals(3, eventCount);
    }
}
