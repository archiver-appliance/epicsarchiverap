/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.plain.FileExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.GenerateData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;

/**
 * Test retrieval for single PVs
 * @author mshankar
 *
 */
@Tag("integration")
public class SinglePVRetrievalTest {
    private static final Logger logger = LogManager.getLogger(SinglePVRetrievalTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();

    @BeforeEach
    public void setUp() throws Exception {
        GenerateData.generateSineForPV(
                ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "Sine1",
                0,
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                FileExtension.PB);
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    @Test
    public void testGetDataForSinglePV() throws Exception {
        testGetOneDaysDataForYear(
                TimeUtils.getCurrentYear(), PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk());
        testGetOneDaysDataForYear(TimeUtils.getCurrentYear() - 1, 0);
        testGetOneDaysDataForYear(TimeUtils.getCurrentYear() + 1, 1);
    }

    private void testGetOneDaysDataForYear(int year, int expectedCount) throws Exception {
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant start = TimeUtils.convertFromISO8601String(year + "-02-01T08:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String(year + "-02-02T08:00:00.000Z");

        EventStream stream = null;
        try {
            stream = rawDataRetrieval.getDataForPVS(
                    new String[] {ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "Sine1"},
                    start,
                    end,
                    desc -> logger.info("Getting data for PV " + desc.getPvName()));

            long previousEpochSeconds = 0;
            int eventCount = 0;

            // We are making sure that the stream we get back has times in sequential order...
            if (stream != null) {
                for (Event e : stream) {
                    long actualSeconds = e.getEpochSeconds();
                    Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
                    previousEpochSeconds = actualSeconds;
                    eventCount++;
                }
            }

            Assertions.assertEquals(
                    eventCount,
                    expectedCount,
                    "Event count is not what we expect. We got " + eventCount + " and we expected " + expectedCount
                            + " for year " + year);
        } finally {
            if (stream != null)
                try {
                    stream.close();
                    stream = null;
                } catch (Throwable t) {
                }
        }
    }
}
