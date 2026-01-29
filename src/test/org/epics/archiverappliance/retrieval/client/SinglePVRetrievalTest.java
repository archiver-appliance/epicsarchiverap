/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import static org.epics.archiverappliance.retrieval.TypeInfoUtil.updatePVStorageType;

import edu.stanford.slac.archiverappliance.plain.PlainStorageType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.GenerateData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Test retrieval for single PVs
 * @author mshankar
 *
 */
@Tag("integration")
class SinglePVRetrievalTest {
    private static final Logger logger = LogManager.getLogger(SinglePVRetrievalTest.class.getName());
    static TomcatSetup tomcatSetup = new TomcatSetup();

    static final int currentYear = TimeUtils.getCurrentYear();
    static final String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "Sine1";

    @BeforeAll
    static void setUp() throws Exception {
        GenerateData.generateSineForPV(pvName, 0, ArchDBRTypes.DBR_SCALAR_DOUBLE, PlainStorageType.PB);
        GenerateData.generateSineForPV(pvName, 0, ArchDBRTypes.DBR_SCALAR_DOUBLE, PlainStorageType.PARQUET);
        tomcatSetup.setUpWebApps(SinglePVRetrievalTest.class.getSimpleName());
    }

    @AfterAll
    static void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    static Stream<Arguments> provideInputs() {
        return Arrays.stream(PlainStorageType.values())
                .flatMap(p -> Stream.of(
                        Arguments.of(currentYear, 86401, p),
                        Arguments.of(currentYear - 1, 0, p),
                        Arguments.of(currentYear + 1, 1, p)));
    }

    @ParameterizedTest
    @MethodSource("provideInputs")
    void testGetOneDaysDataForYear(int year, int expectedCount, PlainStorageType plainStorageType) throws Exception {
        if (plainStorageType != PlainStorageType.PB) {
            updatePVStorageType(pvName, plainStorageType);
        }
        RawDataRetrievalAsEventStream rawDataRetrieval =
                new RawDataRetrievalAsEventStream(ConfigServiceForTests.RAW_RETRIEVAL_URL);
        Instant start = TimeUtils.convertFromISO8601String(year + "-02-01T08:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String(year + "-02-02T08:00:00.000Z");

        EventStream stream = null;
        try {
            stream = rawDataRetrieval.getDataForPVS(
                    new String[] {pvName}, start, end, desc -> logger.info("Getting data for PV " + desc.getPvName()));

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
                } catch (Throwable ignored) {
                }
        }
    }
}
