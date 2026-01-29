/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

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
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;

/**
 * Test retrieval for two PVs
 * @author mshankar
 *
 */
@Tag("integration")
public class TwoPVRetrievalTest {
    private static final Logger logger = LogManager.getLogger(TwoPVRetrievalTest.class.getName());
    static long previousEpochSeconds = 0;
    static TomcatSetup tomcatSetup = new TomcatSetup();

    @BeforeAll
    public static void setUp() throws Exception {
        tomcatSetup.setUpWebApps(TwoPVRetrievalTest.class.getSimpleName());
    }

    @AfterAll
    public static void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void testGetDataForTwoPVs(PlainStorageType plainStorageType) throws Exception {
        GenerateData.generateSineForPV(
                ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "Sine1",
                0,
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                plainStorageType);
        GenerateData.generateSineForPV(
                ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "Sine2",
                45,
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                plainStorageType);
        RawDataRetrievalAsEventStream rawDataRetrieval =
                new RawDataRetrievalAsEventStream(ConfigServiceForTests.RAW_RETRIEVAL_URL);
        Instant start = TimeUtils.convertFromISO8601String("2011-02-01T08:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String("2011-02-02T08:00:00.000Z");
        EventStream stream = null;
        try {
            stream = rawDataRetrieval.getDataForPVS(
                    new String[] {
                        ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "Sine1",
                        ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "Sine2"
                    },
                    start,
                    end,
                    desc -> {
                        logger.info("On the client side, switching to processing PV " + desc.getPvName());
                        previousEpochSeconds = 0;
                    });

            // We are making sure that the stream we get back has times in sequential order...
            if (stream != null) {
                for (Event e : stream) {
                    long actualSeconds = e.getEpochSeconds();
                    Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
                    previousEpochSeconds = actualSeconds;
                }
            }
        } finally {
            if (stream != null)
                try {
                    stream.close();
                    stream = null;
                } catch (Throwable ignored) {
                }
        }
    }
}
