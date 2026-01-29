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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.stream.Stream;

/**
 * Unit test to make sure the client retrieval libraries can retrieval all the DBR types.
 * @author mshankar
 *
 */
@Tag("integration")
public class DBRRetrievalTest {
    private static final Logger logger = LogManager.getLogger(DBRRetrievalTest.class.getName());
    private final short currentYear = TimeUtils.getCurrentYear();
    static TomcatSetup tomcatSetup = new TomcatSetup();

    @BeforeAll
    public static void setUp() throws Exception {

        tomcatSetup.setUpWebApps(DBRRetrievalTest.class.getSimpleName());
    }

    @AfterAll
    public static void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    static Stream<Arguments> provideDataDBRs() {
        return Stream.of(PlainStorageType.values()).flatMap(plainStorageType -> Stream.of(ArchDBRTypes.values())
                .map(type -> Arguments.of(
                        ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX
                                + plainStorageType
                                + (type.isWaveForm() ? "V_" : "S_")
                                + type.getPrimitiveName(),
                        type,
                        plainStorageType)));
    }

    @ParameterizedTest
    @MethodSource("provideDataDBRs")
    public void testGetDataForDBRs(String pvName, ArchDBRTypes type, PlainStorageType plainStorageType)
            throws Exception {

        GenerateData.generateSineForPV(
                pvName,
                0,
                type,
                plainStorageType,
                TimeUtils.convertFromISO8601String(currentYear + "-02-01T08:00:00.000Z"),
                TimeUtils.convertFromISO8601String(currentYear + "-02-02T08:00:00.000Z"));
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant start = TimeUtils.convertFromISO8601String(currentYear + "-02-01T08:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String(currentYear + "-02-02T08:00:00.000Z");

        EventStream stream = null;
        try {
            logger.info("Testing retrieval for DBR " + type.toString());
            stream = rawDataRetrieval.getDataForPVS(
                    new String[] {pvName}, start, end, desc -> logger.info("Getting data for PV " + desc.getPvName()));

            long previousEpochSeconds = 0;

            // Make sure we get the DBR type we expect
            Assertions.assertEquals(stream.getDescription().getArchDBRType(), type);

            // We are making sure that the stream we get back has times in sequential order...
            for (Event e : stream) {
                long actualSeconds = e.getEpochSeconds();
                Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
                previousEpochSeconds = actualSeconds;
            }
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
