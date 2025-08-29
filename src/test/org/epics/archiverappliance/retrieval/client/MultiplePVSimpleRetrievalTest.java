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
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.TomcatSetup;
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
 * Test retrieval for multiple PVs
 * @author mshankar
 *
 */
@Tag("integration")
public class MultiplePVSimpleRetrievalTest {
    private static final Logger logger = LogManager.getLogger(MultiplePVSimpleRetrievalTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    private static long previousEpochSeconds = 0;
    private static final int TOTAL_NUMBER_OF_PVS = 10;
    private static final String[] pvs = new String[TOTAL_NUMBER_OF_PVS];

    @BeforeEach
    public void setUp() throws Exception {
        int phasediff = 360 / TOTAL_NUMBER_OF_PVS;
        for (int i = 0; i < TOTAL_NUMBER_OF_PVS; i++) {
            pvs[i] = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "Sine" + i;
            GenerateData.generateSineForPV(pvs[i], i * phasediff, ArchDBRTypes.DBR_SCALAR_DOUBLE, PlainStorageType.PB);
        }
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    @Test
    public void testGetDataForMultiplePVs() {
        RawDataRetrievalAsEventStream rawDataRetrieval =
                new RawDataRetrievalAsEventStream(ConfigServiceForTests.RAW_RETRIEVAL_URL);
        Instant start = TimeUtils.convertFromISO8601String("2011-02-01T08:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String("2011-02-02T08:00:00.000Z");
        EventStream stream = null;
        try {
            stream = rawDataRetrieval.getDataForPVS(pvs, start, end, new RetrievalEventProcessor() {
                @Override
                public void newPVOnStream(EventStreamDesc desc) {
                    logger.info("On the client side, switching to processing PV " + desc.getPvName());
                    previousEpochSeconds = 0;
                }
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
