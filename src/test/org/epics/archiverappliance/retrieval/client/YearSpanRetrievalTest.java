/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.HashMap;

/**
 * Test retrieval across year spans.
 * @author mshankar
 *
 */
@Tag("integration")
public class YearSpanRetrievalTest {
    private static final Logger logger = LogManager.getLogger(YearSpanRetrievalTest.class.getName());
    static long previousEpochSeconds = 0;
    PBCommonSetup pbSetup = new PBCommonSetup();
    PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
    TomcatSetup tomcatSetup = new TomcatSetup();
    private ConfigService configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(new File("./bin"));
        pbSetup.setUpRootFolder(pbplugin);
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
        generateDataForYears();
    }

    private void generateDataForYears() throws IOException {
        // We skip generation of the file only if all the files exist.
        boolean deletefilesandgeneratedata = false;
        for (short currentyear = (short) 2010; currentyear <= (short) 2013; currentyear++) {
            if (!PlainPBPathNameUtility.getPathNameForTime(
                            pbplugin,
                            "--ArchUnitTestyspan",
                            TimeUtils.getStartOfYear(currentyear),
                            new ArchPaths(),
                            configService.getPVNameToKeyConverter())
                    .toFile()
                    .exists()) {
                logger.info("File for year " + currentyear + " does not exist. Generating data for all the years.");
                deletefilesandgeneratedata = true;
                break;
            }
        }
        // Delete all the files for the specified span
        if (deletefilesandgeneratedata) {
            for (short currentyear = (short) 2010; currentyear <= (short) 2013; currentyear++) {
                Files.deleteIfExists(PlainPBPathNameUtility.getPathNameForTime(
                        pbplugin,
                        "--ArchUnitTestyspan",
                        TimeUtils.getStartOfYear(currentyear),
                        new ArchPaths(),
                        configService.getPVNameToKeyConverter()));
            }

            SimulationEventStream simstream = new SimulationEventStream(
                    ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0), TimeUtils.getStartOfYear(2010), TimeUtils.getEndOfYear(2013), 1);
            // The pbplugin should handle all the rotation etc.
            try (BasicContext context = new BasicContext()) {
                pbplugin.appendData(context, "--ArchUnitTestyspan", simstream);
            }
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    @Test
    public void testYearSpan() {
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant start = TimeUtils.convertFromISO8601String("2011-12-31T08:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String("2012-01-01T08:00:00.000Z");
        EventStream stream = null;
        try {
            stream = rawDataRetrieval.getDataForPVS(
                    new String[]{ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "yspan"},
                    start,
                    end,
                    new RetrievalEventProcessor() {
                        @Override
                        public void newPVOnStream(EventStreamDesc desc) {
                            logger.info("On the client side, switching to processing PV " + desc.getPvName());
                            previousEpochSeconds = 0;
                        }
                    });

            // We are making sure that the stream we get back has times in sequential order...

            HashMap<Short, YearCount> counts = new HashMap<Short, YearCount>();

            for (Event e : stream) {
                long actualSeconds = e.getEpochSeconds();
                Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
                previousEpochSeconds = actualSeconds;

                YearSecondTimestamp actualYST = TimeUtils.convertToYearSecondTimestamp(actualSeconds);
                YearCount ycount = counts.get(actualYST.getYear());
                if (ycount == null) {
                    ycount = new YearCount();
                    counts.put(actualYST.getYear(), ycount);
                }
                ycount.count++;
            }

            Assertions.assertTrue(counts.get((short) 2011).count > 20000);
            Assertions.assertTrue(counts.get((short) 2012).count > 20000);
        } finally {
            if (stream != null)
                try {
                    stream.close();
                    stream = null;
                } catch (Throwable ignored) {
                }
        }
    }

    static class YearCount {
        int count = 0;
    }
}
