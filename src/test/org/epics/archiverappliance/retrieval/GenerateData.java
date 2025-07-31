/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

import edu.stanford.slac.archiverappliance.plain.PlainCommonSetup;
import edu.stanford.slac.archiverappliance.plain.PathNameUtility;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;

/**
 * Used to generate data for unit tests.
 * @author mshankar
 *
 */
public class GenerateData {
    private static final Logger logger = LogManager.getLogger(GenerateData.class.getName());

    static ConfigService configService;

    static {
        try {
            configService = new ConfigServiceForTests(new File("./bin"), 1);
        } catch (ConfigException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * We generate a sine wave for the data if it does not already exist.
     * @throws IOException
     */
    public static long generateSineForPV(
            String pvName, int phasediffindegrees, ArchDBRTypes type, Instant start, Instant end) throws Exception {
        PlainStoragePlugin storagePlugin = new PlainStoragePlugin();
        PlainCommonSetup setup = new PlainCommonSetup();
        setup.setUpRootFolder(storagePlugin);
        long numberOfEvents = 0;
        try (BasicContext context = new BasicContext()) {
            if (!Files.exists(PathNameUtility.getPathNameForTime(
                    storagePlugin, pvName, start, context.getPaths(), configService.getPVNameToKeyConverter()))) {
                SimulationEventStream simstream =
                        new SimulationEventStream(type, new SineGenerator(phasediffindegrees), start, end, 1);
                numberOfEvents = simstream.getNumberOfEvents();
                storagePlugin.appendData(context, pvName, simstream);
            }
        }
        return numberOfEvents;
    }
    /**
     * We generate a sine wave for the data if it does not already exist.
     * @throws IOException
     */
    public static long generateSineForPV(String pvName, int phasediffindegrees, ArchDBRTypes type) throws Exception {

        return generateSineForPV(
                pvName,
                phasediffindegrees,
                type,
                TimeUtils.getStartOfYear(TimeUtils.getCurrentYear()),
                TimeUtils.getEndOfYear(TimeUtils.getCurrentYear()));
    }

    /**
     * Given a plugin URL, the main method generates a couple of years worth of sine data into the plugin until
     * 'yesterday'
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java org.epics.archiverappliance.retrieval.GenerateData <pvName> <pluginURL>");
            return;
        }

        String pvName = args[0];
        String pluginURL = args[1];

        ConfigService configService = new ConfigServiceForTests(new File("./bin"), 1);
        StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(pluginURL, configService);

        Instant end = TimeUtils.minusDays(TimeUtils.now(), 1);
        Instant start = TimeUtils.minusDays(end, 365 * 2);
        long startEpochSeconds = TimeUtils.convertToEpochSeconds(start);
        long endEpochSeconds = TimeUtils.convertToEpochSeconds(end);

        logger.info("Generating data for pv " + pvName + " using plugin " + plugin.getDescription() + " between "
                + TimeUtils.convertToHumanReadableString(start) + " and "
                + TimeUtils.convertToHumanReadableString(end));

        long currentSeconds = startEpochSeconds;
        while (currentSeconds < endEpochSeconds) {
            int eventsPerShot = PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
            ArrayListEventStream instream = new ArrayListEventStream(
                    eventsPerShot,
                    new RemotableEventStreamDesc(
                            ArchDBRTypes.DBR_SCALAR_DOUBLE,
                            pvName,
                            TimeUtils.computeYearForEpochSeconds(currentSeconds)));
            for (int i = 0; i < eventsPerShot; i++) {
                YearSecondTimestamp yts = TimeUtils.convertToYearSecondTimestamp(currentSeconds);
                instream.add(new SimulationEvent(
                        yts.getSecondsintoyear(),
                        yts.getYear(),
                        ArchDBRTypes.DBR_SCALAR_DOUBLE,
                        new ScalarValue<Double>(Math.sin(yts.getSecondsintoyear()))));
                currentSeconds++;
            }
            try (BasicContext context = new BasicContext()) {
                plugin.appendData(context, pvName, instream);
            }
        }
        logger.info("Done generating data for pv " + pvName + " using plugin " + plugin.getDescription() + " between "
                + TimeUtils.convertToHumanReadableString(start) + " and "
                + TimeUtils.convertToHumanReadableString(end));

        System.exit(0);
    }
}
