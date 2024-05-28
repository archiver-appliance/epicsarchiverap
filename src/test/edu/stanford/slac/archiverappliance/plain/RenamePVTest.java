/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.plain.pb.PBCompressionMode;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Test rename PV for the PlainStoragePlugin...
 * @author mshankar
 *
 */
public class RenamePVTest {
    private static final Logger logger = LogManager.getLogger(RenamePVTest.class);
    private ConfigService configService;
    private File rootFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "RenamePV");

    private String oldPVName = "Test:rename:oldPVName";
    private String newPVName = "Test:rename:newPVName";

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        if (rootFolder.exists()) {
            FileUtils.deleteDirectory(rootFolder);
        }
        rootFolder.mkdirs();
    }

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(rootFolder);
    }

    /**
     * Generate data for some partitions.
     * Rename PV
     * Check the number of data points before and after the rename.
     *
     * @throws Exception
     */
    @Test
    public void testRenamePV() throws Exception {
        PlainStoragePlugin plugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=RenameTest&rootFolder=" + rootFolder + "&partitionGranularity=PARTITION_DAY",
                configService);
        short currentYear = TimeUtils.getCurrentYear();
        ArrayListEventStream strm = new ArrayListEventStream(
                86400, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, oldPVName, currentYear));
        for (int i = 0; i < 365 * 86400; i += 1500) {
            strm.add(new POJOEvent(
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(currentYear, i, 0)),
                    new ScalarValue<Double>((double) i),
                    0,
                    0));
        }
        try (BasicContext context = new BasicContext()) {
            plugin.appendData(context, oldPVName, strm);
        }

        long oldPVEventCount = 0;
        int oldPathCount;
        try (BasicContext context = new BasicContext()) {
            List<Callable<EventStream>> callables = plugin.getDataForPV(
                    context,
                    oldPVName,
                    TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp((short) (currentYear - 1), 0, 0)),
                    TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp((short) (currentYear + 1), 0, 0)));
            try (EventStream data = new CurrentThreadWorkerEventStream(oldPVName, callables)) {
                for (@SuppressWarnings("unused") Event e : data) {
                    oldPVEventCount++;
                }
            }
            oldPathCount = PathNameUtility.getAllPathsForPV(
                            context.getPaths(),
                            plugin.getRootFolder(),
                            oldPVName,
                            PlainStoragePlugin.pbFileExtension,
                            PartitionGranularity.PARTITION_DAY,
                            PBCompressionMode.NONE,
                            configService.getPVNameToKeyConverter())
                    .length;
        }
        logger.info("Done generating data with " + oldPVEventCount + " points   About to rename PV");

        try (BasicContext context = new BasicContext()) {
            plugin.renamePV(context, oldPVName, newPVName);
        }
        logger.info("Done renaming PV");

        long newPVEventCount = 0;
        int newPathCount = 0;
        int newPathForOldPVNameCount = -1;
        try (BasicContext context = new BasicContext()) {
            List<Callable<EventStream>> callables = plugin.getDataForPV(
                    context,
                    newPVName,
                    TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp((short) (currentYear - 1), 0, 0)),
                    TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp((short) (currentYear + 1), 0, 0)));
            try (EventStream data = new CurrentThreadWorkerEventStream(newPVName, callables)) {
                for (@SuppressWarnings("unused") Event e : data) {
                    newPVEventCount++;
                }
            }
            newPathCount = PathNameUtility.getAllPathsForPV(
                            context.getPaths(),
                            plugin.getRootFolder(),
                            newPVName,
                            PlainStoragePlugin.pbFileExtension,
                            PartitionGranularity.PARTITION_DAY,
                            PBCompressionMode.NONE,
                            configService.getPVNameToKeyConverter())
                    .length;
            newPathForOldPVNameCount = PathNameUtility.getAllPathsForPV(
                            context.getPaths(),
                            plugin.getRootFolder(),
                            oldPVName,
                            PlainStoragePlugin.pbFileExtension,
                            PartitionGranularity.PARTITION_DAY,
                            PBCompressionMode.NONE,
                            configService.getPVNameToKeyConverter())
                    .length;
        }

        logger.info("Old count " + oldPVEventCount + " and new count " + newPVEventCount);
        logger.info("Old path count " + oldPathCount + " and new path count " + newPathCount);
        Assertions.assertTrue(
                newPVEventCount == oldPVEventCount,
                "Event counts before and after the move are not the same. Old count " + oldPVEventCount
                        + " and new count " + newPVEventCount);
        Assertions.assertTrue(
                oldPathCount == newPathCount,
                "Path counts before and after the move are not the same. Old count " + oldPathCount + " and new count "
                        + newPathCount);
        Assertions.assertTrue(
                newPathForOldPVNameCount == oldPathCount,
                "Path counts for the old PV name after the rename " + newPathForOldPVNameCount
                        + " is not the same as before the rename " + oldPathCount);
    }
}
