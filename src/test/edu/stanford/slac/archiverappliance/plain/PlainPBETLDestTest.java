/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.PB.data.PlainCommonSetup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests the various methods needed for ETLDest as implemented by the PlainPBStorage plugin
 * @author mshankar
 *
 */
public class PlainPBETLDestTest {
    private static final Logger logger = LogManager.getLogger(PlainPBETLDestTest.class);
    PlainCommonSetup setup = new PlainCommonSetup();

    private static int getSecondsBetweenEvents(PartitionGranularity partitionGranularity) {
        return switch (partitionGranularity) {
            case PARTITION_5MIN, PARTITION_15MIN, PARTITION_30MIN, PARTITION_HOUR -> 10;
            case PARTITION_DAY -> 10 * 60;
            case PARTITION_MONTH -> 10 * 60 * 24;
            case PARTITION_YEAR -> 10 * 60 * 24 * 30;
        };
    }

    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void testGetLastKnownEvent(PlainStorageType plainStorageType) throws Exception {
        PlainStoragePlugin storagePlugin = new PlainStoragePlugin(plainStorageType);
        setup.setUpRootFolder(storagePlugin, "PVETLDestTests");

        long epochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
        ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
        for (PartitionGranularity partitionGranularity : PartitionGranularity.values()) {
            logger.debug("Testing last known event for " + partitionGranularity);
            setup.setUpRootFolder(storagePlugin, "PVETLDestTests", partitionGranularity);
            int secondsBetweenEvents = getSecondsBetweenEvents(partitionGranularity);
            String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "PVETLDestTest" + partitionGranularity;
            // getLastKnownEvent for a PV with no data should return null
            try (BasicContext context = new BasicContext()) {
                Assertions.assertNull(storagePlugin.getLastKnownEvent(context, pvName));
            }

            // We append one event at a time and make sure that the last event is what we expect.
            YearSecondTimestamp yts = TimeUtils.convertToYearSecondTimestamp(epochSeconds);
            ArrayListEventStream testData =
                    new ArrayListEventStream(1, new RemotableEventStreamDesc(type, pvName, yts.getYear()));
            Event lastEvent =
                    new SimulationEvent(yts.getSecondsintoyear(), yts.getYear(), type, new ScalarValue<Double>((double)
                            yts.getSecondsintoyear()));
            epochSeconds += secondsBetweenEvents;
            testData.add(lastEvent);
            try (BasicContext context = new BasicContext()) {
                storagePlugin.appendData(context, pvName, testData);
            }
            try (BasicContext context = new BasicContext()) {
                Assertions.assertNotNull(storagePlugin.getLastKnownEvent(context, pvName));
                Assertions.assertEquals(
                        storagePlugin.getLastKnownEvent(context, pvName).getEpochSeconds(),
                        lastEvent.getEpochSeconds());
            }
        }
    }
}
