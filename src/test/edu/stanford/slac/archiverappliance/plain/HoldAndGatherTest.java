/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.PB.data.PlainCommonSetup;
import edu.stanford.slac.archiverappliance.plain.pb.PBPlainFileHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.etl.ETLInfo;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.blackhole.BlackholeStoragePlugin;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

/**
 * Test the ETL hold and gather logic
 * @author mshankar
 *
 */
public class HoldAndGatherTest {
    private static final Logger logger = LogManager.getLogger(HoldAndGatherTest.class.getName());

    static ConfigServiceForTests configService;

    static BlackholeStoragePlugin etlDest;
    long ratio = 10; // Must be larger than the biggest hold + 1

    @BeforeAll
    static void setUp() throws ConfigException {
        configService = new ConfigServiceForTests(-1);
        configService.getETLLookup().manualControlForUnitTests();

        etlDest = new BlackholeStoragePlugin();
    }

    @AfterAll
    static void tearDown() {
        configService.shutdownNow();
    }

    public static Stream<Arguments> provideHoldAndGather() {
        return Stream.of(
                Arguments.of(PartitionGranularity.PARTITION_5MIN, 7, 5),
                Arguments.of(PartitionGranularity.PARTITION_15MIN, 7, 5),
                Arguments.of(PartitionGranularity.PARTITION_30MIN, 7, 5),
                Arguments.of(PartitionGranularity.PARTITION_HOUR, 7, 5),
                Arguments.of(PartitionGranularity.PARTITION_DAY, 7, 5),
                Arguments.of(PartitionGranularity.PARTITION_DAY, 5, 3),
                Arguments.of(PartitionGranularity.PARTITION_MONTH, 2, 1));
    }

    /**
     * We generate data in chunks for a year.
     * At each chunk we predict how many ETL streams we should get.
     */
    @ParameterizedTest
    @MethodSource("provideHoldAndGather")
    void testHoldAndGather(PartitionGranularity granularity, int hold, int gather) throws Exception {

        PlainStoragePlugin etlSrc = new PlainStoragePlugin();
        PlainCommonSetup srcSetup = new PlainCommonSetup();
        srcSetup.setUpRootFolder(etlSrc, "ETLHoldGatherTest_" + granularity, granularity);

        etlSrc.setHoldETLForPartitions(hold);
        etlSrc.setGatherETLinPartitions(gather);

        logger.info("Testing ETL hold gather for " + etlSrc.getPartitionGranularity());

        Instant currTime = TimeUtils.getStartOfYear(TimeUtils.getCurrentYear());
        Instant endTime = currTime.plusSeconds(ratio * granularity.getApproxSecondsPerChunk());

        long incrementSeconds = granularity.getApproxSecondsPerChunk() / ratio;

        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX
                + PBPlainFileHandler.DEFAULT_PB_HANDLER.pluginIdentifier() + hold + "_" + gather + "_ETL_hold_gather"
                + etlSrc.getPartitionGranularity();
        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores = new String[] {etlSrc.getURLRepresentation(), etlDest.getURLRepresentation()};
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());

        while (currTime.isBefore(endTime)) {
            long eventsPerShot = granularity.getApproxSecondsPerChunk() / incrementSeconds;
            short year = TimeUtils.getYear(currTime);
            ArrayListEventStream instream = new ArrayListEventStream(
                    (int) eventsPerShot, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
            for (int i = 0; i < eventsPerShot; i++) {
                int secondsIntoYear = TimeUtils.getSecondsIntoYear(currTime);
                instream.add(new SimulationEvent(
                        secondsIntoYear, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>((double)
                                secondsIntoYear)));
                currTime = currTime.plusSeconds(incrementSeconds);
            }
            try (BasicContext context = new BasicContext()) {
                etlSrc.appendData(context, pvName, instream);
            }

            List<ETLInfo> etlStreams = etlSrc.getETLStreams(pvName, currTime, new ETLContext());
            Assertions.assertTrue(
                    (etlStreams == null) || (etlStreams.isEmpty()) || (etlStreams.size() == (gather)),
                    "At " + currTime + " we have "
                            + ((etlStreams == null) ? "null" : Integer.toString(etlStreams.size())) + " for "
                            + granularity + " hold = " + hold + " gather = " + gather);
            ETLExecutor.runETLs(configService, currTime);
        }

        srcSetup.deleteTestFolder();
    }
}
