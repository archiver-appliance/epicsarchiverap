/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import edu.stanford.slac.archiverappliance.PlainPB.utils.ValidatePBFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.sql.Timestamp;

/**
 * Very basic ETL tests.
 * @author mshankar
 *
 */
@Tag("slow")
public class SimpleETLTest {
    private static final Logger logger = LogManager.getLogger(SimpleETLTest.class);

    /**
     * Generates some data in STS; then calls the ETL to move it to MTS and checks that the total amount of data before and after is the same.
     */
    @Test
    public void testMove() throws Exception {
        for (PartitionGranularity granularity : PartitionGranularity.values()) {
            PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin();
            PBCommonSetup srcSetup = new PBCommonSetup();
            PlainPBStoragePlugin etlDest = new PlainPBStoragePlugin();
            PBCommonSetup destSetup = new PBCommonSetup();
            ConfigServiceForTests configService = new ConfigServiceForTests(new File("./bin"), 1);

            if (granularity.getNextLargerGranularity() == null) continue;
            srcSetup.setUpRootFolder(etlSrc, "SimpleETLTestSrc_" + granularity, granularity);
            destSetup.setUpRootFolder(
                    etlDest, "SimpleETLTestDest" + granularity, granularity.getNextLargerGranularity());

            logger.info("Testing simple ETL testMove for " + etlSrc.getPartitionGranularity() + " to "
                    + etlDest.getPartitionGranularity());

            String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testMove"
                    + etlSrc.getPartitionGranularity();
            SimulationEventStream simstream =
                    new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0));
            try (BasicContext context = new BasicContext()) {
                etlSrc.appendData(context, pvName, simstream);
            }
            logger.info("Done creating src data for PV " + pvName);

            long beforeCount = 0;
            try (BasicContext context = new BasicContext();
                    EventStream before = new CurrentThreadWorkerEventStream(
                            pvName,
                            etlSrc.getDataForPV(
                                    context,
                                    pvName,
                                    TimeUtils.minusDays(TimeUtils.now(), 366),
                                    TimeUtils.plusDays(TimeUtils.now(), 366)))) {
                for (@SuppressWarnings("unused") Event e : before) {
                    beforeCount++;
                }
            }

            PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
            String[] dataStores = new String[] {etlSrc.getURLRepresentation(), etlDest.getURLRepresentation()};
            typeInfo.setDataStores(dataStores);
            configService.updateTypeInfoForPV(pvName, typeInfo);
            configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
            configService.getETLLookup().manualControlForUnitTests();

            Timestamp timeETLruns = TimeUtils.now();
            DateTime ts = new DateTime(DateTimeZone.UTC);
            if (ts.getMonthOfYear() == 1) {
                // This means that we never test this in Jan but I'd rather have the null check than skip this.
                timeETLruns = TimeUtils.plusDays(timeETLruns, 35);
            }
            ETLExecutor.runETLs(configService, timeETLruns);
            logger.info("Done performing ETL");

            Timestamp startOfRequest = TimeUtils.minusDays(TimeUtils.now(), 366);
            Timestamp endOfRequest = TimeUtils.plusDays(TimeUtils.now(), 366);

            // Check that all the files in the destination store are valid files.
            Path[] allPaths = PlainPBPathNameUtility.getAllPathsForPV(
                    new ArchPaths(),
                    etlDest.getRootFolder(),
                    pvName,
                    ".pb",
                    etlDest.getPartitionGranularity(),
                    CompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
            Assertions.assertNotNull(
                    allPaths, "PlainPBFileNameUtility returns null for getAllFilesForPV for " + pvName);
            Assertions.assertTrue(
                    allPaths.length > 0,
                    "PlainPBFileNameUtility returns empty array for getAllFilesForPV for " + pvName
                            + " when looking in " + etlDest.getRootFolder());

            for (Path destPath : allPaths) {
                Assertions.assertTrue(
                        ValidatePBFile.validatePBFile(destPath, false),
                        "File validation failed for "
                                + destPath.toAbsolutePath());
            }

            logger.info("Asking for data between"
                    + TimeUtils.convertToHumanReadableString(startOfRequest)
                    + " and "
                    + TimeUtils.convertToHumanReadableString(endOfRequest));

            long afterCount = 0;
            try (BasicContext context = new BasicContext();
                    EventStream afterDest = new CurrentThreadWorkerEventStream(
                            pvName, etlDest.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
                Assertions.assertNotNull(afterDest);
                for (@SuppressWarnings("unused") Event e : afterDest) {
                    afterCount++;
                }
            }
            logger.info("Of the " + beforeCount + " events, " + afterCount + " events were moved into the dest store.");
            Assertions.assertTrue((afterCount != 0), "Seems like no events were moved by ETL " + afterCount);
            try (BasicContext context = new BasicContext();
                    EventStream afterSrc = new CurrentThreadWorkerEventStream(
                            pvName, etlSrc.getDataForPV(context, pvName, startOfRequest, endOfRequest))) {
                for (@SuppressWarnings("unused") Event e : afterSrc) {
                    afterCount++;
                }
            }

            Assertions.assertEquals(
                    beforeCount,
                    afterCount,
                    "Before count " + beforeCount + " and after count " + afterCount + " differ");

            srcSetup.deleteTestFolder();
            destSetup.deleteTestFolder();
            configService.shutdownNow();
        }
    }
}
