/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.GenerateData;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Test the PBFileInfo.
 * @author mshankar
 *
 */
public class PBFileInfoTest {
    PBCommonSetup setup = new PBCommonSetup();
    Path PBfile;
    String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "PVInfo";

    @AfterEach
    public void tearDown() throws Exception {
        Files.deleteIfExists(PBfile);
    }

    @Test
    public void testPBInfo() throws Exception {
        PlainPBStoragePlugin storagePlugin = new PlainPBStoragePlugin();

        setup.setUpRootFolder(storagePlugin);
        PBfile = PlainPBPathNameUtility.getPathNameForTime(
                storagePlugin,
                pvName,
                TimeUtils.getStartOfCurrentYearInSeconds(),
                new ArchPaths(),
                (new ConfigServiceForTests(new File("./bin")).getPVNameToKeyConverter()));
        GenerateData.generateSineForPV(pvName, 0, ArchDBRTypes.DBR_SCALAR_DOUBLE);
        PBFileInfo info = new PBFileInfo(PBfile);
        Assertions.assertEquals(info.getPVName(), pvName, "PVInfo PV name " + info.getPVName());
        Assertions.assertEquals(info.getDataYear(), TimeUtils.getCurrentYear(), "PVInfo year " + info.getDataYear());
        Assertions.assertEquals(info.getType(), ArchDBRTypes.DBR_SCALAR_DOUBLE, "PVInfo type " + info.getType());
        long firstSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
        Assertions.assertEquals(
                info.getFirstEventEpochSeconds(),
                firstSeconds,
                "PVInfo first event time " + info.getFirstEventEpochSeconds() + "/" + firstSeconds);
        long lastSeconds = TimeUtils.getStartOfYearInSeconds(TimeUtils.getCurrentYear() + 1) - 1;
        Assertions.assertEquals(
                info.getLastEventEpochSeconds(),
                lastSeconds,
                "PVInfo last event time " + info.getLastEventEpochSeconds() + "!=" + lastSeconds);
    }
}
