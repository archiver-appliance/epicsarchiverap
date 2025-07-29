/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.plain.PBFileInfo;
import edu.stanford.slac.archiverappliance.plain.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.plain.PlainPBStoragePlugin;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.GenerateData;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

/**
 * Test the PBFileInfo.
 * @author mshankar
 *
 */
public class PBFileInfoTest {
    PBCommonSetup setup = new PBCommonSetup();
    Path pBfile;
    String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "PVInfo";

    @AfterEach
    public void tearDown() throws Exception {
        Files.deleteIfExists(pBfile);
    }

    @Test
    public void testPBInfo() throws Exception {
        PlainPBStoragePlugin storagePlugin = new PlainPBStoragePlugin();
        short currentYear = TimeUtils.getCurrentYear();
        setup.setUpRootFolder(storagePlugin);
        pBfile = PlainPBPathNameUtility.getPathNameForTime(
                storagePlugin,
                pvName,
                TimeUtils.getStartOfYear(currentYear),
                new ArchPaths(),
                (new ConfigServiceForTests(-1).getPVNameToKeyConverter()));
        Instant start = TimeUtils.getStartOfYear(currentYear);
        Instant end = start.plusSeconds(10000);
        GenerateData.generateSineForPV(pvName, 0, ArchDBRTypes.DBR_SCALAR_DOUBLE, start, end);
        PBFileInfo info = new PBFileInfo(pBfile);
        Assertions.assertEquals(info.getPVName(), pvName, "PVInfo PV name " + info.getPVName());
        Assertions.assertEquals(info.getDataYear(), currentYear, "PVInfo year " + info.getDataYear());
        Assertions.assertEquals(info.getType(), ArchDBRTypes.DBR_SCALAR_DOUBLE, "PVInfo type " + info.getType());
        Assertions.assertEquals(start, info.getFirstEvent().getEventTimeStamp());
        Assertions.assertEquals(end.minusSeconds(1), info.getLastEvent().getEventTimeStamp());
    }
}
