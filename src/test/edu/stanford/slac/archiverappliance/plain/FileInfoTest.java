/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.plain.pb.PBFileInfo;
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
 * Test the FileInfo.
 * @author mshankar
 *
 */
class FileInfoTest {
    PlainCommonSetup setup = new PlainCommonSetup();
    Path plainFile;
    String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "PVInfo";

    @AfterEach
    public void tearDown() throws Exception {
        Files.deleteIfExists(plainFile);
    }

    @Test
    void testPBInfo() throws Exception {
        PlainStoragePlugin storagePlugin = new PlainStoragePlugin();
        short currentYear = TimeUtils.getCurrentYear();
        setup.setUpRootFolder(storagePlugin);
        plainFile = PathNameUtility.getPathNameForTime(
                storagePlugin,
                pvName,
                TimeUtils.getStartOfYear(currentYear),
                new ArchPaths(),
                (new ConfigServiceForTests(-1).getPVNameToKeyConverter()));
        Instant start = TimeUtils.getStartOfYear(currentYear);
        Instant end = start.plusSeconds(10000);
        GenerateData.generateSineForPV(pvName, 0, ArchDBRTypes.DBR_SCALAR_DOUBLE, start, end);
        FileInfo info = new PBFileInfo(plainFile);
        Assertions.assertEquals(info.getPVName(), pvName, "PVInfo PV name " + info.getPVName());
        Assertions.assertEquals(info.getDataYear(), currentYear, "PVInfo year " + info.getDataYear());
        Assertions.assertEquals(ArchDBRTypes.DBR_SCALAR_DOUBLE, info.getType(), "PVInfo type " + info.getType());
        Assertions.assertEquals(start, info.getFirstEvent().getEventTimeStamp());
        Assertions.assertEquals(end.minusSeconds(1), info.getLastEvent().getEventTimeStamp());
    }
}
