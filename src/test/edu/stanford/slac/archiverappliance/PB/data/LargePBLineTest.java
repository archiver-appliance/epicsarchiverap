/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import edu.stanford.slac.archiverappliance.PlainPB.utils.ValidatePBFile;
import gov.aps.jca.dbr.DBR_TIME_Double;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;

/**
 * Test storage and retrieval of events whose serialized sizes are large.
 * @author mshankar
 *
 */
public class LargePBLineTest {
    private ConfigService configService;
    PBCommonSetup largeLineSetup = new PBCommonSetup();
    private static final Logger logger = LogManager.getLogger(LargePBLineTest.class.getName());

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
    }

    @AfterEach
    public void tearDown() throws Exception {
        largeLineSetup.deleteTestFolder();
    }
    @Test
    public void testLargeLines() throws Exception {
        PlainPBStoragePlugin storagePlugin = new PlainPBStoragePlugin();
        largeLineSetup.setUpRootFolder(storagePlugin, "largeLineTest", PartitionGranularity.PARTITION_HOUR);

        // We create vector doubles with a large number of elements; write it out and then test the read.
        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "LargeLineTest"
                + storagePlugin.getPartitionGranularity();
        ArchDBRTypes type = ArchDBRTypes.DBR_WAVEFORM_DOUBLE;
        short year = TimeUtils.getCurrentYear();
        ArrayListEventStream strm = new ArrayListEventStream(1024, new RemotableEventStreamDesc(type, pvName, year));
        for (int i = 1; i < 7200; i++) {
            DBR_TIME_Double retvd = new DBR_TIME_Double(ArrayUtils.toPrimitive(
                    Collections.nCopies(i, Math.sin(i * Math.PI / 3600)).toArray(new Double[0])));
            retvd.setTimeStamp(new gov.aps.jca.dbr.TimeStamp(TimeUtils.getStartOfCurrentYearInSeconds() + i));
            retvd.setSeverity(1);
            retvd.setStatus(0);
            strm.add(new PBVectorDouble(retvd));
        }
        try (BasicContext context = new BasicContext()) {
            storagePlugin.appendData(context, pvName, strm);
        } catch (Exception ex) {
            logger.error("Exception appending data " +strm, ex);
            Assertions.fail(ex.getMessage());
        }

        Path[] allPaths = PlainPBPathNameUtility.getAllPathsForPV(
                new ArchPaths(),
                storagePlugin.getRootFolder(),
                pvName,
                PlainPBStoragePlugin.pbFileExtension,
                storagePlugin.getPartitionGranularity(),
                CompressionMode.NONE,
                configService.getPVNameToKeyConverter());
        Assertions.assertNotNull(allPaths, "testLargeLines returns null for getAllFilesForPV for " + pvName);
        Assertions.assertTrue(
                allPaths.length > 0, "testLargeLines returns empty array for getAllFilesForPV for " + pvName);

        for (Path destPath : allPaths) {
            try {
                new PBFileInfo(destPath);
                Assertions.assertTrue(
                        ValidatePBFile.validatePBFile(destPath, false),
                        "File validation failed for " + destPath.toAbsolutePath());
            } catch (Exception ex) {
                logger.error("Exception parsing file" + destPath.toAbsolutePath(), ex);
                Assertions.fail(ex.getMessage());
            }
        }
    }
}
