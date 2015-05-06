/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import edu.stanford.slac.archiverappliance.PlainPB.utils.ValidatePBFile;
import gov.aps.jca.dbr.DBR_TIME_Double;

/**
 * Test storage and retrieval of events whose serialized sizes are large.
 * @author mshankar
 *
 */
public class LargePBLineTest {
	private ConfigService configService;
	PlainPBStoragePlugin largeLineTest = new PlainPBStoragePlugin();
	PBCommonSetup largeLineSetup = new PBCommonSetup();
	private static Logger logger = Logger.getLogger(LargePBLineTest.class.getName());

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		largeLineSetup.setUpRootFolder(largeLineTest, "largeLineTest", PartitionGranularity.PARTITION_HOUR);
	}

	@After
	public void tearDown() throws Exception {
		largeLineSetup.deleteTestFolder();
	}

	@Test
	public void testLargeLines() throws Exception {
		// We create vector doubles with a large number of elements; write it out and then test the read.
		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "LargeLineTest" + largeLineTest.getPartitionGranularity();
		ArchDBRTypes type = ArchDBRTypes.DBR_WAVEFORM_DOUBLE;
		short year = TimeUtils.getCurrentYear();
		for(int i = 1; i < 7200; i++) {
			try(BasicContext context = new BasicContext()) {
				ArrayListEventStream strm = new ArrayListEventStream(1024, new RemotableEventStreamDesc(type, pvName, year));
				DBR_TIME_Double retvd = new DBR_TIME_Double(ArrayUtils.toPrimitive(Collections.nCopies(i, Math.sin(i*Math.PI/3600)).toArray(new Double[0])));
				retvd.setTimeStamp(new gov.aps.jca.dbr.TimeStamp(TimeUtils.getStartOfCurrentYearInSeconds() + i));
				retvd.setSeverity(1);
				retvd.setStatus(0);
				strm.add(new PBVectorDouble(retvd));
				largeLineTest.appendData(context, pvName, strm);
			} catch(Exception ex) {
				logger.error("Exception appending data " + i, ex);
				fail(ex.getMessage());
			}
		}
		
		Path[] allPaths = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), largeLineTest.getRootFolder(), pvName, ".pb", largeLineTest.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("testLargeLines returns null for getAllFilesForPV for " + pvName, allPaths != null);
		assertTrue("testLargeLines returns empty array for getAllFilesForPV for " + pvName, allPaths.length > 0);
		
		for(Path destPath : allPaths) {
			try {
				PBFileInfo info = new PBFileInfo(destPath);
				info.getLastEventEpochSeconds();
				assertTrue("File validation failed for " + destPath.toAbsolutePath().toString(), ValidatePBFile.validatePBFile(destPath, false));
			} catch(Exception ex) {
				logger.error("Exception parsing file" + destPath.toAbsolutePath().toString(), ex);
				fail(ex.getMessage());
			}
		}
		
	}

}
