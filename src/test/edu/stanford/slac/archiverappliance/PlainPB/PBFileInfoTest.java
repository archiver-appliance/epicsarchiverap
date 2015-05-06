/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.GenerateData;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;

/**
 * Test the PBFileInfo.
 * @author mshankar
 *
 */
public class PBFileInfoTest {
	PlainPBStoragePlugin storagePlugin = new PlainPBStoragePlugin();
	PBCommonSetup setup = new PBCommonSetup();
	Path PBfile;
	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "PVInfo";
	private ConfigService configService;

	
	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		setup.setUpRootFolder(storagePlugin);
		PBfile = PlainPBPathNameUtility.getPathNameForTime(storagePlugin, pvName, TimeUtils.getStartOfCurrentYearInSeconds(), new ArchPaths(), configService.getPVNameToKeyConverter());
		GenerateData.generateSineForPV(pvName, 0, ArchDBRTypes.DBR_SCALAR_DOUBLE);
	}

	@After
	public void tearDown() throws Exception {
		Files.deleteIfExists(PBfile);
	}

	@Test
	public void testPBInfo() throws Exception {
		
		PBFileInfo info = new PBFileInfo(PBfile);
		assertTrue("PVInfo PV name " + info.getPVName(), info.getPVName().equals(pvName));
		assertTrue("PVInfo year " + info.getDataYear(), info.getDataYear() == TimeUtils.getCurrentYear());
		assertTrue("PVInfo type " + info.getType(), info.getType().equals(ArchDBRTypes.DBR_SCALAR_DOUBLE));
		long firstSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		assertTrue("PVInfo first event time " + info.getFirstEventEpochSeconds() + "/" + firstSeconds, info.getFirstEventEpochSeconds() == firstSeconds);
		long lastSeconds = TimeUtils.getStartOfYearInSeconds(TimeUtils.getCurrentYear()+1)-1;
		assertTrue("PVInfo last event time " + info.getLastEventEpochSeconds() + "!=" + lastSeconds, info.getLastEventEpochSeconds() == lastSeconds);
	}
}
