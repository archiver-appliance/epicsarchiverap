/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB;


import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.GenerateData;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStreamIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PB.data.PBScalarDouble;
import edu.stanford.slac.archiverappliance.PB.search.FileEventStreamSearch;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

/**
 * Test searches in PB files.
 * @author mshankar
 *
 */
public class SearchInPBFileTest {
	private static Logger logger = LogManager.getLogger(SearchInPBFileTest.class.getName());
	PBCommonSetup pbSetup = new PBCommonSetup();
	private ConfigService configService;

	@BeforeEach
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
	}
	@Test
	@Tag("slow")
	public void testSeekToTime() throws Exception {
		PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
		pbSetup.setUpRootFolder(pbplugin);
		GenerateData.generateSineForPV("Sine1", 0, ArchDBRTypes.DBR_SCALAR_DOUBLE);
		try {
			Path testPath = PlainPBPathNameUtility.getPathNameForTime(pbplugin, "Sine1", TimeUtils.getStartOfCurrentYearInSeconds(), new ArchPaths(), configService.getPVNameToKeyConverter());
			logger.info("Searching for times in file " + testPath);
			long filelen = Files.size(testPath);
			int step = 983;
			PBFileInfo fileInfo = new PBFileInfo(testPath);
			// step is some random prime number that hopefully makes this go thru all the reasonable cases.
			// We need to start from 2 as the SimulationEventStreamIterator generates data from 1 and we return success only if we find e1 < sample <= e2
			for(int secondsintoyear = 2; secondsintoyear < SimulationEventStreamIterator.DEFAULT_NUMBER_OF_SAMPLES; secondsintoyear+=step) {
				FileEventStreamSearch bsend = new FileEventStreamSearch(testPath, fileInfo.getPositionOfFirstSample());
				boolean posFound = bsend.seekToTime(ArchDBRTypes.DBR_SCALAR_DOUBLE, secondsintoyear);
				Assertions.assertTrue(posFound, "Could not find " + secondsintoyear);
				long position = bsend.getFoundPosition();
				Assertions.assertTrue(position > 0);
				Assertions.assertTrue(position < filelen);
				try(LineByteStream lis = new LineByteStream(testPath, position)) {
					lis.seekToFirstNewLine();
					ByteArray bar = new ByteArray(LineByteStream.MAX_LINE_SIZE);
					lis.readLine(bar);
					PBScalarDouble pbEvent = new PBScalarDouble(TimeUtils.getCurrentYear(), bar);
					Assertions.assertEquals(pbEvent.getSecondsIntoYear(), secondsintoyear - 1, "Searched for " + secondsintoyear + " got " + pbEvent.getSecondsIntoYear());
				}
			}
		} catch(Exception ex) {
			logger.error(ex.getMessage(), ex);
			Assertions.fail(ex.getMessage());
		}
	}
}
