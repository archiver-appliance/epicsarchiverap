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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;

/**
 * Test the mapping of PVs to file/key names
 * @author mshankar
 *
 */
public class PlainPBFileNameUtilityTest {
	String fileName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "PlainPBFileNameUtility/";
	String rootFolderStr = fileName;
	private ConfigService configService;


	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		File rootFolder = new File(rootFolderStr);
		if(rootFolder.exists()) {
			FileUtils.deleteDirectory(rootFolder);
		}
		
		rootFolder.mkdirs();
	}

	@After
	public void tearDown() throws Exception {
		File rootFolder = new File(rootFolderStr);
		FileUtils.deleteDirectory(rootFolder);
	}

	@Test
	public void testGetFilesWithDataOnAHourPartition() throws Exception {
		// Lets create some files that cater to this partition.
		long startOfYearEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		String pvName = "First:Second:Third:HourPart_1";
		PartitionGranularity partitionHour = PartitionGranularity.PARTITION_HOUR;
		String extension = ".pb";
		for(int hours = 0; hours < 24; hours++) {
			mkPath(PlainPBPathNameUtility.getPathNameForTime(rootFolderStr, pvName, (startOfYearEpochSeconds + hours*3600), partitionHour, new ArchPaths(), CompressionMode.NONE, configService.getPVNameToKeyConverter()));
		}
		
		Path[] matchingPaths = PlainPBPathNameUtility.getPathsWithData(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds, 0), TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600 - 1, 0), extension, partitionHour, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + matchingPaths.length, matchingPaths.length == 4);
		
		Path[] etlPaths = PlainPBPathNameUtility.getPathsBeforeCurrentPartition(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600, 0), extension, partitionHour, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + etlPaths.length, etlPaths.length == 4);
		
		File mostRecentFile = PlainPBPathNameUtility.getMostRecentPathBeforeTime(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600, 0), extension, partitionHour, CompressionMode.NONE, configService.getPVNameToKeyConverter()).toFile();
		assertTrue("Most recent file is null?", mostRecentFile != null);
		assertTrue("Unxpected most recent file " + mostRecentFile.getAbsolutePath(), mostRecentFile.getName().endsWith("03.pb"));
	}
	
	@Test
	public void testGetFilesWithDataOnADayPartition() throws Exception {
		// Lets create some files that cater to this partition.
		long startOfYearEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		String pvName = "First:Second:Third:DayPart_1";
		PartitionGranularity partitionDay = PartitionGranularity.PARTITION_DAY;
		String extension = ".pb";
		for(int days = 0; days < 50; days++) {
			mkPath(PlainPBPathNameUtility.getPathNameForTime(rootFolderStr, pvName, (startOfYearEpochSeconds + days*24*3600), partitionDay, new ArchPaths(), CompressionMode.NONE, configService.getPVNameToKeyConverter()));
		}
		
		Path[] matchingPaths = PlainPBPathNameUtility.getPathsWithData(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds, 0), TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 10*24*3600 - 1, 0), extension, partitionDay, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + matchingPaths.length, matchingPaths.length == 10);

		Path[] etlPaths = PlainPBPathNameUtility.getPathsBeforeCurrentPartition(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 10*24*3600, 0), extension, partitionDay, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + etlPaths.length, etlPaths.length == 10);
		
		// We have 50 days of data, ask for the 51st day here.
		File mostRecentFile = PlainPBPathNameUtility.getMostRecentPathBeforeTime(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 51*24*3600, 0), extension, partitionDay, CompressionMode.NONE, configService.getPVNameToKeyConverter()).toFile();
		assertTrue("Most recent file is null?", mostRecentFile != null);
		assertTrue("Unxpected most recent file " + mostRecentFile.getAbsolutePath(), mostRecentFile.getName().endsWith("19.pb"));

	}

	@Test
	public void testGetFilesWithDataOnAMonthPartition() throws Exception {
		// Lets create some files that cater to this partition.
		long startOfYearEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		DateTime curr = new DateTime(startOfYearEpochSeconds*1000);
		String pvName = "First:Second:Third:MonthPart_1";
		PartitionGranularity partition = PartitionGranularity.PARTITION_MONTH;
		String extension = ".pb";
		DateTime endMonth = null;
		for(int months = 1; months <= 12; months++) {
			mkPath(PlainPBPathNameUtility.getPathNameForTime(rootFolderStr, pvName, curr.getMillis()/1000, partition, new ArchPaths(), CompressionMode.NONE, configService.getPVNameToKeyConverter()));
			curr = curr.plusMonths(1);
			if(months == 4) endMonth = curr;
		}
		
		Path[] matchingPaths = PlainPBPathNameUtility.getPathsWithData(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds, 0), TimeUtils.convertFromEpochSeconds(endMonth.getMillis()/1000 - 1, 0), extension, partition, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + matchingPaths.length, matchingPaths.length == 4);

		Path[] etlPaths = PlainPBPathNameUtility.getPathsBeforeCurrentPartition(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(endMonth.getMillis()/1000, 0), extension,  partition, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + etlPaths.length, etlPaths.length == 3);
		
		// Ask for the next year here; the last file written out is for Nov so expect 11.pb here
		File mostRecentFile = PlainPBPathNameUtility.getMostRecentPathBeforeTime(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds((curr.plusMonths(5).getMillis())/1000, 0), extension, partition, CompressionMode.NONE, configService.getPVNameToKeyConverter()).toFile();
		assertTrue("Most recent file is null?", mostRecentFile != null);
		assertTrue("Unxpected most recent file " + mostRecentFile.getAbsolutePath(), mostRecentFile.getName().endsWith("11.pb"));

	}

	@Test
	public void testGetFilesWithDataOnAYearlyPartition() throws Exception {
		// Lets create some files that cater to this partition.
		long startOfYearEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		DateTime curr = new DateTime(startOfYearEpochSeconds*1000);
		String pvName = "First:Second:Third:YearPart_1";
		PartitionGranularity partition = PartitionGranularity.PARTITION_YEAR;
		String extension = ".pb";
		DateTime endYear = null;
		for(int years = 0; years < 20; years++) {
			mkPath(PlainPBPathNameUtility.getPathNameForTime(rootFolderStr, pvName, curr.getMillis()/1000, partition, new ArchPaths(), CompressionMode.NONE, configService.getPVNameToKeyConverter()));
			curr = curr.plusYears(1);
			if(years == 7) endYear = curr;
		}
		
		Path[] matchingPaths = PlainPBPathNameUtility.getPathsWithData(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds, 0), TimeUtils.convertFromEpochSeconds(endYear.getMillis()/1000 - 1, 0), extension, partition, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + matchingPaths.length, matchingPaths.length == 8);

		Path[] etlPaths = PlainPBPathNameUtility.getPathsBeforeCurrentPartition(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(endYear.getMillis()/1000, 0), extension,  partition, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + etlPaths.length, etlPaths.length == 8);
		
		// Ask for the next year here; the last file written out is for Nov so expect 11.pb here
		File mostRecentFile = PlainPBPathNameUtility.getMostRecentPathBeforeTime(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds((curr.plusYears(1).getMillis())/1000, 0), extension, partition, CompressionMode.NONE, configService.getPVNameToKeyConverter()).toFile();
		assertTrue("Most recent file is null?", mostRecentFile != null);
		assertTrue("Unxpected most recent file " + mostRecentFile.getAbsolutePath(), mostRecentFile.getName().endsWith(curr.getYear() + ".pb"));

		File mostRecentFile2 = PlainPBPathNameUtility.getMostRecentPathBeforeTime(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds((endYear.getMillis())/1000, 0), extension, partition, CompressionMode.NONE, configService.getPVNameToKeyConverter()).toFile();
		assertTrue("Most recent file is null?", mostRecentFile2 != null);
		String expectedEnd2 = endYear.getYear() + ".pb";
		assertTrue("Unxpected most recent file " + mostRecentFile2.getAbsolutePath() + " expecting " + expectedEnd2, mostRecentFile2.getName().endsWith(expectedEnd2));
	}
	
	
	@Test
	public void testGetFilesWithDataOnA5MinPartition() throws Exception {
		// Lets create some files that cater to this partition.
		long startOfYearEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		String pvName = "First:Second:Third:Hour:5MinPart_1";
		PartitionGranularity partition5Min = PartitionGranularity.PARTITION_5MIN;
		String extension = ".pb";
		for(int mins = 0; mins < 24*60; mins+=5) {
			mkPath(PlainPBPathNameUtility.getPathNameForTime(rootFolderStr, pvName, (startOfYearEpochSeconds + mins*60), partition5Min, new ArchPaths(), CompressionMode.NONE, configService.getPVNameToKeyConverter()));
		}
		
		Path[] matchingPaths = PlainPBPathNameUtility.getPathsWithData(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds, 0), TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600 - 1, 0), extension, partition5Min, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + matchingPaths.length, matchingPaths.length == 4*12);
		
		Path[] etlPaths = PlainPBPathNameUtility.getPathsBeforeCurrentPartition(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600, 0), extension, partition5Min, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + etlPaths.length, etlPaths.length == 4*12);
		
		File mostRecentFile = PlainPBPathNameUtility.getMostRecentPathBeforeTime(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600, 0), extension, partition5Min, CompressionMode.NONE, configService.getPVNameToKeyConverter()).toFile();
		assertTrue("Most recent file is null?", mostRecentFile != null);
		assertTrue("Unxpected most recent file " + mostRecentFile.getAbsolutePath(), mostRecentFile.getName().endsWith("03_55.pb"));
	}
	
	
	@Test
	public void testGetFilesWithDataOnA15MinPartition() throws Exception {
		// Lets create some files that cater to this partition.
		long startOfYearEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		String pvName = "First:Second:Third:Hour:15MinPart_1";
		PartitionGranularity partition = PartitionGranularity.PARTITION_15MIN;
		String extension = ".pb";
		for(int mins = 0; mins < 24*60; mins+=5) {
			mkPath(PlainPBPathNameUtility.getPathNameForTime(rootFolderStr, pvName, (startOfYearEpochSeconds + mins*60), partition, new ArchPaths(), CompressionMode.NONE, configService.getPVNameToKeyConverter()));
		}
		
		Path[] matchingPaths = PlainPBPathNameUtility.getPathsWithData(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds, 0), TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600 - 1, 0), extension, partition, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + matchingPaths.length, matchingPaths.length == 4*4);
		
		Path[] etlPaths = PlainPBPathNameUtility.getPathsBeforeCurrentPartition(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600, 0), extension, partition, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + etlPaths.length, etlPaths.length == 4*4);
		
		File mostRecentFile = PlainPBPathNameUtility.getMostRecentPathBeforeTime(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600, 0), extension, partition, CompressionMode.NONE, configService.getPVNameToKeyConverter()).toFile();
		assertTrue("Most recent file is null?", mostRecentFile != null);
		assertTrue("Unxpected most recent file " + mostRecentFile.getAbsolutePath(), mostRecentFile.getName().endsWith("03_45.pb"));
	}


	@Test
	public void testGetFilesWithDataOnA30MinPartition() throws Exception {
		// Lets create some files that cater to this partition.
		long startOfYearEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		String pvName = "First:Second:Third:Hour:30MinPart_1";
		PartitionGranularity partition = PartitionGranularity.PARTITION_30MIN;
		String extension = ".pb";
		for(int mins = 0; mins < 24*60; mins+=5) {
			mkPath(PlainPBPathNameUtility.getPathNameForTime(rootFolderStr, pvName, (startOfYearEpochSeconds + mins*60), partition, new ArchPaths(), CompressionMode.NONE, configService.getPVNameToKeyConverter()));
		}
		
		Path[] matchingPaths = PlainPBPathNameUtility.getPathsWithData(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds, 0), TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600 - 1, 0), extension, partition, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + matchingPaths.length, matchingPaths.length == 4*2);
		
		Path[] etlPaths = PlainPBPathNameUtility.getPathsBeforeCurrentPartition(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600, 0), extension, partition, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		assertTrue("File count " + etlPaths.length, etlPaths.length == 4*2);
		
		File mostRecentFile = PlainPBPathNameUtility.getMostRecentPathBeforeTime(new ArchPaths(), rootFolderStr, pvName, TimeUtils.convertFromEpochSeconds(startOfYearEpochSeconds + 4*3600, 0), extension, partition, CompressionMode.NONE, configService.getPVNameToKeyConverter()).toFile();
		assertTrue("Most recent file is null?", mostRecentFile != null);
		assertTrue("Unxpected most recent file " + mostRecentFile.getAbsolutePath(), mostRecentFile.getName().endsWith("03_30.pb"));
	}
	
	private static void mkPath(Path nf) throws IOException {
		if(!Files.exists(nf)) {
			Files.createDirectories(nf.getParent());
			Files.createFile(nf);
		}
	}
}
