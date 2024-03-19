/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;


import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;

/**
 * Test the ETL source funtionality of PlainPBStoragePlugin
 * @author mshankar
 *
 */
public class ETLSourceGetStreamsTest {
	PlainPBStoragePlugin pbplugin = null;
	File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ETLSrcStreamsTest");
	private ConfigService configService;

	@BeforeEach
	public void setUp() throws Exception {
		testFolder.mkdirs();
		configService = new ConfigServiceForTests(-1);
		pbplugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + testFolder + "/src&partitionGranularity=PARTITION_HOUR", configService);
	}

	@AfterEach
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(testFolder);
	}
	
	class DataForGetETLStreamsTest {
		long sampleRange;
		int skipSeconds;
		public DataForGetETLStreamsTest(long sampleRange, int skipSeconds) {
			this.sampleRange = sampleRange;
			this.skipSeconds = skipSeconds;
		}
	}

	@Test
	public void getETLStreams() throws Exception {
		short currentYear = TimeUtils.getCurrentYear();
		ZonedDateTime startOfToday = ZonedDateTime.now(ZoneId.from(ZoneOffset.UTC))
				.withHour(0)
				.withMinute(0)
				.withSecond(0);

		HashMap<PartitionGranularity, DataForGetETLStreamsTest> testParams = new HashMap<PartitionGranularity, DataForGetETLStreamsTest>();
		testParams.put(PartitionGranularity.PARTITION_5MIN, new DataForGetETLStreamsTest(3600*24, 600)); // One day; sample every 10 mins
		testParams.put(PartitionGranularity.PARTITION_15MIN, new DataForGetETLStreamsTest(3600*24, 600)); // One day; sample every 10 mins
		testParams.put(PartitionGranularity.PARTITION_30MIN, new DataForGetETLStreamsTest(3600*24, 600)); // One day; sample every 10 mins
		testParams.put(PartitionGranularity.PARTITION_HOUR, new DataForGetETLStreamsTest(3600*24, 600)); // One day; sample every 10 mins
		testParams.put(PartitionGranularity.PARTITION_DAY, new DataForGetETLStreamsTest(3600*24*7, 1800)); // One week; sample every 30 mins
		testParams.put(PartitionGranularity.PARTITION_MONTH, new DataForGetETLStreamsTest(3600*24*7*365, 3600*12)); // One year; sample every 1/2 day
		testParams.put(PartitionGranularity.PARTITION_YEAR, new DataForGetETLStreamsTest(3600L * 24 * 7 * 365 * 10, 3600 * 24 * 7)); // 10 years; sample every week
		
		
		for(PartitionGranularity partitionGranularity : PartitionGranularity.values()) {
			ETLContext etlContext = new ETLContext();
			DataForGetETLStreamsTest testParam = testParams.get(partitionGranularity);
			long sampleRange = testParam.sampleRange;
			int skipSeconds = testParam.skipSeconds;

			File rootFolder = new File(testFolder.getAbsolutePath() + File.separator + partitionGranularity.toString());
			rootFolder.mkdirs();
			pbplugin.setRootFolder(rootFolder.getAbsolutePath());
			pbplugin.setPartitionGranularity(partitionGranularity);
			String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":ETLSourceGetStreamsTest:" + partitionGranularity;
			ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
			ArrayListEventStream testData = new ArrayListEventStream(1000, new RemotableEventStreamDesc(type, pvName, currentYear));
			for(long i = 0; i < sampleRange; i+=skipSeconds) {
				testData.add(new SimulationEvent(TimeUtils.getSecondsIntoYear(startOfToday.toEpochSecond() + i), TimeUtils.computeYearForEpochSeconds(startOfToday.toEpochSecond() + i), type, new ScalarValue<Double>((double) i)));
			}
			try(BasicContext context = new BasicContext()) {
				pbplugin.appendData(context, pvName, testData);
			}
			// This should have generated many files; one for each partition.
			// So we now check the number of files we get as we cruise thru the whole day.
			
			int expectedFiles = 0;
			Instant firstSecondOfNextPartition = TimeUtils.getNextPartitionFirstSecond(startOfToday.toInstant(), partitionGranularity);
			for(long i = 0; i < sampleRange; i+=skipSeconds) {
				Instant currentTime = startOfToday.toInstant().plusSeconds(i);
				if (currentTime.isAfter(firstSecondOfNextPartition) || currentTime.equals(firstSecondOfNextPartition)) {
					firstSecondOfNextPartition = TimeUtils.getNextPartitionFirstSecond(currentTime, partitionGranularity);
					expectedFiles++;
				}
				List<ETLInfo> ETLFiles = pbplugin.getETLStreams(pvName, currentTime, etlContext);
				Assertions.assertTrue((ETLFiles != null) ? (ETLFiles.size() == expectedFiles) : (expectedFiles == 0), "getETLStream failed for "
						+ TimeUtils.convertToISO8601String(currentTime)
						+ " for partition " + partitionGranularity
						+ " Expected " + expectedFiles + " got " + (ETLFiles != null ? Integer.toString(ETLFiles.size()) : "null"));
			}
		}
	}
}
