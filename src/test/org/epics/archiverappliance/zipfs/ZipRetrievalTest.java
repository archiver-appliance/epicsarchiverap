package org.epics.archiverappliance.zipfs;

import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.text.DecimalFormat;
import java.time.Instant;

/**
 * Test basic functionality when using zipfs.  
 * Test using zip per pv and zip per day mechanisms for storage
 * @author mshankar
 *
 */
public class ZipRetrievalTest {
	private static Logger logger = LogManager.getLogger(ZipRetrievalTest.class.getName());
	File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ZipRetrievalTest");
	private ConfigService configService;


	@BeforeEach
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(-1);
		if(testFolder.exists()) { 
			FileUtils.deleteDirectory(testFolder);
		}
		testFolder.mkdirs();		
	}

	@AfterEach
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(testFolder);
	}

	@Test
	public void testWrapperSeekableByteChannel() throws Exception {
		File skFile = new File(testFolder.getAbsolutePath() + File.separator + "sk.txt");
		int lineCount = 1000;
		try(PrintWriter writer = new PrintWriter(new FileOutputStream(skFile))) {
			DecimalFormat format = new DecimalFormat("00000");
			for(int i = 0; i < lineCount; i++) {
				writer.print(format.format(i));
				writer.print("\n");
			}
		}
		
		try(ArchPaths paths = new ArchPaths()) {
			Path zipPath = paths.get(true, "jar:file://" + testFolder.getAbsolutePath(), "sk.zip!/sk.txt");
			Files.copy(skFile.toPath(), zipPath, StandardCopyOption.REPLACE_EXISTING);
		}
		
		String zipFileStr = testFolder.getAbsolutePath() + "/sk.zip";
		Assertions.assertTrue(new File(zipFileStr).exists(), "Zip file does not exist " + zipFileStr);
		logger.info("Checking seeks etc");
		try(ArchPaths paths = new ArchPaths()) {
			Path zipPath = paths.get("jar:file://" + testFolder.getAbsolutePath(), "sk.zip!/sk.txt");
			logger.debug(zipPath.toUri().toString());
			// Each line has 6 bytes...
			DecimalFormat format = new DecimalFormat("00000");
			for(int i = 0; i < lineCount; i++) {
				try(LineByteStream lis = new LineByteStream(zipPath, i*6)) {
					String line = new String(lis.readLine());
					String expectedStr = format.format(i);
					Assertions.assertTrue(line.equals(expectedStr), i + " line failed " + line);
					lis.seekToBeforeLastLine();
					line = new String(lis.readLine());
					expectedStr = format.format(lineCount - 1);
					Assertions.assertTrue(line.equals(expectedStr), "Last line failed " + line);
				}
			}
		}
	}
	
	@Test
	public void testSimpleArchivePVZipPerPV() throws Exception {
		String rootFolder = testFolder.getAbsolutePath();
		PlainPBStoragePlugin storagePlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=ZipTest&rootFolder=" + rootFolder + "&partitionGranularity=PARTITION_HOUR&compress=ZIP_PER_PV", configService);
		logger.info(storagePlugin.getURLRepresentation());
		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":SimpleZipTest";
		ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
		int phasediffindegrees = 10;
        short currentYear = TimeUtils.getCurrentYear();
        SimulationEventStream simstream = new SimulationEventStream(dbrType, new SineGenerator(phasediffindegrees), TimeUtils.getStartOfYear(currentYear), TimeUtils.getEndOfYear(currentYear), 1);
		try(BasicContext context = new BasicContext()) {
			storagePlugin.appendData(context, pvName, simstream);
		}
		File expectedZipFile = new File(testFolder + File.separator + configService.getPVNameToKeyConverter().convertPVNameToKey(pvName) + "_pb.zip");
		Assertions.assertTrue(expectedZipFile.exists(), "Zip file does not seem to exist " + expectedZipFile);

		logger.info("Testing retrieval for zip per pv");
		try(BasicContext context = new BasicContext();
				EventStream strm = new CurrentThreadWorkerEventStream(pvName, storagePlugin.getDataForPV(context, pvName, TimeUtils.getStartOfYear(TimeUtils.getCurrentYear()), TimeUtils.getEndOfYear(TimeUtils.getCurrentYear())))
				) {
			int eventCount = 0;
			for(@SuppressWarnings("unused") Event ev : strm) {
				eventCount++;
			}
			logger.info("Got " + eventCount + " events");
            Assertions.assertTrue(eventCount >= (simstream.getNumberOfEvents() - 1), "Retrieval does not seem to return any events " + eventCount);
		}
	}
	
	@Test
	public void testCompareContrastRetrievalZipPerPV() throws Exception {
		String rootFolder = testFolder.getAbsolutePath();
		ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
		{
			String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":CmpCrstUncompressed";
			PlainPBStoragePlugin storagePlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=ZipTest&rootFolder=" + rootFolder + "&partitionGranularity=PARTITION_DAY", configService);
			logger.info(storagePlugin.getURLRepresentation());
			int phasediffindegrees = 10;
            short currentYear = TimeUtils.getCurrentYear();
            SimulationEventStream simstream = new SimulationEventStream(dbrType, new SineGenerator(phasediffindegrees), TimeUtils.getStartOfYear(currentYear), TimeUtils.getEndOfYear(currentYear), 1);
			try(BasicContext context = new BasicContext()) {
				storagePlugin.appendData(context, pvName, simstream);
			}
		}
		{
			String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":CmpCrstCompressed";
			PlainPBStoragePlugin storagePlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=ZipTest&rootFolder=" + rootFolder + "&partitionGranularity=PARTITION_DAY&compress=ZIP_PER_PV", configService);
			logger.info(storagePlugin.getURLRepresentation());
			int phasediffindegrees = 10;
            short currentYear = TimeUtils.getCurrentYear();
            SimulationEventStream simstream = new SimulationEventStream(dbrType, new SineGenerator(phasediffindegrees), TimeUtils.getStartOfYear(currentYear), TimeUtils.getEndOfYear(currentYear), 1);
			try(BasicContext context = new BasicContext()) {
				storagePlugin.appendData(context, pvName, simstream);
			}
		}
		
		// Fetch a days worth of data from both plugins and not the difference.
		short currentYear = TimeUtils.getCurrentYear();
		{
			String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":CmpCrstUncompressed";
			PlainPBStoragePlugin storagePlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=ZipTest&rootFolder=" + rootFolder + "&partitionGranularity=PARTITION_DAY", configService);
			DecimalFormat format = new DecimalFormat("00");
			long totalTimeConsumed = 0;
			int numdays = 27;
			for(int day = 1; day <= numdays; day++) {
                Instant start = TimeUtils.convertFromISO8601String(currentYear + "-02-" + format.format(day) + "T08:00:00.000Z");
                Instant end = TimeUtils.convertFromISO8601String(currentYear + "-02-" + format.format(day + 1) + "T08:00:00.000Z");
				long startTime = System.currentTimeMillis();
				try(BasicContext context = new BasicContext();
						EventStream strm = new CurrentThreadWorkerEventStream(pvName, storagePlugin.getDataForPV(context, pvName, start, end))
						) {
					int eventCount = 0;
					for(@SuppressWarnings("unused") Event ev : strm) {
						eventCount++;
					}
					Assertions.assertTrue(eventCount >= 100, "Retrieval from uncompressed data does not seem to return any events " + eventCount);
					long endTime = System.currentTimeMillis();
					long timeconsumed = endTime - startTime;
					totalTimeConsumed += timeconsumed;
				}
			}
			logger.info("Uncompressed - Time taken to get data and count em " + ((double)(totalTimeConsumed))/numdays + "(ms)");
		}
			
		{
			String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":CmpCrstCompressed";
			PlainPBStoragePlugin storagePlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=ZipTest&rootFolder=" + rootFolder + "&partitionGranularity=PARTITION_DAY&compress=ZIP_PER_PV", configService);
			DecimalFormat format = new DecimalFormat("00");
			long totalTimeConsumed = 0;
			int numdays = 27;
			for(int day = 1; day <= numdays; day++) {
                Instant start = TimeUtils.convertFromISO8601String(currentYear + "-02-" + format.format(day) + "T08:00:00.000Z");
                Instant end = TimeUtils.convertFromISO8601String(currentYear + "-02-" + format.format(day + 1) + "T08:00:00.000Z");
				long startTime = System.currentTimeMillis();
				try(BasicContext context = new BasicContext();
						EventStream strm = new CurrentThreadWorkerEventStream(pvName, storagePlugin.getDataForPV(context, pvName, start, end))
						) {
					int eventCount = 0;
					for(@SuppressWarnings("unused") Event ev : strm) {
						eventCount++;
					}
					Assertions.assertTrue(eventCount >= 100, "Retrieval from compressed data does not seem to return any events " + eventCount);
					long endTime = System.currentTimeMillis();
					long timeconsumed = endTime - startTime;
					totalTimeConsumed += timeconsumed;
				}
			}
			logger.info("Compressed - Time taken to get data and count em " + ((double)(totalTimeConsumed))/numdays + "(ms)");
		}
	}
}
