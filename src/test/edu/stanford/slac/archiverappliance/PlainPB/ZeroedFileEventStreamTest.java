package edu.stanford.slac.archiverappliance.PlainPB;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;

/**
 * Test the PlainPB Event stream when we have unexpected garbage in the data.
 * @author mshankar
 *
 */
public class ZeroedFileEventStreamTest {
	private static final Logger logger = LogManager.getLogger(ZeroedFileEventStreamTest.class.getName());
	String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ZeroedFileEventStreamTestTest/";
	File rootFolder = new File(rootFolderName);
	static String pvNamePrefix = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ZeroedFileEventStreamTestTest";
	PlainPBStoragePlugin pbplugin;
	static short currentYear = TimeUtils.getCurrentYear();
	private ConfigService configService;
	static ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		pbplugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + rootFolderName + "&partitionGranularity=PARTITION_YEAR", configService);
	}
	
	private static void generateFreshData(PlainPBStoragePlugin pbplugin4data, String pvName) {
		File rootFolder = new File(pbplugin4data.getRootFolder());
		if(rootFolder.exists()) {
			try {
				FileUtils.deleteDirectory(rootFolder);
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
		}

		try(BasicContext context = new BasicContext()) {
			for(int day = 0; day < 365; day++) {
				ArrayListEventStream testData = new ArrayListEventStream(24*60*60, new RemotableEventStreamDesc(type, pvName, currentYear));
				int startofdayinseconds = day*24*60*60;
				for(int secondintoday = 0; secondintoday < 24*60*60; secondintoday+=5*60) {
					testData.add(new SimulationEvent(startofdayinseconds + secondintoday, currentYear, type, new ScalarValue<Double>((double) secondintoday)));
				}
				pbplugin4data.appendData(context, pvName, testData);
			}
		} catch (IOException e) {
			logger.error(e);
			fail();
		}
	}
	

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(new File(rootFolderName));
		FileUtils.deleteDirectory(new File(rootFolderName + "Dest"));
	}

	/**
	 * Generate PB file with bad footers and then see if we survive PBFileInfo.
	 */
	@Test
	public void testBadFooters() {
		logger.info("Testing garbage in the last record");
		String pvName = pvNamePrefix + "testBadFooters";
		generateFreshData(pbplugin, pvName);
		Path[] paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolderName, pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		} catch (IOException e) {
			logger.error(e);
			fail();
		}

		assertTrue("Cannot seem to find any plain pb files in " + rootFolderName + " for pv " + pvName, paths != null && paths.length > 0);
		
		// Overwrite the tail end of each file with some garbage. 
		for(Path path : paths) { 
			try(SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
				// Seek to somewhere at the end
				int bytesToOverwrite = 100;
				channel.position(channel.size() - bytesToOverwrite);
				ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
				byte[] junk = new byte[bytesToOverwrite];
				new Random().nextBytes(junk);
				buf.put(junk);
				buf.flip();
				channel.write(buf);
			} catch (IOException e) {
				logger.error(e);
				fail();
			}

			PBFileInfo info = null;
			try {
				info = new PBFileInfo(path);
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
			assertTrue("Cannot generate PBFileInfo from " + path, true);
			assertEquals("pvNames are different " + info.getPVName() + " expecting " + pvName, info.getPVName(), pvName);
			assertNotNull("Last event is null", info.getLastEvent());
			Timestamp lastEventTs = info.getLastEvent().getEventTimeStamp();
			logger.info(TimeUtils.convertToHumanReadableString(lastEventTs));
			assertTrue("Last event is incorrect " + TimeUtils.convertToHumanReadableString(lastEventTs), lastEventTs.after(TimeUtils.convertFromISO8601String(currentYear + "-12-30T00:00:00.000Z")) && lastEventTs.before(TimeUtils.convertFromISO8601String(currentYear+1 + "-01-01T00:00:00.000Z")));
			try(FileBackedPBEventStream strm = new FileBackedPBEventStream(pvName, path, type)) {
				long eventCount = 0;
				for(@SuppressWarnings("unused") Event e : strm) { 
					eventCount++;
				}
				assertTrue("Event count is too low " + eventCount, eventCount > 365);
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
		}
	}

	
	/**
	 * Generate PB file with bad footers in the ETL source and then see if we survive ETL
	 */
	@Test
	public void testBadFootersInSrcETL() {
		PlainPBStoragePlugin srcPlugin = null;
		try {
			srcPlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + rootFolderName + "&partitionGranularity=PARTITION_MONTH", configService);
		} catch (IOException e) {
			logger.error(e);
			fail();
		}
		String pvName = pvNamePrefix + "testBadFootersInSrcETL";
		assert srcPlugin != null;
		generateFreshData(srcPlugin, pvName);
		Path[] paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolderName, pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		} catch (IOException e) {
			logger.error(e);
			fail();
		}

		assertTrue("Cannot seem to find any plain pb files in " + rootFolderName + " for pv " + pvName, paths != null && paths.length > 0);
		
		// Overwrite the tail end of each file with some garbage. 
		for(Path path : paths) { 
			try(SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
				// Seek to somewhere at the end
				int bytesToOverwrite = 100;
				channel.position(channel.size() - bytesToOverwrite);
				ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
				byte[] junk = new byte[bytesToOverwrite];
				new Random().nextBytes(junk);
				buf.put(junk);
				buf.flip();
				channel.write(buf);
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
		}
		
		PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		String[] dataStores = new String[] { srcPlugin.getURLRepresentation(), "pb://localhost?name=STS&rootFolder=" + rootFolderName + "Dest" + "&partitionGranularity=PARTITION_YEAR" }; 
		typeInfo.setDataStores(dataStores);
		configService.updateTypeInfoForPV(pvName, typeInfo);
		try {
			configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
		} catch (AlreadyRegisteredException e) {
			logger.error(e);
			fail();
		}
		configService.getETLLookup().manualControlForUnitTests();

		Timestamp timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 366);
		DateTime ts = new DateTime(DateTimeZone.UTC);
		if(ts.getMonthOfYear() == 1) {
			// This means that we never test this in Jan but I'd rather have the null check than skip this. 
			timeETLruns = TimeUtils.plusDays(timeETLruns, 35);
		}
		try {
			ETLExecutor.runETLs(configService, timeETLruns);
		} catch (IOException e) {
			logger.error(e);
			fail();
		}
		logger.info("Done performing ETL");
		
		paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolderName + "Dest", pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		} catch (IOException e) {
			logger.error(e);
			fail();
		}

		assertTrue("ETL did not seem to move any data?", paths.length > 0);

		long eventCount = 0;
		for(Path path : paths) {
			PBFileInfo info = null;
			try {
				info = new PBFileInfo(path);
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
			assertNotNull("Cannot generate PBFileInfo from " + path, info);
			assertEquals("pvNames are different " + info.getPVName() + " expecting " + pvName, info.getPVName(), pvName);
			assertNotNull("Last event is null", info.getLastEvent());
			Timestamp lastEventTs = info.getLastEvent().getEventTimeStamp();
			logger.info(TimeUtils.convertToHumanReadableString(lastEventTs));
			assertTrue("Last event is incorrect " + TimeUtils.convertToHumanReadableString(lastEventTs), lastEventTs.after(TimeUtils.convertFromISO8601String(currentYear + "-12-30T00:00:00.000Z")) && lastEventTs.before(TimeUtils.convertFromISO8601String(currentYear+1 + "-01-01T00:00:00.000Z")));
			try(FileBackedPBEventStream strm = new FileBackedPBEventStream(pvName, path, type)) {
				for(@SuppressWarnings("unused") Event e : strm) { 
					eventCount++;
				}
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
		}
		int expectedEventCount = 360*24*12;
		assertTrue("Event count is too low " + eventCount + " expecting at least " + expectedEventCount, eventCount >= expectedEventCount);
	}
	
	
	/**
	 * Generate PB file with bad footers in the ETL dest and then see if we survive ETL
	 */
	@Test
	public void testBadFootersInDestETL() {
		String pvName = pvNamePrefix + "testBadFootersInDestETL";

		PlainPBStoragePlugin destPlugin = null;
		try {
			destPlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + rootFolderName + "Dest" + "&partitionGranularity=PARTITION_YEAR", configService);
		} catch (IOException e) {
			logger.error(e);
			fail();
		}
		assert destPlugin != null;
		File destFolder = new File(destPlugin.getRootFolder());
		if(destFolder.exists()) {
			try {
				FileUtils.deleteDirectory(destFolder);
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
		}

		PlainPBStoragePlugin srcPlugin = null;
		try {
			srcPlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + rootFolderName + "&partitionGranularity=PARTITION_MONTH", configService);
		} catch (IOException e) {
			logger.error(e);
			fail();
		}
		assert srcPlugin != null;
		File srcFolder = new File(srcPlugin.getRootFolder());
		if(srcFolder.exists()) {
			try {
				FileUtils.deleteDirectory(srcFolder);
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
		}

		try(BasicContext context = new BasicContext()) {
			for(int day = 0; day < 180; day++) { // Generate data for half the year...
				ArrayListEventStream testData = new ArrayListEventStream(24*60*60, new RemotableEventStreamDesc(type, pvName, currentYear));
				int startofdayinseconds = day*24*60*60;
				for(int secondintoday = 0; secondintoday < 24*60*60; secondintoday+=5*60) {
					testData.add(new SimulationEvent(startofdayinseconds + secondintoday, currentYear, type, new ScalarValue<Double>((double) secondintoday)));
				}
				destPlugin.appendData(context, pvName, testData);
			}
		} catch (IOException e) {
			logger.error(e);
			fail();
		}

		Path[] paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), destPlugin.getRootFolder(), pvName, ".pb", destPlugin.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
		} catch (IOException e) {
			logger.error(e);
			fail();
		}

		assertTrue("Cannot seem to find any plain pb files in " + destPlugin.getRootFolder() + " for pv " + pvName, paths.length > 0);
		
		// Overwrite the tail end of each file with some garbage. 
		for(Path path : paths) { 
			try(SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
				// Seek to somewhere at the end
				int bytesToOverwrite = 100;
				channel.position(channel.size() - bytesToOverwrite);
				ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
				byte[] junk = new byte[bytesToOverwrite];
				new Random().nextBytes(junk);
				buf.put(junk);
				buf.flip();
				channel.write(buf);
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
		}
		

		try(BasicContext context = new BasicContext()) {
			for(int day = 180; day < 365; day++) { // Generate data for the remaining half
				ArrayListEventStream testData = new ArrayListEventStream(24*60*60, new RemotableEventStreamDesc(type, pvName, currentYear));
				int startofdayinseconds = day*24*60*60;
				for(int secondintoday = 0; secondintoday < 24*60*60; secondintoday+=5*60) {
					testData.add(new SimulationEvent(startofdayinseconds + secondintoday, currentYear, type, new ScalarValue<Double>((double) secondintoday)));
				}
				srcPlugin.appendData(context, pvName, testData);
			}
		} catch (IOException e) {
			logger.error(e);
			fail();
		}

		PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		String[] dataStores = new String[] { srcPlugin.getURLRepresentation(), destPlugin.getURLRepresentation() }; 
		typeInfo.setDataStores(dataStores);
		configService.updateTypeInfoForPV(pvName, typeInfo);
		try {
			configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
		} catch (AlreadyRegisteredException e) {
			logger.error(e);
			fail();
		}
		configService.getETLLookup().manualControlForUnitTests();

		Timestamp timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 366);
		DateTime ts = new DateTime(DateTimeZone.UTC);
		if(ts.getMonthOfYear() == 1) {
			// This means that we never test this in Jan but I'd rather have the null check than skip this. 
			timeETLruns = TimeUtils.plusDays(timeETLruns, 35);
		}
		try {
			ETLExecutor.runETLs(configService, timeETLruns);
		} catch (IOException e) {
			logger.error(e);
			fail();
		}
		logger.info("Done performing ETL");
		
		paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), destPlugin.getRootFolder(), pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		} catch (IOException e) {
			logger.error(e);
			fail();
		}

		assertTrue("ETL did not seem to move any data?", paths.length > 0);

		long eventCount = 0;
		for(Path path : paths) {
			PBFileInfo info = null;
			try {
				info = new PBFileInfo(path);
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
			assertNotNull("Cannot generate PBFileInfo from " + path, info);
			assertEquals("pvNames are different " + info.getPVName() + " expecting " + pvName, info.getPVName(), pvName);
			assertNotNull("Last event is null", info.getLastEvent());
			Timestamp lastEventTs = info.getLastEvent().getEventTimeStamp();
			logger.info(TimeUtils.convertToHumanReadableString(lastEventTs));
			assertTrue("Last event is incorrect " + TimeUtils.convertToHumanReadableString(lastEventTs), lastEventTs.after(TimeUtils.convertFromISO8601String(currentYear + "-12-30T00:00:00.000Z")) && lastEventTs.before(TimeUtils.convertFromISO8601String(currentYear+1 + "-01-01T00:00:00.000Z")));
			try(FileBackedPBEventStream strm = new FileBackedPBEventStream(pvName, path, type)) {
				for(@SuppressWarnings("unused") Event e : strm) { 
					eventCount++;
				}
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
		}
		int expectedEventCount = 360*24*12;
		assertTrue("Event count is too low " + eventCount + " expecting at least " + expectedEventCount, eventCount >= expectedEventCount);
	}

	
	/**
	 * Generate PB file with bad footers and then see if we survive retrieval
	 */
	@Test
	public void testBadFootersRetrieval() {
		String pvName = pvNamePrefix + "testBadFootersRetrieval";

		generateFreshData(pbplugin, pvName);
		Path[] paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolderName, pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		} catch (IOException e) {
			logger.error(e);
			fail();
		}

		assertTrue("Cannot seem to find any plain pb files in " + rootFolderName + " for pv " + pvName, paths.length > 0);
		
		// Overwrite the tail end of each file with some garbage. 
		for(Path path : paths) { 
			try(SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
				// Seek to somewhere at the end
				int bytesToOverwrite = 100;
				channel.position(channel.size() - bytesToOverwrite);
				ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
				byte[] junk = new byte[bytesToOverwrite];
				new Random().nextBytes(junk);
				buf.put(junk);
				buf.flip();
				channel.write(buf);
			} catch (IOException e) {
				logger.error(e);
				fail();
			}
		}
		
		Timestamp start = TimeUtils.convertFromISO8601String(currentYear + "-03-01T00:00:00.000Z");
		Timestamp end = TimeUtils.convertFromISO8601String(currentYear + "-04-01T00:00:00.000Z");
		try(BasicContext context = new BasicContext(); EventStream result = new CurrentThreadWorkerEventStream(pvName, pbplugin.getDataForPV(context, pvName, start, end))) {
			long eventCount = 0;
			for(@SuppressWarnings("unused") Event e : result) { 
				eventCount++;
			}
			int expectedCount = 31*24*12 + 1;  // 12 points per hour
			assertEquals("Event count is too low " + eventCount + " expecting " + expectedCount, eventCount, expectedCount);
		} catch (IOException e) {
			logger.error(e);
			fail();
		}
	}
	
	
	/**
	 * Generate PB file with zeroes at random places and then see if we survive retrieval
	 */
	@Test
	public void testZeroedDataRetrieval(){
		String pvName = pvNamePrefix + "testZeroedDataRetrieval";

		generateFreshData(pbplugin, pvName);
		Path[] paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolderName, pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		} catch (IOException e) {
			logger.warn(e);
			fail();
		}

		assertTrue("Cannot seem to find any plain pb files in " + rootFolderName + " for pv " + pvName, paths != null && paths.length > 0);
		
		// Overwrite some lines in the file at random places. 
		int zeroedLines = 100;
		Random random = new Random();
		for(Path path : paths) { 
			try(SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
				for(int i = 0; i < zeroedLines; i++) { 
					int bytesToOverwrite = 10;
					// Seek to a random spot after the first line
					long randomSpot = 512 + (long)((channel.size()-512)*random.nextFloat());
					channel.position(randomSpot - bytesToOverwrite);
					ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
					byte[] junk = new byte[bytesToOverwrite];
					new Random().nextBytes(junk);
					buf.put(junk);
					buf.flip();
					channel.write(buf);
				}
			} catch (IOException e) {
				logger.warn(e);
				fail();
			}
		}
		
		Timestamp start = TimeUtils.convertFromISO8601String(currentYear + "-03-01T00:00:00.000Z");
		Timestamp end = TimeUtils.convertFromISO8601String(currentYear + "-04-01T00:00:00.000Z");
		try(BasicContext context = new BasicContext(); EventStream result = new CurrentThreadWorkerEventStream(pvName, pbplugin.getDataForPV(context, pvName, start, end))) {
			long eventCount = 0;
			for(@SuppressWarnings("unused") Event e : result) { 
				eventCount++;
			}
			int expectedCount = 31*24*12 + 1;  // 12 points per hour
			// There is really no right answer here. We should not lose too many points because of the zeroing.... 
			assertTrue("Event count is too low " + eventCount + " expecting approximately " + expectedCount, Math.abs(eventCount - expectedCount) < zeroedLines*3);
		} catch (IOException e) {
			logger.warn(e);
			fail();
		}
	}

}

