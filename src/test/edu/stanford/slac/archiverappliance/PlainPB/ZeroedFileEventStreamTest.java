package edu.stanford.slac.archiverappliance.PlainPB;

import static org.junit.Assert.assertTrue;

import java.io.File;
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
	private static Logger logger = LogManager.getLogger(ZeroedFileEventStreamTest.class.getName());
	String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ZeroedFileEventStreamTestTest/";
	File rootFolder = new File(rootFolderName);
	static String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ZeroedFileEventStreamTestTest";
	PlainPBStoragePlugin pbplugin;
	static short currentYear = TimeUtils.getCurrentYear();
	private ConfigService configService;
	static ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		pbplugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + rootFolderName + "&partitionGranularity=PARTITION_YEAR", configService);
	}
	
	private static void generateFreshData(PlainPBStoragePlugin pbplugin4data) throws Exception { 
		File rootFolder = new File(pbplugin4data.getRootFolder());
		if(rootFolder.exists()) { 
			FileUtils.deleteDirectory(rootFolder);
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
		}
	}
	

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(new File(rootFolderName));
		FileUtils.deleteDirectory(new File(rootFolderName + "Dest"));
	}

	/**
	 * Generate PB file with bad footers and then see if we survive PBFileInfo.
	 * @throws Exception
	 */
	@Test
	public void testBadFooters() throws Exception {
		logger.info("Testing garbage in the last record");
		generateFreshData(pbplugin);
		Path[] paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolderName, pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
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
			}

			PBFileInfo info = new PBFileInfo(path);
			assertTrue("Cannot generate PBFileInfo from " + path, info != null);
			assertTrue("pvNames are different " + info.getPVName() + " expecting " + pvName, info.getPVName().equals(pvName));
			assertTrue("Last event is null", info.getLastEvent() != null);
			Timestamp lastEventTs = info.getLastEvent().getEventTimeStamp();
			logger.info(TimeUtils.convertToHumanReadableString(lastEventTs));
			assertTrue("Last event is incorrect " + TimeUtils.convertToHumanReadableString(lastEventTs), lastEventTs.after(TimeUtils.convertFromISO8601String(currentYear + "-12-30T00:00:00.000Z")) && lastEventTs.before(TimeUtils.convertFromISO8601String(currentYear+1 + "-01-01T00:00:00.000Z")));
			try(FileBackedPBEventStream strm = new FileBackedPBEventStream(pvName, path, type)) {
				long eventCount = 0;
				for(@SuppressWarnings("unused") Event e : strm) { 
					eventCount++;
				}
				assertTrue("Event count is too low " + eventCount, eventCount > 365);
			}
		}
	}

	
	/**
	 * Generate PB file with bad footers in the ETL source and then see if we survive ETL
	 * @throws Exception
	 */
	@Test
	public void testBadFootersInSrcETL() throws Exception {
		PlainPBStoragePlugin srcPlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + rootFolderName + "&partitionGranularity=PARTITION_MONTH", configService);
		generateFreshData(srcPlugin);
		Path[] paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolderName, pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
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
			}
		}
		
		PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		String[] dataStores = new String[] { srcPlugin.getURLRepresentation(), "pb://localhost?name=STS&rootFolder=" + rootFolderName + "Dest" + "&partitionGranularity=PARTITION_YEAR" }; 
		typeInfo.setDataStores(dataStores);
		configService.updateTypeInfoForPV(pvName, typeInfo);
		configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
		configService.getETLLookup().manualControlForUnitTests();

		Timestamp timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 366);
		DateTime ts = new DateTime(DateTimeZone.UTC);
		if(ts.getMonthOfYear() == 1) {
			// This means that we never test this in Jan but I'd rather have the null check than skip this. 
			timeETLruns = TimeUtils.plusDays(timeETLruns, 35);
		}
		ETLExecutor.runETLs(configService, timeETLruns);
		logger.info("Done performing ETL");
		
		paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolderName + "Dest", pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		}
		
		assertTrue("ETL did not seem to move any data?", paths != null && paths.length > 0);

		long eventCount = 0;
		for(Path path : paths) { 
			PBFileInfo info = new PBFileInfo(path);
			assertTrue("Cannot generate PBFileInfo from " + path, info != null);
			assertTrue("pvNames are different " + info.getPVName() + " expecting " + pvName, info.getPVName().equals(pvName));
			assertTrue("Last event is null", info.getLastEvent() != null);
			Timestamp lastEventTs = info.getLastEvent().getEventTimeStamp();
			logger.info(TimeUtils.convertToHumanReadableString(lastEventTs));
			assertTrue("Last event is incorrect " + TimeUtils.convertToHumanReadableString(lastEventTs), lastEventTs.after(TimeUtils.convertFromISO8601String(currentYear + "-12-30T00:00:00.000Z")) && lastEventTs.before(TimeUtils.convertFromISO8601String(currentYear+1 + "-01-01T00:00:00.000Z")));
			try(FileBackedPBEventStream strm = new FileBackedPBEventStream(pvName, path, type)) {
				for(@SuppressWarnings("unused") Event e : strm) { 
					eventCount++;
				}
			}
		}
		int expectedEventCount = 360*24*12;
		assertTrue("Event count is too low " + eventCount + " expecting at least " + expectedEventCount, eventCount >= expectedEventCount);
	}
	
	
	/**
	 * Generate PB file with bad footers in the ETL dest and then see if we survive ETL
	 * @throws Exception
	 */
	@Test
	public void testBadFootersInDestETL() throws Exception {
		PlainPBStoragePlugin destPlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + rootFolderName + "Dest" + "&partitionGranularity=PARTITION_YEAR", configService);
		File destFolder = new File(destPlugin.getRootFolder());
		if(destFolder.exists()) { 
			FileUtils.deleteDirectory(destFolder);
		}

		PlainPBStoragePlugin srcPlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + rootFolderName + "&partitionGranularity=PARTITION_MONTH", configService);
		File srcFolder = new File(srcPlugin.getRootFolder());
		if(srcFolder.exists()) { 
			FileUtils.deleteDirectory(srcFolder);
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
		}
		
		Path[] paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), destPlugin.getRootFolder(), pvName, ".pb", destPlugin.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
		}
		
		assertTrue("Cannot seem to find any plain pb files in " + destPlugin.getRootFolder() + " for pv " + pvName, paths != null && paths.length > 0);
		
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
		}

		PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		String[] dataStores = new String[] { srcPlugin.getURLRepresentation(), destPlugin.getURLRepresentation() }; 
		typeInfo.setDataStores(dataStores);
		configService.updateTypeInfoForPV(pvName, typeInfo);
		configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
		configService.getETLLookup().manualControlForUnitTests();

		Timestamp timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 366);
		DateTime ts = new DateTime(DateTimeZone.UTC);
		if(ts.getMonthOfYear() == 1) {
			// This means that we never test this in Jan but I'd rather have the null check than skip this. 
			timeETLruns = TimeUtils.plusDays(timeETLruns, 35);
		}
		ETLExecutor.runETLs(configService, timeETLruns);
		logger.info("Done performing ETL");
		
		paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), destPlugin.getRootFolder(), pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
		}
		
		assertTrue("ETL did not seem to move any data?", paths != null && paths.length > 0);

		long eventCount = 0;
		for(Path path : paths) { 
			PBFileInfo info = new PBFileInfo(path);
			assertTrue("Cannot generate PBFileInfo from " + path, info != null);
			assertTrue("pvNames are different " + info.getPVName() + " expecting " + pvName, info.getPVName().equals(pvName));
			assertTrue("Last event is null", info.getLastEvent() != null);
			Timestamp lastEventTs = info.getLastEvent().getEventTimeStamp();
			logger.info(TimeUtils.convertToHumanReadableString(lastEventTs));
			assertTrue("Last event is incorrect " + TimeUtils.convertToHumanReadableString(lastEventTs), lastEventTs.after(TimeUtils.convertFromISO8601String(currentYear + "-12-30T00:00:00.000Z")) && lastEventTs.before(TimeUtils.convertFromISO8601String(currentYear+1 + "-01-01T00:00:00.000Z")));
			try(FileBackedPBEventStream strm = new FileBackedPBEventStream(pvName, path, type)) {
				for(@SuppressWarnings("unused") Event e : strm) { 
					eventCount++;
				}
			}
		}
		int expectedEventCount = 360*24*12;
		assertTrue("Event count is too low " + eventCount + " expecting at least " + expectedEventCount, eventCount >= expectedEventCount);
	}

	
	/**
	 * Generate PB file with bad footers and then see if we survive retrieval
	 * @throws Exception
	 */
	@Test
	public void testBadFootersRetrieval() throws Exception {
		generateFreshData(pbplugin);
		Path[] paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolderName, pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
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
			assertTrue("Event count is too low " + eventCount + " expecting " + expectedCount, eventCount == expectedCount);
		}
	}
	
	
	/**
	 * Generate PB file with zeroes at random places and then see if we survive retrieval
	 * @throws Exception
	 */
	@Test
	public void testZeroedDataRetrieval() throws Exception {
		generateFreshData(pbplugin);
		Path[] paths = null;
		try(BasicContext context = new BasicContext()) {
			 paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolderName, pvName, ".pb", PartitionGranularity.PARTITION_YEAR, CompressionMode.NONE, configService.getPVNameToKeyConverter());
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
		}
	}

}

