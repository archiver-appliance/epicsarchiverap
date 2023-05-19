package org.epics.archiverappliance.retrieval;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.Mean;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessors;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

/**
 * Tests strategies for wrapping event streams
 * We create a couple of years worth of 1HZ DBR_DOUBLE data and then try to wrap Post Processors using various wrapping strategies.  
 * @author mshankar
 *
 */
public class EventStreamWrapTest {
	private static Logger logger = LogManager.getLogger(EventStreamWrapTest.class.getName());
	String shortTermFolderName=ConfigServiceForTests.getDefaultPBTestFolder()+"/EventStreamWrapTest";
	PlainPBStoragePlugin storageplugin;
	short currentYear = TimeUtils.getCurrentYear();
	ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
	private  ConfigServiceForTests configService;
	private String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX +  "S_" + type.getPrimitiveName();

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		if(new File(shortTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(shortTermFolderName));
		}
		new File(shortTermFolderName).mkdirs();
		
		storageplugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_MONTH", configService);

		for(short year : new short[] { ((short)(currentYear-1)), currentYear} ) {
			for(int day = 0; day < 365; day++) { 
				try(BasicContext context = new BasicContext()) {
					ArrayListEventStream testData = new ArrayListEventStream(86400, new RemotableEventStreamDesc(type, pvName, year));
					int startofdayinseconds = day*86400;
					for(int s = 0; s < 86400; s++) {
						testData.add(new SimulationEvent(startofdayinseconds + s, year, type, new ScalarValue<Double>(1.0)));
					}
					storageplugin.appendData(context, pvName, testData);
				}
			}
		}
	}

	@After
	public void tearDown() throws Exception {
		if(new File(shortTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(shortTermFolderName));
		}
	}

	@Test
	public void testWrappers() throws Exception {
		testSimpleWrapper();
		testMultiThreadWrapper();
	}
	
	
	private void testSimpleWrapper() throws Exception { 
		Timestamp end = TimeUtils.now();
		Timestamp start = TimeUtils.minusDays(end, 365);
		Mean mean_86400 = (Mean) PostProcessors.findPostProcessor("mean_86400");
		mean_86400.initialize("mean_86400", pvName);
		PVTypeInfo info = new PVTypeInfo();
		info.setComputedStorageRate(40);
		mean_86400.estimateMemoryConsumption(pvName, info, start, end, null);
		try(BasicContext context = new BasicContext()) {
			long t0 = System.currentTimeMillis();
			List<Callable<EventStream>> callables = storageplugin.getDataForPV(context, pvName, start, end, mean_86400);
			for(Callable<EventStream> callable : callables) { 
				callable.call();
			}
			long eventCount = 0;
			EventStream consolidatedEventStream = ((PostProcessorWithConsolidatedEventStream)mean_86400).getConsolidatedEventStream();
			// In cases where the data spans year boundaries, we continue with the same stream.
			boolean continueprocessing = true;
			while(continueprocessing) {
				try { 
					for(Event e : consolidatedEventStream) {
						assertTrue("All values are 1 so mean should be 1. Instead we got " + e.getSampleValue().getValue().doubleValue() + " at " + eventCount + " for pv " + pvName, e.getSampleValue().getValue().doubleValue() == 1.0);
						eventCount++;
					}
					continueprocessing = false;
				} catch(ChangeInYearsException ex) { 
					logger.debug("Change in years");
				}
			}
			long t1 = System.currentTimeMillis();
			// We get 365 or 366 events based on what now() is
			assertTrue("Expecting 366 values got " + eventCount + " for pv " + pvName, eventCount >= 365 && eventCount >= 366);
			logger.info("Simple wrapper took " + (t1-t0) + "(ms)");
		}
	}
	
	/**
	 * We wrap a thread around each source event stream. Since the source data is generated using month partitions, we should get about 12 source event streams..
	 * @throws Exception
	 */
	private void testMultiThreadWrapper() throws Exception {
		Timestamp end = TimeUtils.now();
		Timestamp start = TimeUtils.minusDays(end, 365);
		Mean mean_86400 = (Mean) PostProcessors.findPostProcessor("mean_86400");
		mean_86400.initialize("mean_86400", pvName);
		PVTypeInfo info = new PVTypeInfo();
		info.setComputedStorageRate(40);
		mean_86400.estimateMemoryConsumption(pvName, info, start, end, null);
		try(BasicContext context = new BasicContext()) {
			ExecutorService executors = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
			long t0 = System.currentTimeMillis();
			List<Callable<EventStream>> callables = storageplugin.getDataForPV(context, pvName, start, end, mean_86400);
			List<Future<EventStream>> futures = new ArrayList<Future<EventStream>>();
			for(Callable<EventStream> callable : callables) { 
				futures.add(executors.submit(callable));
			}
			
			for(Future<EventStream> future : futures) {
				try { 
					future.get();
				} catch(Exception ex) { 
					logger.error("Exception computing mean_86400", ex);
				}
			}
			
			long eventCount = 0;
			EventStream consolidatedEventStream = ((PostProcessorWithConsolidatedEventStream)mean_86400).getConsolidatedEventStream();
			// In cases where the data spans year boundaries, we continue with the same stream.
			boolean continueprocessing = true;
			while(continueprocessing) {
				try { 
					for(Event e : consolidatedEventStream) {
						assertTrue("All values are 1 so mean should be 1. Instead we got " + e.getSampleValue().getValue().doubleValue() + " at " + eventCount + " for pv " + pvName, e.getSampleValue().getValue().doubleValue() == 1.0);
						eventCount++;
					}
					continueprocessing = false;
				} catch(ChangeInYearsException ex) { 
					logger.debug("Change in years");
				}
			}
			long t1 = System.currentTimeMillis();
			executors.shutdown();
			// assertTrue("Expecting 365 values got " + eventCount + " for pv " + pvName, eventCount == 365);
			logger.info("Multi threaded wrapper took " + (t1-t0) + "(ms)");
		}
	}	
}
