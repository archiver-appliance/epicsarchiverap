package org.epics.archiverappliance.zipfs;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Timestamp;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SlowTests;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEventStreamIterator;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

@Category(SlowTests.class)
public class ZipETLTest {
	private static Logger logger = LogManager.getLogger(ZipETLTest.class.getName());
	File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ZipETLTest");
	private ConfigService configService;

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		if(testFolder.exists()) { 
			FileUtils.deleteDirectory(testFolder);
		}
		testFolder.mkdirs();		
	}

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(testFolder);
	}

	@Test
	public void testETLIntoZipPerPV() throws Exception {
		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":ETLZipTest";
		String srcRootFolder = testFolder.getAbsolutePath() + File.separator + "srcFiles";
		PlainPBStoragePlugin etlSrc = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=ZipETL&rootFolder=" + srcRootFolder + "&partitionGranularity=PARTITION_DAY", configService);
		logger.info(etlSrc.getURLRepresentation());
		ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
		int phasediffindegrees = 10;
		SimulationEventStream simstream = new SimulationEventStream(dbrType, new SineGenerator(phasediffindegrees));
		try(BasicContext context = new BasicContext()) {
			etlSrc.appendData(context, pvName, simstream);
		}
		
		String destRootFolder = testFolder.getAbsolutePath() + File.separator + "destFiles";
		PlainPBStoragePlugin etlDest = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=ZipETL&rootFolder=" + destRootFolder + "&partitionGranularity=PARTITION_DAY&compress=ZIP_PER_PV", configService);
		logger.info(etlDest.getURLRepresentation());
		
		PVTypeInfo typeInfo = new PVTypeInfo(pvName, dbrType, true, 1);
		String[] dataStores = new String[] { etlSrc.getURLRepresentation(), etlDest.getURLRepresentation() }; 
		typeInfo.setDataStores(dataStores);
		configService.updateTypeInfoForPV(pvName, typeInfo);
		configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
		configService.getETLLookup().manualControlForUnitTests();
		
		Timestamp timeETLruns = TimeUtils.now();
		DateTime ts = new DateTime(DateTimeZone.UTC);
		if(ts.getMonthOfYear() == 1) {
			// This means that we never test this in Jan but I'd rather have the null check than skip this.
			timeETLruns = TimeUtils.plusDays(timeETLruns, 35);
		}
		ETLExecutor.runETLs(configService, timeETLruns);
		logger.info("Done performing ETL");

		
		
		File expectedZipFile = new File(destRootFolder + File.separator + configService.getPVNameToKeyConverter().convertPVNameToKey(pvName) + "_pb.zip");
		assertTrue("Zip file does not seem to exist " + expectedZipFile, expectedZipFile.exists());

		logger.info("Testing retrieval for zip per pv");
		int eventCount = 0;
		try(BasicContext context = new BasicContext();
				EventStream strm = new CurrentThreadWorkerEventStream(pvName, etlSrc.getDataForPV(context, pvName, TimeUtils.getStartOfYear(TimeUtils.getCurrentYear()), TimeUtils.getEndOfYear(TimeUtils.getCurrentYear())))
				) {
			if(strm != null) {
				for(@SuppressWarnings("unused") Event ev : strm) {
					eventCount++;
				}
			}
		}
		try(BasicContext context = new BasicContext();
				EventStream strm = new CurrentThreadWorkerEventStream(pvName, etlDest.getDataForPV(context, pvName, TimeUtils.getStartOfYear(TimeUtils.getCurrentYear()), TimeUtils.getEndOfYear(TimeUtils.getCurrentYear())))
				) {
			for(@SuppressWarnings("unused") Event ev : strm) {
				eventCount++;
			}
		}
		logger.info("Got " + eventCount + " events");
		assertTrue("Retrieval does not seem to return any events " + eventCount, eventCount >= (SimulationEventStreamIterator.DEFAULT_NUMBER_OF_SAMPLES-1));		
	}
}