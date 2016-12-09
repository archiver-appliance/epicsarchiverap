package org.epics.archiverappliance.etl;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Path;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;

/**
 * Unit test to test for timezones that are ahead of UTC; use "Australia/Sydney".
 * We generate a few partitions worth of data and then see that data is moving over as time goes by.
 * @author mshankar
 *
 */
public class TImezoneAheadTest {
	private static Logger logger = Logger.getLogger(TImezoneAheadTest.class.getName());

	@Before
	public void setUp() throws Exception {
		System.getProperties().put("user.timezone", "Australia/Sydney");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testETLMove() throws Exception {
		testETLMoveForPartitionGranularity(PartitionGranularity.PARTITION_HOUR, PartitionGranularity.PARTITION_DAY);
		testETLMoveForPartitionGranularity(PartitionGranularity.PARTITION_DAY, PartitionGranularity.PARTITION_DAY);
	}
	
	private void testETLMoveForPartitionGranularity(PartitionGranularity srcGranularity, PartitionGranularity destGranularity) throws Exception { 
		logger.debug(TimeUtils.convertToHumanReadableString(TimeUtils.now()));
		
		PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin();
		PBCommonSetup srcSetup = new PBCommonSetup();
		PlainPBStoragePlugin etlDest = new PlainPBStoragePlugin();
		PBCommonSetup destSetup = new PBCommonSetup();
		DefaultConfigService configService = new ConfigServiceForTests(new File("./bin"), 1);

		srcSetup.setUpRootFolder(etlSrc, "TimeZoneAheadETLTestSrc_"+srcGranularity, srcGranularity);
		destSetup.setUpRootFolder(etlDest, "TimeZoneAheadETLTestDest"+srcGranularity, destGranularity);
		
		long nowEpochSeconds = TimeUtils.getCurrentEpochSeconds();
		long startEpochSeconds =  nowEpochSeconds - 10*srcGranularity.getApproxSecondsPerChunk();
		long endEpochSeconds = nowEpochSeconds + 10*srcGranularity.getApproxSecondsPerChunk();
		
		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testTimeZoneAhead" + etlSrc.getPartitionGranularity();

		PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		String[] dataStores = new String[] { etlSrc.getURLRepresentation(), etlDest.getURLRepresentation() }; 
		typeInfo.setDataStores(dataStores);
		configService.updateTypeInfoForPV(pvName, typeInfo);
		configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
		configService.getETLLookup().manualControlForUnitTests();

		{
			long eventSeconds = startEpochSeconds;
			while(eventSeconds <= endEpochSeconds) {
				int eventsPerShot = 10*1024;
				ArrayListEventStream instream = new ArrayListEventStream(eventsPerShot, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.computeYearForEpochSeconds(eventSeconds)));
				for(int i = 0; i < eventsPerShot; i++) {
					instream.add(new SimulationEvent(TimeUtils.convertToYearSecondTimestamp(eventSeconds), ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Number>(eventSeconds)));
					eventSeconds++;
				}
				try(BasicContext context = new BasicContext()) {
					etlSrc.appendData(context, pvName, instream);
				}				
			}
		}
		
		{

			long eventSeconds = startEpochSeconds + srcGranularity.getApproxSecondsPerChunk();
			while(eventSeconds <= endEpochSeconds) {
				Path[] srcPathsBefore = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), etlSrc.getRootFolder(), pvName, ".pb", etlSrc.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
				Path[] destPathsBefore = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), etlDest.getRootFolder(), pvName, ".pb", etlDest.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
				long srcBeforeEpochSeconds = -1;
				
				if(srcPathsBefore.length > 0) { 
					srcBeforeEpochSeconds = (new PBFileInfo(srcPathsBefore[0])).getFirstEventEpochSeconds();
				}

				ETLExecutor.runETLs(configService, TimeUtils.convertFromEpochSeconds(eventSeconds, 0));

				Path[] srcPathsAfter = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), etlSrc.getRootFolder(), pvName, ".pb", etlSrc.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
				Path[] destPathsAfter = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), etlDest.getRootFolder(), pvName, ".pb", etlDest.getPartitionGranularity(), CompressionMode.NONE, configService.getPVNameToKeyConverter());
				
				logger.info("Running ETL at " + TimeUtils.convertToHumanReadableString(eventSeconds) 
				+ " Before " + srcPathsBefore.length + "/" + destPathsBefore.length
				+ " After " + srcPathsAfter.length + "/" + destPathsAfter.length);
				
				
				long srcAfterEpochSeconds = -1;
				
				if(srcPathsAfter.length > 0) { 
					srcAfterEpochSeconds = (new PBFileInfo(srcPathsAfter[0])).getFirstEventEpochSeconds();
				}
				
				if(srcAfterEpochSeconds > 0 && srcBeforeEpochSeconds > 0) { 
					assertTrue("The first event in the source after ETL "
							+ TimeUtils.convertToHumanReadableString(srcAfterEpochSeconds)
							+ " should be greater then the first event in the source before ETL"
							+ TimeUtils.convertToHumanReadableString(srcBeforeEpochSeconds), srcAfterEpochSeconds > srcBeforeEpochSeconds);
				} else { 
					logger.warn("ETL did not move data at " + TimeUtils.convertToHumanReadableString(eventSeconds) );
				}

				eventSeconds = eventSeconds + srcGranularity.getApproxSecondsPerChunk();
			}
		}
		srcSetup.deleteTestFolder();
		destSetup.deleteTestFolder();
	}

}
