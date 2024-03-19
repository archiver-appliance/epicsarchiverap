package org.epics.archiverappliance.retrieval.extrafields;

import edu.stanford.slac.archiverappliance.PB.data.PBScalarDouble;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;
import java.util.HashMap;

/**
 * Generate data for a year with some extra fields sprinkled and test retrieval to make sure we can get at these extra fields..
 * @author mshankar
 *
 */
@Tag("integration")
public class GetExtraFieldsTest {
	private static Logger logger = LogManager.getLogger(GetExtraFieldsTest.class.getName());
	File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder());
	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":GetExtraFieldsTest";
	TomcatSetup tomcatSetup = new TomcatSetup();
	private ConfigService configService;

	@BeforeEach
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(-1);
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
	}

	@AfterEach
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
	}

	@Test
	public void testGetExtraFields() throws Exception {
		short currentYear = TimeUtils.getCurrentYear();
		PlainPBStoragePlugin pbplugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + testFolder + "&partitionGranularity=PARTITION_YEAR", configService);
		File pbFile = new File(testFolder + File.separator +configService.getPVNameToKeyConverter().convertPVNameToKey(pvName));
		if(pbFile.exists()) pbFile.delete();
		logger.info("Generating data for extra fields");
		for(int day = 0; day < 365; day++) {
			try(BasicContext context = new BasicContext()) {
				int startOfDay = day * 86400;
				ArrayListEventStream stream = new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));
				for(int second = 0; second < 86400; second++) {
					PBScalarDouble event = new PBScalarDouble(new SimulationEvent(startOfDay + second,currentYear, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>((double)second)));
					if(second == 0) {
						HashMap<String, String> extraFields = new HashMap<String, String>();
						extraFields.put("HIHI", Double.toString(10.0));
						extraFields.put("LOLO", Double.toString(-10.0));
						event.setFieldValues(extraFields, true);
					}
					stream.add(event);
				}
				pbplugin.appendData(context, pvName, stream);
			}
		}
		
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
        Instant start = TimeUtils.convertFromISO8601String(currentYear + "-02-01T08:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String(currentYear + "-02-02T08:00:00.000Z");
		
		
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName + ".HIHI" }, start, end, null)) {
			long previousEpochSeconds = 0;
			int eventCount = 0;
			int expectedCount = 1;

			// We are making sure that the stream we get back has times in sequential order...
			if(stream != null) {
				for(Event e : stream) {
					long actualSeconds = e.getEpochSeconds();
					logger.info("Event at " + TimeUtils.convertToISO8601String(actualSeconds));
					Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
					previousEpochSeconds = actualSeconds;
					eventCount++;
				}
			}
			
			Assertions.assertTrue(eventCount == expectedCount, "Event count is not what we expect. We got " + eventCount + " and we expected " + expectedCount + " for year " + currentYear);
		}
		
		start = TimeUtils.convertFromISO8601String(currentYear + "-02-01T08:00:00.000Z");
		end = TimeUtils.convertFromISO8601String(currentYear + "-10-01T08:00:00.000Z");
		long st0 = System.currentTimeMillis();
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName + ".HIHI" }, start, end, null)) {
			long previousEpochSeconds = 0;
			int eventCount = 0;
			int expectedCount = 200;

			// We are making sure that the stream we get back has times in sequential order...
			if(stream != null) {
				for(Event e : stream) {
					long actualSeconds = e.getEpochSeconds();
					Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
					previousEpochSeconds = actualSeconds;
					eventCount++;
				}
			}
			long st1 = System.currentTimeMillis();
			logger.info("Time taken to retrieve " + eventCount + " events " + (st1-st0) + "ms");
			
			Assertions.assertTrue(eventCount > expectedCount, "Event count is not what we expect. We got " + eventCount + " and we expected at least " + expectedCount + " for year " + currentYear);
		}
	}	
}
