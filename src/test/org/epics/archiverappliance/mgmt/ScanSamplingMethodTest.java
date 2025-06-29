package org.epics.archiverappliance.mgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.PVCaPut;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.retrieval.client.RetrievalEventProcessor;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test the SCAN sampling method. These are the test cases
 * <ol>
 * <li>PVs changing at 10Hz/1Hz/0.1Hz; we archive at SCAN/1.0. We should get a sample every second.</li>
 * <li>Second test is to change a PV rapidly and then stop. Wait a bit and then get the data; we should get the final value that was set and not something that was set earlier. 
 * </ol>
 * @author mshankar
 *
 */
@Tag("integration")@Tag("localEpics")
public class ScanSamplingMethodTest {
	private static Logger logger = LogManager.getLogger(ScanSamplingMethodTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();

	@BeforeEach
	public void setUp() throws Exception {
		siocSetup.startSIOCWithDefaultDB();
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
	}

	@AfterEach
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();
	}

	@Test
	public void testScanPV() throws Exception {
        String[] pvs = {"ArchUnitTest:counter10Hz", "ArchUnitTest:counter1Hz", "ArchUnitTest:counter1By10thHz", "ArchUnitTest:manual"};
        List<JSONObject> arSpecs = List.of(pvs).stream().map((x) -> new JSONObject(Map.of("pv", x, "samplingmethod", "SCAN", "samplingperiod", "1.0"))).collect(Collectors.toList());
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        logger.info("Archiving PVs SCAN/1.0");
        GetUrlContent.postDataAndGetContentAsJSONArray(mgmtURL + "archivePV", GetUrlContent.from(arSpecs));
        for(String pv : pvs) {
            PVAccessUtil.waitForStatusChange(pv, "Being archived", 10, mgmtURL, 15);
        }

		Thread.sleep(60*1000);
		double lastValue = rapidlyChangeManualPV(pvs[3]);
		Thread.sleep(20*1000);

		testDataRetrieval(pvs[0], 60, 1100,  false);
		testDataRetrieval(pvs[1], 60, 1100,  true);
		testDataRetrieval(pvs[2], 8,  10100, true);
		
		testLastSampleOfManualPV(pvs[3], lastValue);
		
	}

	private void testDataRetrieval(String pvName, int expectedCount, long expectedGapBetweenSamples, boolean consecutiveValuesExpected) {
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
        Instant end = TimeUtils.now();
        Instant start = TimeUtils.minusHours(end, 1);

		EventStream stream = null;
		try {
			stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, start, end, new RetrievalEventProcessor() {
				@Override
				public void newPVOnStream(EventStreamDesc desc) {
					logger.info("Getting data for PV " + desc.getPvName());
				}
			});

			// We are making sure that the stream we get back has a sample every second.
			long eventCount = 0;
			if(stream != null) {
				long previousEventMillis = -1;
				long previousValue = -1;
				for(Event e : stream) {
                    long currentMillis = e.getEventTimeStamp().toEpochMilli();
					Assertions.assertTrue(previousEventMillis == -1 || ((currentMillis - previousEventMillis) <= expectedGapBetweenSamples), "Gap between samples " + (currentMillis - previousEventMillis) + " is more than expected " + expectedGapBetweenSamples + " for PV " + pvName);
					previousEventMillis = currentMillis;
					eventCount++;
					if(consecutiveValuesExpected) { 
						long currentValue = e.getSampleValue().getValue().longValue();
						Assertions.assertTrue(previousValue == -1 || (currentValue == (previousValue + 1)), "We expect not to miss any value. Current " + currentValue + " and previous " + previousValue + " for pv " + pvName);
						previousValue = currentValue;
					}
				}
			}
			Assertions.assertTrue(eventCount >= expectedCount, "Event count is not what we expect. We got " + eventCount + " and we expected at least " + expectedCount + " for pv " + pvName);
		} finally {
			if(stream != null) try { stream.close(); stream = null; } catch(Throwable t) { }
		}
	}	
	
	
	private double rapidlyChangeManualPV(String pvName) throws Exception {
		double lastValue = -1000.0;
		new PVCaPut().caPut(pvName, 1.0);
		Thread.sleep(2000);
		new PVCaPut().caPutValues(pvName, new double[] { 1.1, 1.2, 1.3, lastValue}, 100);
		return lastValue;
	}
	
	
	private void testLastSampleOfManualPV(String pvName, double lastValue) {
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
        Instant end = TimeUtils.plusHours(TimeUtils.now(), 10);
        Instant start = TimeUtils.minusHours(end, 10);

		EventStream stream = null;
		try {
			stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, start, end, new RetrievalEventProcessor() {
				@Override
				public void newPVOnStream(EventStreamDesc desc) {
					logger.info("Getting data for PV " + desc.getPvName());
				}
			});

			// We want to make sure that the last sample we get is what we expect.
			long eventCount = 0;
			if(stream != null) {
				double eventValue = 0.0;
				for(Event e : stream) {
					eventValue = e.getSampleValue().getValue().doubleValue();
					eventCount++;
				}
				Assertions.assertTrue(eventValue == lastValue, "We expected the last value to be " + lastValue + ". Instead it is " + eventValue);
			}
			Assertions.assertTrue(eventCount >= 1, "Event count is not what we expect. We got " + eventCount + " and we expected at least one event");
		} finally {
			if(stream != null) try { stream.close(); stream = null; } catch(Throwable t) { }
		}
	}	

}