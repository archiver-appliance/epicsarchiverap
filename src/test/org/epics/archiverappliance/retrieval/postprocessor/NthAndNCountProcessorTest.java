package org.epics.archiverappliance.retrieval.postprocessor;

import java.sql.Timestamp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.CallableEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.NCount;
import org.epics.archiverappliance.retrieval.postprocessors.Nth;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


/**
 * 
 * <code>NthAndNCountProcessorTest</code> contains the unit tests for the {@link Nth} and
 * {@link NCount} post processors.
 *
 * @author <a href="mailto:jaka.bobnar@cosylab.com">Jaka Bobnar</a>
 *
 */
public class NthAndNCountProcessorTest {
	
	private static Logger logger = LogManager.getLogger(NthAndNCountProcessorTest.class.getName());
	private String pvName = "Test_NthNCount";
	
	
	/**
	 * Generates the sample data stream. The data is equally spaced one minute apart for 60 days,
	 * starting from 1st June at 10 am precisely. Some of the data in the array are duplicated to
	 * check that operators filters the data properly.
	 * 
	 * @param year the year for which the sample data is generated
	 * @return the generated data
	 */
	private ArrayListEventStream getData(short year) {
		int totSamples = 60*24*60; // days * hours/day * minutes/hour
		logger.info("Creating " + totSamples + " samples one minute apart");
		YearSecondTimestamp startOfSamples = TimeUtils.convertToYearSecondTimestamp(TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z"));
		ArrayListEventStream testData = new ArrayListEventStream(totSamples, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
		for(int s = 0; s < totSamples; s++) {
			testData.add(new SimulationEvent(startOfSamples.getSecondsintoyear() + s*60, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>((double) s)));
		}
		//add duplicates, which should be filtered out
		for(int s = 0; s < totSamples/10; s++) {
			testData.add(new SimulationEvent(startOfSamples.getSecondsintoyear() + s*60, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>((double) s)));
		}
		return testData;
		
	}

	/**
	 * Test Nth operator on the full time window.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testNthProcessorFullTimeRange() throws Exception {		
		short year = (short)(TimeUtils.getCurrentYear()-1);
		YearSecondTimestamp startOfSamples = TimeUtils.convertToYearSecondTimestamp(TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z"));
		ArrayListEventStream testData = getData(year);
				
		int[] sampling = new int[]{1,2,5,10,60};
		 
		Timestamp start = TimeUtils.convertFromISO8601String(year + "-05-01T10:00:00.000Z");
		Timestamp end   = TimeUtils.convertFromISO8601String(year + "-08-31T09:59:59.999Z");
		int expectedSamplesInPeriod = 60 * 24 * 60;
		PVTypeInfo pvTypeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		pvTypeInfo.setSamplingPeriod(60);
		
		for (int i = 0; i < sampling.length; i++) {
    		Nth nthProcessor = new Nth();
    		nthProcessor.initialize("nth_"+sampling[i], pvName);
    		nthProcessor.estimateMemoryConsumption(pvName, pvTypeInfo, start, end, null);
    		nthProcessor.wrap(CallableEventStream.makeOneStreamCallable(testData, null, false)).call();
    		EventStream retData = nthProcessor.getConsolidatedEventStream();
    		int eventCount = 0;
    		Timestamp previousTimeStamp = TimeUtils.convertFromYearSecondTimestamp(startOfSamples);
    		int n = 0;
    		for(Event e : retData) {
    			Timestamp eventTs = e.getEventTimeStamp();
    			Assertions.assertTrue(eventTs.compareTo(previousTimeStamp) >= 0, "Event timestamp " + TimeUtils.convertToISO8601String(eventTs) + " is the same or after previous timestamp " + TimeUtils.convertToISO8601String(previousTimeStamp));
    			Assertions.assertEquals(testData.get(n).getSampleValue().getValue().doubleValue(), e.getSampleValue().getValue().doubleValue(), Double.MIN_VALUE, "Event value should match the n-th value in the test data");
    			n+=sampling[i];
    			previousTimeStamp = eventTs;
    			eventCount++;
    		}
    		Assertions.assertEquals(expectedSamplesInPeriod/sampling[i], eventCount, "The number of events should be the total number of events in time range divided by sampling number");
		}
		
	}
	
	/**
	 * Test Nth operator with time window limited to 5 days.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testNthProcessorLimitedTimeRange() throws Exception {
		short year = (short)(TimeUtils.getCurrentYear()-1);
		YearSecondTimestamp startOfSamples = TimeUtils.convertToYearSecondTimestamp(TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z"));
		ArrayListEventStream testData = getData(year);
		
		int[] sampling = new int[]{1,2,5,10,60};
		
		Timestamp start = TimeUtils.convertFromISO8601String(year + "-06-05T10:00:00.000Z");
		Timestamp end   = TimeUtils.convertFromISO8601String(year + "-06-10T09:59:59.999Z");
		int expectedSamplesInPeriod = 5 * 24 * 60; //5 days of data
		PVTypeInfo pvTypeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		pvTypeInfo.setSamplingPeriod(60);
		
		for (int i = 0; i < sampling.length; i++) {
    		Nth nthProcessor = new Nth();
    		nthProcessor.initialize("nth_"+sampling[i], pvName);
    		nthProcessor.estimateMemoryConsumption(pvName, pvTypeInfo, start, end, null);
    		nthProcessor.wrap(CallableEventStream.makeOneStreamCallable(testData, null, false)).call();
    		EventStream retData = nthProcessor.getConsolidatedEventStream();
    		int eventCount = 0;
    		Timestamp previousTimeStamp = TimeUtils.convertFromYearSecondTimestamp(startOfSamples);
    		//starting with the n-th from the raw data array
    		int n = 4*24*60;
    		for(Event e : retData) {
    			Timestamp eventTs = e.getEventTimeStamp();
    			Assertions.assertTrue(eventTs.compareTo(previousTimeStamp) >= 0, "Event timestamp " + TimeUtils.convertToISO8601String(eventTs) + " is the same or after previous timestamp " + TimeUtils.convertToISO8601String(previousTimeStamp));
    			Assertions.assertEquals(testData.get(n).getSampleValue().getValue().doubleValue(), e.getSampleValue().getValue().doubleValue(), Double.MIN_VALUE, "Event value should match the n-th value in the test data");
    			n+=sampling[i];
    			previousTimeStamp = eventTs;
    			eventCount++;
    		}
    		Assertions.assertEquals(expectedSamplesInPeriod/sampling[i], eventCount, "The number of events should be the total number of events in the time range divided by sampling number");
		}
	}
		
	/**
	 * Test if large sets are truncated to the maximum size.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testNthProcessorTruncation() throws Exception {
		//create a data set larger than allowed
		short year = (short)(TimeUtils.getCurrentYear()-1);
		YearSecondTimestamp startOfSamples = TimeUtils.convertToYearSecondTimestamp(TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z"));
		ArrayListEventStream testData = new ArrayListEventStream(Nth.MAX_COUNT + 1, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
		for(int s = 0; s < Nth.MAX_COUNT + 1; s++) {
			testData.add(new SimulationEvent(startOfSamples.getSecondsintoyear() + s, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>((double) s)));
		}
		//retrieve data for the total time window
		Timestamp start = TimeUtils.convertFromISO8601String(year + "-06-01T00:00:00.000Z");
		Timestamp end   = TimeUtils.convertFromISO8601String(year + "-12-30T09:59:59.999Z");
		PVTypeInfo pvTypeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		pvTypeInfo.setSamplingPeriod(1);

		Nth nthProcessor = new Nth();
		nthProcessor.initialize("nth_1", pvName);
		nthProcessor.estimateMemoryConsumption(pvName, pvTypeInfo, start, end, null);
		nthProcessor.wrap(CallableEventStream.makeOneStreamCallable(testData, null, false)).call();
		EventStream retData = nthProcessor.getConsolidatedEventStream();
		int eventCount = 0;
		for(@SuppressWarnings("unused") Event e : retData) {
			eventCount++;
		}
		Assertions.assertEquals(Nth.MAX_COUNT, eventCount, "The number of events should be the truncated to the max allowed number of events");
	}
	
	/**
	 * Tests tha NCount processor properly counts the number of samples.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testNCountProcessor() throws Exception {
		short year = (short)(TimeUtils.getCurrentYear()-1);
		ArrayListEventStream testData = getData(year);
		
		Timestamp start = TimeUtils.convertFromISO8601String(year + "-06-05T10:00:00.000Z");
		for (int i = 2; i < 5; i++) {
    		Timestamp end   = TimeUtils.convertFromISO8601String(year + "-06-" + (i*5) + "T09:59:59.999Z");
    		int expectedSamplesInPeriod = (i-1)*5 * 24 * 60; //(i-1) * 5 days of data
    		PVTypeInfo pvTypeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
    		pvTypeInfo.setSamplingPeriod(60);
    		
    		NCount nCountProcessor = new NCount();
    		nCountProcessor.initialize("",pvName);
    		nCountProcessor.estimateMemoryConsumption(pvName,pvTypeInfo,start,end,null);
    		nCountProcessor.wrap(CallableEventStream.makeOneStreamCallable(testData, null, false)).call();
    		EventStream retData = nCountProcessor.getConsolidatedEventStream();
    		int eventCount = 0;
    		for(Event e : retData) {
    			Assertions.assertEquals(expectedSamplesInPeriod, e.getSampleValue().getValue().intValue(), "The returned value should be equal to the expected number of samples");
    			eventCount++;
    		}
    		
    		Assertions.assertEquals(1, eventCount, "Only one sample expected");
		}
		
	}
}
