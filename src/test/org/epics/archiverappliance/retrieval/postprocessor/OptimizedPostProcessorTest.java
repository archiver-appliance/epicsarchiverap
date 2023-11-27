package org.epics.archiverappliance.retrieval.postprocessor;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.CallableEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.Optimized;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

/**
 * 
 * <code>OptimizedPostProcessorTest</code> tests the optimized post processor.
 *
 * @author <a href="mailto:jaka.bobnar@cosylab.com">Jaka Bobnar</a>
 *
 */
public class OptimizedPostProcessorTest {
        
    private String pvName = "Test";
    
    private ArrayListEventStream getData(short year) {
        int totSamples = 1*24*60; // days * hours/day * minutes/hour
        YearSecondTimestamp startOfSamples = TimeUtils.convertToYearSecondTimestamp(TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z"));
        ArrayListEventStream testData = new ArrayListEventStream(totSamples, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_INT, pvName, year));
        for(int s = 0; s < totSamples; s++) {
            testData.add(new SimulationEvent(startOfSamples.getSecondsintoyear() + s*60, year, ArchDBRTypes.DBR_SCALAR_INT, new ScalarValue<>(s)));
        }
        return testData;
        
    }

    /**
     * Test the return values if there are less points than requested by the caller.
     * @throws Exception
     */
    @Test
    public void testPPLessPointsThanRequested() throws Exception {      
        short year = (short)(TimeUtils.getCurrentYear()-1);
        YearSecondTimestamp startOfSamples = TimeUtils.convertToYearSecondTimestamp(TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z"));
        ArrayListEventStream testData = getData(year);

        Instant start = TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String(year + "-06-02T09:59:59.999Z");
        int expectedSamplesInPeriod = 24 * 60;
        PVTypeInfo pvTypeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        pvTypeInfo.setSamplingPeriod(60);
        
        Optimized optimizedPP = new Optimized();
        optimizedPP.initialize("optimized_10000", pvName);
        optimizedPP.estimateMemoryConsumption(pvName, pvTypeInfo, start, end, null);
        optimizedPP.wrap(CallableEventStream.makeOneStreamCallable(testData, null, false)).call();
        EventStream retData = optimizedPP.getConsolidatedEventStream();
        int eventCount = 0;
        Instant previousTimeStamp = TimeUtils.convertFromYearSecondTimestamp(startOfSamples);
        for(Event e : retData) {
            Instant eventTs = e.getEventTimeStamp();
            Assertions.assertTrue(eventTs.compareTo(previousTimeStamp) >= 0, "Event timestamp " + TimeUtils.convertToISO8601String(eventTs) + " is the same or after previous timestamp " + TimeUtils.convertToISO8601String(previousTimeStamp));
            Assertions.assertEquals(testData.get(eventCount).getSampleValue().getValue().doubleValue(), e.getSampleValue().getValue().doubleValue(), Double.MIN_VALUE, "Event value should match the n-th value in the test data");
            previousTimeStamp = eventTs;
            eventCount++;
        }
        Assertions.assertEquals(expectedSamplesInPeriod, eventCount, "The number of events should be the total number of events in time range");
    }
        
    /**
     * Test the return value if the number of available points is much higher than the requested number
     * @throws Exception
     */
    @Test
    public void testPPMorePointsThanRequested() throws Exception {      
        short year = (short)(TimeUtils.getCurrentYear()-1);
        YearSecondTimestamp startOfSamples = TimeUtils.convertToYearSecondTimestamp(TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z"));
        ArrayListEventStream testData = getData(year);

        Instant start = TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String(year + "-06-02T10:00:00.000Z");
        int expectedSamplesInPeriod = 161;
        PVTypeInfo pvTypeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        pvTypeInfo.setSamplingPeriod(60);
        
        Optimized optimizedPP = new Optimized();
        optimizedPP.initialize("optimized_160", pvName);
        optimizedPP.estimateMemoryConsumption(pvName, pvTypeInfo, start, end, null);
        optimizedPP.wrap(CallableEventStream.makeOneStreamCallable(testData, null, false)).call();
        EventStream retData = optimizedPP.getConsolidatedEventStream();
        int eventCount = 0;
        Instant previousTimeStamp = TimeUtils.convertFromYearSecondTimestamp(startOfSamples);
        for(Event e : retData) {
            Instant eventTs = e.getEventTimeStamp();
            if (eventCount != 0 && eventCount != 160) {
                //first and last bin are shifted
                Assertions.assertTrue(eventTs.compareTo(previousTimeStamp) >= 0, "Event timestamp " + TimeUtils.convertToISO8601String(eventTs) + " is the same or after previous timestamp " + TimeUtils.convertToISO8601String(previousTimeStamp));
                @SuppressWarnings("unchecked")
				VectorValue<Double> list = (VectorValue<Double>)e.getSampleValue();
                Assertions.assertEquals(5, list.getElementCount(), "There should be 5 numbers for each event");
                Assertions.assertEquals(9, list.getValue(4).intValue(), "Each bin should be composed from 9 values");
                double mean = ((eventCount-1)*9 + 7);
                Assertions.assertEquals(mean, list.getValue(0).doubleValue(), Double.MIN_VALUE, "Event mean value should match the calculated one");
                Assertions.assertEquals((eventCount-1)*9+3, list.getValue(2).doubleValue(), Double.MIN_VALUE, "Event min value should match the calculated one");
                Assertions.assertEquals((eventCount-1)*9+11, list.getValue(3).doubleValue(), Double.MIN_VALUE, "Event max value should match the calculated one");
                
                SummaryStatistics st = new SummaryStatistics();
                for (int i = 3; i < 12; i++) {
                    st.addValue(((eventCount-1)*9+i));
                }
                double std = st.getStandardDeviation();
                Assertions.assertEquals(std, list.getValue(1).doubleValue(), Double.MIN_VALUE, "Event std value should match the calculated one");
            }
            previousTimeStamp = eventTs;
            eventCount++;
        }
        Assertions.assertEquals(expectedSamplesInPeriod, eventCount, "The number of events should match the expected");
    }
}
