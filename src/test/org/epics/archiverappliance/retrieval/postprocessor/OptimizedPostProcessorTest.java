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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

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

    /**
     * Test for inclusion of last value before first bin into first bin
     * @throws Exception
     */
    @ParameterizedTest
    @MethodSource("testInclusionOfLastValueBeforeFirstBinIntoFirstBin_generateData")
    public void testInclusionOfLastValueBeforeFirstBinIntoFirstBin(int millisToAddToStart,
                                                                   int millisToAddToEnd,
                                                                   List<Double> expectedValues) {
        String optimizedTestPVName = "Test_OptimizedInclusionOfLastValueBeforeFirstBinIntoFirstBin";
        YearSecondTimestamp startOfSamples = TimeUtils.convertToYearSecondTimestamp(TimeUtils.convertFromISO8601String("2024-06-01T10:00:00.000Z"));
        ArrayListEventStream testData = new ArrayListEventStream(0,
                new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE,
                        optimizedTestPVName,
                        startOfSamples.getYear()));
        testData.add(new SimulationEvent(startOfSamples.getSecondsintoyear(),
                startOfSamples.getYear(),
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new ScalarValue<>(0.0)));

        testData.add(new SimulationEvent(startOfSamples.getSecondsintoyear() + 10,
                startOfSamples.getYear(),
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new ScalarValue<>(10.0)));

        testData.add(new SimulationEvent(startOfSamples.getSecondsintoyear() + 20,
                startOfSamples.getYear(),
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new ScalarValue<>(20.0)));

        testData.add(new SimulationEvent(startOfSamples.getSecondsintoyear() + 30,
                startOfSamples.getYear(),
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new ScalarValue<>(30.0)));

        testData.add(new SimulationEvent(startOfSamples.getSecondsintoyear() + 40,
                startOfSamples.getYear(),
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new ScalarValue<>(40.0)));

        testData.add(new SimulationEvent(startOfSamples.getSecondsintoyear() + 50,
                startOfSamples.getYear(),
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new ScalarValue<>(50.0)));

        Optimized optimizedPostProcessor = new Optimized();
        try {
            optimizedPostProcessor.initialize("optimized_9", optimizedTestPVName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Instant start = TimeUtils.convertFromYearSecondTimestamp(startOfSamples).plusMillis(millisToAddToStart);
        Instant end = TimeUtils.convertFromYearSecondTimestamp(startOfSamples).plusMillis(millisToAddToEnd);
        optimizedPostProcessor.estimateMemoryConsumption(optimizedTestPVName, new PVTypeInfo(optimizedTestPVName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1), start, end, null);
        var callableEventStream = CallableEventStream.makeOneStreamCallable(testData, null, false);
        try {
            optimizedPostProcessor.wrap(callableEventStream).call();
        } catch (Exception e) {
            Assertions.fail("An exception occurred when calling optimizedPostProcessor.wrap(callableEventStream).call()");
        }

        List<Event> events = new LinkedList<>();
        for (Event event : optimizedPostProcessor.getConsolidatedEventStream()) {
            events.add(event);
        }

        Assertions.assertEquals(expectedValues.size(), events.size());

        for (int i = 0; i < expectedValues.size(); i++) {
            double receivedValue = events.get(i).getSampleValue().getValue().doubleValue();
            double expectedValue = expectedValues.get(i);

            if (receivedValue != expectedValue) {
                Assertions.fail("Received value '" + events.get(i).getSampleValue().getValue().doubleValue() + "' at index " + i + " does not equal the expected value '" + expectedValues.get(i) + "'.");
            }
        }
    }

    static Stream<Arguments> testInclusionOfLastValueBeforeFirstBinIntoFirstBin_generateData() {
        return Stream.of(Arguments.of(-2, -1, Arrays.asList()),
                         Arguments.of(-2, 0, Arrays.asList(0.0)),
                         Arguments.of(-2, 1, Arrays.asList(0.0)),
                         Arguments.of(0, 9999, Arrays.asList(0.0)),
                         Arguments.of(29999, 29999, Arrays.asList(20.0)),
                         Arguments.of(29999, 30020, Arrays.asList(20.0, 30.0)),
                         Arguments.of(30000, 30020, Arrays.asList(30.0)),
                         Arguments.of(31000, 32000, Arrays.asList(30.0)),
                         Arguments.of(49999, 51000, Arrays.asList(40.0, 50.0)),
                         Arguments.of(50000, 51000, Arrays.asList(50.0)),
                         Arguments.of(51000, 52000, Arrays.asList(50.0)));
    }
}
