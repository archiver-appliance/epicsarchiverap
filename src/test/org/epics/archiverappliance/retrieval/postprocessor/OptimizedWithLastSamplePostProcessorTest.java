package org.epics.archiverappliance.retrieval.postprocessor;

import edu.stanford.slac.archiverappliance.PB.data.PBScalarInt;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorValue;
import org.epics.archiverappliance.engine.ConnectionLossFields;
import org.epics.archiverappliance.retrieval.CallableEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.OptimizedWithLastSample;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

/**
 *
 * <code>OptimizedWithLastSamplePostProcessorTest</code> tests the OptimizedLastSample post processor.
 *
 * @author Rebecca Williams
 *
 */
public class OptimizedWithLastSamplePostProcessorTest {

    private String pvName = "Test";
    private long disconnectTime = 0;
    private long reconnectTime = 0;
    private short year = (short) (TimeUtils.getCurrentYear() - 1);

    /**
     * Generate test data. This can simulate periods of no data and
     * add the field data required to simulate a disconnection occurring
     * in the data.
     *
     * @param totSamples Total number of samples to simulate
     * @param simNoData Simulate periods of no data
     * @param startNoData Start of samples with no data
     * @param endNoData End of samples with no data
     * @param simDisconnect Simulate a disconnect
     * @param disconnectDurationMin Duration of the disconnect in seconds
     *
     * return ArrayListEventStream
     */
    private ArrayListEventStream getData(
            int totSamples,
            boolean simNoData,
            int startNoData,
            int endNoData,
            boolean simDisconnect,
            int disconnectDurationMin) {
        YearSecondTimestamp startOfSamples = TimeUtils.convertToYearSecondTimestamp(
                TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z"));
        ArrayListEventStream testData = new ArrayListEventStream(
                totSamples, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_INT, pvName, year));
        long yearInSecs = TimeUtils.convertFromISO8601String(year + "-01-01T00:00:00.000Z")
                .getEpochSecond();
        for (int s = 0; s < totSamples; s++) {
            int secsIntoYear = startOfSamples.getSecondsintoyear() + s * 60;
            SimulationEvent ev =
                    new SimulationEvent(secsIntoYear, year, ArchDBRTypes.DBR_SCALAR_INT, new ScalarValue<>(s));
            PBScalarInt pEv = new PBScalarInt(ev);
            // Simulate no data for a period of time, either because the
            // PV value has not changed or there has been a disconnect
            if (simNoData && s > startNoData && s < endNoData) continue;
            if (simDisconnect && s == endNoData) {
                // Simulate a reconnection at the end of a period with no data
                disconnectTime = yearInSecs + secsIntoYear - (60 * disconnectDurationMin);
                reconnectTime = yearInSecs + secsIntoYear;
                pEv.addFieldValue(
                        ConnectionLossFields.CNX_REGAINED_EPSECS.getFieldName(), String.valueOf(reconnectTime));
                pEv.addFieldValue(ConnectionLossFields.CNX_LOST_EPSECS.getFieldName(), String.valueOf(disconnectTime));
            }
            testData.add(pEv);
        }
        return testData;
    }

    /**
     * Helped method to determine if an Instant representation of a date
     * was after or on certain time
     * @param t1 Instant time to be compared.
     * @param t2 Instant time to compare against.
     *
     * return boolean
     */
    private boolean afterOrOn(Instant t1, Instant t2) {
        if (t1.isAfter(t2) || t1.equals(t2)) return true;
        else return false;
    }

    /**
     * Test helper method to check for the correct number of connectionChange flags in
     * the binned data.
     *
     * @param numSamples Number of data samples
     * @param expectedSamplesInPeriod Number of samples within timeframe requested
     * @param testData Data stream
     */
    public void checkConnectionChanges(int numSamples, int expectedSamplesInPeriod, ArrayListEventStream testData)
            throws Exception {
        Instant start = TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z");
        Instant end = start.plusSeconds(numSamples * 60);
        PVTypeInfo pvTypeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        pvTypeInfo.setSamplingPeriod(60);

        OptimizedWithLastSample optimizedPP = new OptimizedWithLastSample();
        optimizedPP.initialize("optimLastSample_" + expectedSamplesInPeriod, pvName);
        optimizedPP.estimateMemoryConsumption(pvName, pvTypeInfo, start, end, null);
        optimizedPP
                .wrap(CallableEventStream.makeOneStreamCallable(testData, null, false))
                .call();
        EventStream retData = optimizedPP.getConsolidatedEventStream();

        Instant disconnect = TimeUtils.convertFromEpochSeconds(disconnectTime, 0);
        Instant reconnect = TimeUtils.convertFromEpochSeconds(reconnectTime, 0);
        long binIntervalSecs = (end.getEpochSecond() - start.getEpochSecond()) / expectedSamplesInPeriod;
        long expectedNumConnectionChanges =
                (reconnect.getEpochSecond() - disconnect.getEpochSecond()) / binIntervalSecs + 1;
        if (reconnectTime == 0)
            // If the reconnect time is 0 then it was not set in the simulated
            // data meaning that there was no disconnect + reconnect.
            expectedNumConnectionChanges = 0;
        else if (disconnect.isBefore(start))
            // If the disconnect happened before the requested time frame
            expectedNumConnectionChanges = (reconnect.getEpochSecond() - start.getEpochSecond()) / binIntervalSecs;

        int eventCount = 0;
        int connectionChangeCount = 0;
        for (Event e : retData) {
            if (eventCount != 0 && eventCount != expectedSamplesInPeriod) {
                @SuppressWarnings("unchecked")
                VectorValue<Double> list = (VectorValue<Double>) e.getSampleValue();
                if (afterOrOn(e.getEventTimeStamp(), disconnect)
                        && e.getEventTimeStamp().isBefore(reconnect)) {
                    // Disconnected and expect no events
                    Assertions.assertTrue(
                            e.getProtobufMessage().toString().contains("connectionChange"),
                            "Expect connectionChange flag");
                    Assertions.assertEquals(0.0, list.getValue(4), "Bin should contain no events");
                    connectionChangeCount = connectionChangeCount + 1;
                } else if (e.getEventTimeStamp().isAfter(disconnect)
                        && e.getEventTimeStamp().isBefore(reconnect.plusSeconds(binIntervalSecs))) {
                    // Message where the reconnection was detected - will contain events
                    Assertions.assertTrue(
                            e.getProtobufMessage().toString().contains("connectionChange"),
                            "Expect connectionChange flag");
                    Assertions.assertNotEquals(0.0, list.getValue(4), "Number of events should not be 0");
                    connectionChangeCount = connectionChangeCount + 1;
                } else {
                    // Otherwise do not expect this flag
                    Assertions.assertFalse(
                            e.getProtobufMessage().toString().contains("connectionChange"),
                            "Do not expect the connectionChange flag");
                }
                Assertions.assertEquals(6, list.getElementCount(), "There should be 6 numbers for each event");
            }
            eventCount++;
        }
        Assertions.assertEquals(expectedNumConnectionChanges, connectionChangeCount);
    }

    @Test
    public void testDisconnectAfterPeriodOfNoEvents() throws Exception {
        int numSamples = 1000;
        int expectedSamplesInPeriod = 500;
        ArrayListEventStream testData = getData(numSamples, true, 500, 600, true, 20);
        checkConnectionChanges(numSamples, expectedSamplesInPeriod, testData);
    }

    @Test
    public void testDisconnectAfterEvents() throws Exception {
        int numSamples = 1000;
        int expectedSamplesInPeriod = 500;
        ArrayListEventStream testData = getData(numSamples, true, 500, 700, true, 198);
        checkConnectionChanges(numSamples, expectedSamplesInPeriod, testData);
    }

    @Test
    public void testReconnectionInLastEvent() throws Exception {
        int numSamples = 100;
        int expectedSamplesInPeriod = 50;
        ArrayListEventStream testData = getData(numSamples, true, 80, 99, true, 10);
        checkConnectionChanges(numSamples, expectedSamplesInPeriod, testData);
    }

    @Test
    public void testDisconnectBeforeTimeframeStart() throws Exception {
        int numSamples = 100;
        int expectedSamplesInPeriod = 50;
        ArrayListEventStream testData = getData(numSamples, true, 0, 50, true, 60);
        checkConnectionChanges(numSamples, expectedSamplesInPeriod, testData);
    }

    @Test
    public void testNoReconnect() throws Exception {
        int numSamples = 100;
        int expectedSamplesInPeriod = 50;
        ArrayListEventStream testData = getData(numSamples, true, 50, 100, true, 52);
        checkConnectionChanges(numSamples, expectedSamplesInPeriod, testData);
    }
}
