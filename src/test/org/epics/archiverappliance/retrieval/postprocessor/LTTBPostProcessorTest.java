package org.epics.archiverappliance.retrieval.postprocessor;

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
import org.epics.archiverappliance.retrieval.CallableEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.LTTB;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessors;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * <code>LTTBPostProcessorTest</code> tests the lttb post processor.
 *
 * lttb splits the requested range into as many time bins as there are requested points and keeps one real
 * archived sample from each, so the output count tracks the number of non empty bins rather than the requested
 * number exactly.
 *
 * @author caraxlr
 */
public class LTTBPostProcessorTest {

    private static final String pvName = "Test_LTTB";
    private static final short year = (short) (TimeUtils.getCurrentYear() - 1);

    private static final YearSecondTimestamp startOfSamples =
            TimeUtils.convertToYearSecondTimestamp(TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z"));

    /** A stream of scalar doubles, one per second, taking the values supplied. */
    private static ArrayListEventStream streamOf(double... values) {
        return streamAt(1.0, values);
    }

    /** A stream of scalar doubles spaced secondsApart seconds apart. */
    private static ArrayListEventStream streamAt(double secondsApart, double... values) {
        ArrayListEventStream testData = new ArrayListEventStream(
                values.length, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
        for (int s = 0; s < values.length; s++) {
            testData.add(new SimulationEvent(
                    startOfSamples.getSecondsintoyear() + (int) (s * secondsApart),
                    year,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    new ScalarValue<>(values[s])));
        }
        return testData;
    }

    private static List<Event> run(String userarg, ArrayListEventStream testData, int spanSeconds) throws Exception {
        LTTB lttb = new LTTB();
        lttb.initialize(userarg, pvName);
        Instant start = TimeUtils.convertFromYearSecondTimestamp(startOfSamples);
        lttb.estimateMemoryConsumption(
                pvName,
                new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1),
                start,
                start.plusSeconds(spanSeconds),
                null);
        lttb.wrap(CallableEventStream.makeOneStreamCallable(testData, null, false))
                .call();
        EventStream retData = lttb.getConsolidatedEventStream();
        List<Event> events = new ArrayList<>();
        for (Event e : retData) {
            events.add(e);
        }
        return events;
    }

    private static double[] valuesOf(List<Event> events) {
        double[] values = new double[events.size()];
        for (int i = 0; i < events.size(); i++) {
            values[i] = events.get(i).getSampleValue().getValue().doubleValue();
        }
        return values;
    }

    private static int secondsIntoYearOf(Event e) {
        return TimeUtils.convertToYearSecondTimestamp(e.getEventTimeStamp()).getSecondsintoyear();
    }

    @Test
    public void testFewerSamplesThanRequested() throws Exception {
        List<Event> events = run("lttb_100", streamOf(1.0, 2.0, 3.0, 4.0), 10);
        Assertions.assertArrayEquals(new double[] {1.0, 2.0, 3.0, 4.0}, valuesOf(events));
    }

    @Test
    public void testEndpointsAreAlwaysKept() throws Exception {
        List<Event> events = run("lttb_2", streamOf(1.0, 7.0, 2.0, 9.0, 4.0), 20);
        Assertions.assertEquals(1.0, events.get(0).getSampleValue().getValue().doubleValue());
        Assertions.assertEquals(
                4.0, events.get(events.size() - 1).getSampleValue().getValue().doubleValue());
    }

    @Test
    public void testConstantData() throws Exception {
        List<Event> events = run("lttb_3", streamOf(5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0), 24);
        Assertions.assertTrue(events.size() <= 8);
        for (double v : valuesOf(events)) {
            Assertions.assertEquals(5.0, v);
        }
    }

    @Test
    public void testMonotonicRampKeepsEndpointsAndIsOrdered() throws Exception {
        int n = 1000;
        double[] ramp = new double[n];
        for (int i = 0; i < n; i++) {
            ramp[i] = i;
        }
        // 1000 samples one second apart over a 1000s range asking for 50 points -> 20s bins.
        List<Event> events = run("lttb_50", streamOf(ramp), n);
        Assertions.assertEquals(52, events.size(), "One sample per bin, plus the two endpoints");
        Assertions.assertEquals(0.0, events.get(0).getSampleValue().getValue().doubleValue());
        Assertions.assertEquals(
                n - 1.0,
                events.get(events.size() - 1).getSampleValue().getValue().doubleValue());
        Instant previous = Instant.ofEpochMilli(0);
        for (Event e : events) {
            Assertions.assertTrue(e.getEventTimeStamp().isAfter(previous), "Events should be in time order");
            previous = e.getEventTimeStamp();
        }
    }

    /** The behaviour that distinguishes lttb from nth and mean - a lone spike survives. */
    @Test
    public void testSingleSampleSpikeIsRetained() throws Exception {
        int n = 1000;
        double[] flat = new double[n];
        flat[417] = 100.0;
        List<Event> events = run("lttb_20", streamOf(flat), n);
        boolean spikeFound = false;
        for (Event e : events) {
            if (e.getSampleValue().getValue().doubleValue() == 100.0) {
                spikeFound = true;
                Assertions.assertEquals(
                        startOfSamples.getSecondsintoyear() + 417,
                        secondsIntoYearOf(e),
                        "The spike should keep its original timestamp");
            }
        }
        Assertions.assertTrue(spikeFound, "The lone spike should survive downsampling");
    }

    /** All returned samples must be samples that were actually archived. */
    @Test
    public void testOutputIsASubsetOfTheInput() throws Exception {
        int n = 500;
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = Math.sin(i / 7.0) * 10 + i * 0.01;
        }
        ArrayListEventStream testData = streamOf(values);
        List<Event> events = run("lttb_40", testData, n);
        // 500s of samples in 12s bins, plus the two endpoints.
        Assertions.assertTrue(events.size() <= 46, "Got " + events.size() + " points");
        for (Event e : events) {
            boolean matched = false;
            for (Event raw : testData) {
                if (raw.getEventTimeStamp().equals(e.getEventTimeStamp())
                        && raw.getSampleValue().getValue().doubleValue()
                                == e.getSampleValue().getValue().doubleValue()) {
                    matched = true;
                    break;
                }
            }
            Assertions.assertTrue(matched, "Every returned sample should be an original archived sample");
        }
    }

    /**
     * Binning by time rather than by sample index is the point of this implementation: a burst of samples at the
     * start must not consume the whole point budget and starve the quiet remainder of the range. The trade off is
     * that the burst itself is reduced to one sample per bin however dense it is.
     */
    @Test
    public void testBurstDoesNotStarveTheQuietRange() throws Exception {
        // 600 samples in the first 600s, then 30 samples one per minute over the remaining 1800s.
        ArrayListEventStream testData = new ArrayListEventStream(
                630, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
        for (int i = 0; i < 600; i++) {
            testData.add(new SimulationEvent(
                    startOfSamples.getSecondsintoyear() + i,
                    year,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    new ScalarValue<>(Math.sin(i / 10.0))));
        }
        for (int i = 0; i < 30; i++) {
            testData.add(new SimulationEvent(
                    startOfSamples.getSecondsintoyear() + 600 + i * 60,
                    year,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    new ScalarValue<>((double) i)));
        }
        // 2400s range, 40 points -> 60s bins. The burst spans 10 bins, the quiet tail spans 30.
        List<Event> events = run("lttb_40", testData, 2400);
        int inQuietTail = 0;
        for (Event e : events) {
            if (secondsIntoYearOf(e) >= startOfSamples.getSecondsintoyear() + 600) {
                inQuietTail++;
            }
        }
        Assertions.assertTrue(
                inQuietTail >= 25, "The quiet tail should get about one point per bin, got " + inQuietTail);
        Instant previous = Instant.ofEpochMilli(0);
        for (Event e : events) {
            Assertions.assertTrue(e.getEventTimeStamp().isAfter(previous), "Events should be in time order");
            previous = e.getEventTimeStamp();
        }
    }

    @Test
    public void testNaNsAreNotSelected() throws Exception {
        int n = 300;
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = (i % 3 == 0) ? Double.NaN : i;
        }
        // Keep the endpoints real; they are always returned.
        values[0] = 0.0;
        values[n - 1] = n - 1.0;
        for (double v : valuesOf(run("lttb_30", streamOf(values), n))) {
            Assertions.assertFalse(Double.isNaN(v), "A NaN should never be selected");
        }
    }

    /** A NaN first sample is returned as it was archived, and the bins select as they would without it. */
    @Test
    public void testNaNFirstSampleStillSelects() throws Exception {
        int n = 100;
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = Math.sin(i / 5.0) * 10;
        }
        values[0] = Double.NaN;
        List<Event> events = run("lttb_10", streamOf(values), n);
        Assertions.assertEquals(12, events.size(), "One sample per bin, plus the two endpoints");
        Assertions.assertTrue(
                Double.isNaN(events.get(0).getSampleValue().getValue().doubleValue()),
                "The first sample is returned as archived, NaN and all");
        for (int i = 1; i < events.size(); i++) {
            Assertions.assertFalse(
                    Double.isNaN(events.get(i).getSampleValue().getValue().doubleValue()),
                    "Only the reserved endpoint may be a NaN");
        }
    }

    @Test
    public void testWaveformsArePassedThrough() throws Exception {
        ArrayListEventStream testData = new ArrayListEventStream(
                10, new RemotableEventStreamDesc(ArchDBRTypes.DBR_WAVEFORM_DOUBLE, pvName, year));
        for (int s = 0; s < 10; s++) {
            testData.add(new SimulationEvent(
                    startOfSamples.getSecondsintoyear() + s,
                    year,
                    ArchDBRTypes.DBR_WAVEFORM_DOUBLE,
                    new VectorValue<>(List.of((double) s, s + 1.0, s + 2.0))));
        }
        Assertions.assertEquals(10, run("lttb_3", testData, 10).size());
    }

    @Test
    public void testRegistryLookup() throws Exception {
        PostProcessor pp = PostProcessors.findPostProcessor("lttb_500");
        Assertions.assertInstanceOf(LTTB.class, pp);
        pp.initialize("lttb_500", pvName);
        Assertions.assertEquals("lttb_500", pp.getExtension());

        PostProcessor bare = PostProcessors.findPostProcessor("lttb");
        Assertions.assertInstanceOf(LTTB.class, bare);
        bare.initialize("lttb", pvName);
        Assertions.assertEquals("lttb", bare.getExtension());

        // The registry matches on startsWith in registration order, so lttb captures any later lttb* identity.
        Assertions.assertInstanceOf(LTTB.class, PostProcessors.findPostProcessor("lttbSample_500"));
    }
}
