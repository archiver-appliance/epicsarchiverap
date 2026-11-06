package org.epics.archiverappliance.retrieval.postprocessor;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.retrieval.postprocessors.LTTBBin;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * <code>LTTBBinTest</code> exercises the selection rule on its own, with bins built by hand rather than fed
 * through the post processor, so that the triangle arithmetic and its edge cases can be pinned down directly.
 *
 * @author caraxlr
 */
public class LTTBBinTest {

    private static final String pvName = "Test_LTTB";
    private static final short year = (short) (TimeUtils.getCurrentYear() - 1);

    private static final YearSecondTimestamp startOfSamples =
            TimeUtils.convertToYearSecondTimestamp(TimeUtils.convertFromISO8601String(year + "-06-01T10:00:00.000Z"));

    /** A scalar double sample, secondsIn seconds after the start of the samples. */
    private static Event event(int secondsIn, double value) {
        return new SimulationEvent(
                startOfSamples.getSecondsintoyear() + secondsIn,
                year,
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new ScalarValue<>(value));
    }

    private static LTTBBin binOf(Event... events) {
        LTTBBin bin = new LTTBBin(pvName);
        for (Event e : events) {
            bin.addEvent(e);
        }
        return bin;
    }

    /** The sample furthest off the line from the anchor to the next bin's mean is the one returned. */
    @Test
    public void testPicksTheLargestTriangle() {
        Event anchor = event(0, 0.0);
        Event onTheLine = event(1, 1.0);
        Event theSpike = event(2, 50.0);
        Event alsoOnTheLine = event(3, 3.0);
        LTTBBin bin = binOf(onTheLine, theSpike, alsoOnTheLine);
        LTTBBin next = binOf(event(4, 4.0), event(5, 5.0));

        Assertions.assertSame(theSpike, bin.select(anchor, next, null, null));
    }

    /** A flat bin on the line still returns something; it cannot return nothing just because the area is zero. */
    @Test
    public void testPicksFromAFlatBin() {
        Event anchor = event(0, 0.0);
        LTTBBin bin = binOf(event(1, 0.0), event(2, 0.0));
        LTTBBin next = binOf(event(3, 0.0));

        Assertions.assertNotNull(bin.select(anchor, next, null, null));
    }

    /** Equal areas resolve to the later sample, matching the published algorithm. */
    @Test
    public void testTieGoesToTheLaterSample() {
        Event anchor = event(0, 0.0);
        Event earlier = event(1, 10.0);
        Event later = event(2, 20.0);
        // Both sit on a line of their own, so the two triangles have the same area.
        LTTBBin bin = binOf(earlier, later);
        LTTBBin next = binOf(event(3, 30.0));

        Assertions.assertSame(later, bin.select(anchor, next, null, null));
    }

    /**
     * The endpoints of the range are returned anyway, so a bin never picks them a second time. The last sample
     * falls in the final bin, which is the bin that passes itself as the lookahead.
     */
    @Test
    public void testFinalBinDoesNotPickTheLastSample() {
        Event reservedFirst = event(0, 0.0);
        Event ordinary = event(1, 1.0);
        Event theOtherOrdinary = event(2, 2.0);
        Event reservedLast = event(3, 100.0);
        LTTBBin bin = binOf(ordinary, theOtherOrdinary, reservedLast);

        // The last sample deviates most, so without the guard it would win.
        Assertions.assertSame(theOtherOrdinary, bin.select(reservedFirst, bin, reservedFirst, reservedLast));
    }

    /** A final bin holding nothing but the last sample of the range has nothing left to give. */
    @Test
    public void testFinalBinOfOnlyTheLastSampleReturnsNull() {
        Event reservedFirst = event(0, 0.0);
        Event anchor = event(1, 1.0);
        Event reservedLast = event(2, 2.0);
        LTTBBin bin = binOf(reservedLast);

        Assertions.assertNull(bin.select(anchor, bin, reservedFirst, reservedLast));
    }

    /** A NaN has no position, so it is never the sample a bin returns. */
    @Test
    public void testNaNIsNeverPicked() {
        Event anchor = event(0, 0.0);
        Event notANumber = event(1, Double.NaN);
        Event ordinary = event(2, 5.0);
        LTTBBin bin = binOf(notANumber, ordinary);
        LTTBBin next = binOf(event(3, 0.0));

        Assertions.assertSame(ordinary, bin.select(anchor, next, null, null));
    }

    /** A bin of nothing but NaN cannot contribute a sample. */
    @Test
    public void testAllNaNBinReturnsNull() {
        Event anchor = event(0, 0.0);
        LTTBBin bin = binOf(event(1, Double.NaN), event(2, Double.NaN));
        LTTBBin next = binOf(event(3, 1.0));

        Assertions.assertNull(bin.select(anchor, next, null, null));
        Assertions.assertEquals(0, bin.numValues());
    }

    /** A NaN anchor would make every area NaN, so the bin's own mean stands in for it. */
    @Test
    public void testNaNAnchorStillSelects() {
        // Only the first bin can see this, where the anchor is the first sample of the range, NaN or not.
        Event anchor = event(0, Double.NaN);
        Event theSpike = event(2, 50.0);
        LTTBBin bin = binOf(event(1, 1.0), theSpike, event(3, 2.0));
        LTTBBin next = binOf(event(4, 1.0));

        Assertions.assertSame(theSpike, bin.select(anchor, next, null, null));
    }

    /** A next bin that holds only NaN has no mean, so the anchor's value stands in as the third vertex. */
    @Test
    public void testNextBinWithoutValues() {
        Event anchor = event(0, 0.0);
        Event theSpike = event(2, 50.0);
        LTTBBin bin = binOf(event(1, 1.0), theSpike);
        LTTBBin next = binOf(event(3, Double.NaN));

        Assertions.assertSame(theSpike, bin.select(anchor, next, null, null));
    }

    /** The means skip the NaN samples rather than counting them as zero. */
    @Test
    public void testMeansIgnoreNaN() {
        LTTBBin bin = binOf(event(1, 10.0), event(2, Double.NaN), event(3, 20.0));

        Assertions.assertEquals(2, bin.numValues());
        Assertions.assertEquals(15.0, bin.meanY(), 1.0e-9);
        Assertions.assertEquals(LTTBBin.seconds(event(2, 0.0)), bin.meanX(), 1.0e-9);
    }

    /** Sub second spacing counts towards the x axis; two samples in the same second are not the same point. */
    @Test
    public void testSubSecondTimestampsAreDistinct() {
        Event first = event(1, 0.0);
        Event second = event(2, 0.0);

        Assertions.assertEquals(1.0, LTTBBin.seconds(second) - LTTBBin.seconds(first), 1.0e-9);
    }
}
