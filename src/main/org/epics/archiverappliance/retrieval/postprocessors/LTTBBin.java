package org.epics.archiverappliance.retrieval.postprocessors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;

import java.util.ArrayList;

/**
 * One time bin of the Largest Triangle Three Buckets downsampling.
 *
 * <p>Input: every sample whose timestamp falls in the bin, added one at a time with {@link #addEvent(Event)},
 * plus, at selection time, the sample already picked for the previous bin and the next bin, of which only
 * the mean is used.
 *
 * <p>Algorithm: each candidate in the bin forms a triangle with the previously picked sample and the mean
 * (x, y) of the next bin; the candidate with the largest triangle area wins, later sample on a tie. NaN values
 * are left out of the mean and are never picked.
 *
 * <p>Output: that single sample, or null if the bin holds nothing pickable.
 *
 * @author caraxlr
 */
public class LTTBBin {

    private static final Logger logger = LogManager.getLogger(LTTBBin.class.getName());

    final ArrayList<Event> samples = new ArrayList<Event>();

    /** The sums are over the samples that carry a value, so that a NaN does not poison the mean. */
    private double sumX = 0.0;

    private double sumY = 0.0;
    private int numValues = 0;

    private final String pvName;

    public LTTBBin(String pvName) {
        this.pvName = pvName;
    }

    public void addEvent(Event e) {
        samples.add(e);
        double val = value(e);
        if (!Double.isNaN(val)) {
            sumX += seconds(e);
            sumY += val;
            numValues++;
        } else if (logger.isDebugEnabled()) {
            // A mostly NaN pv would flood the log, so this is not a warning.
            logger.debug("Skipping NAN for pv " + pvName);
        }
    }

    /** The number of samples in the bin that carry a value rather than a NaN. */
    public int numValues() {
        return numValues;
    }

    /** The mean of the bin, as the third vertex of the triangle. */
    public double meanX() {
        return numValues > 0 ? sumX / numValues : seconds(samples.get(0));
    }

    public double meanY() {
        return numValues > 0 ? sumY / numValues : 0.0;
    }

    /**
     * The sample this bin returns: of the samples in the bin, the one forming the largest triangle with the
     * anchor and the mean of the next bin. Null if the bin holds nothing that can be picked.
     *
     * @param anchorSample the sample returned for the previous bin, the first vertex of the triangle
     * @param next the bin after this one, whose mean is the third vertex; the last bin passes itself
     * @param reservedFirst the first sample of the range, which is returned anyway and so is never picked here
     * @param reservedLast the last sample of the range, likewise
     */
    public Event select(Event anchorSample, LTTBBin next, Event reservedFirst, Event reservedLast) {
        double ax = seconds(anchorSample);
        double ay = value(anchorSample);
        if (Double.isNaN(ay)) {
            // A NaN anchor makes every area NaN, so nothing is ever picked and the anchor never moves on.
            ay = meanY();
        }
        double avgX = next.meanX();
        double avgY = next.numValues > 0 ? next.meanY() : ay;
        double largestArea = -1;
        Event picked = null;
        for (Event e : samples) {
            if (e == reservedFirst || e == reservedLast) {
                continue;
            }
            double by = value(e);
            if (Double.isNaN(by)) {
                continue;
            }
            double area = Math.abs((seconds(e) - ax) * (avgY - ay) - (avgX - ax) * (by - ay));
            if (area >= largestArea) {
                // Prefer the later sample in case of a tie
                largestArea = area;
                picked = e;
            }
        }
        return picked;
    }

    /** Seconds since the epoch, with the sub second part, so that irregular sampling is handled correctly. */
    public static double seconds(Event e) {
        return e.getEpochSeconds() + e.getEventTimeStamp().getNano() / 1.0e9;
    }

    public static double value(Event e) {
        return e.getSampleValue().getValue().doubleValue();
    }
}
