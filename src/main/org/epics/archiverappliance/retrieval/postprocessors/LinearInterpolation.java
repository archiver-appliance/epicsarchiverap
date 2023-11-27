package org.epics.archiverappliance.retrieval.postprocessors;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;

import java.time.Instant;
import java.util.LinkedList;

/**
 * Implements the arithmetic mean across an interval
 * @author mshankar
 *
 */
public class LinearInterpolation extends SummaryStatsPostProcessor implements PostProcessor {
	private static Logger logger = LogManager.getLogger(LinearInterpolation.class.getName());
	static final String IDENTITY = "linear";
	
	private class InterpolationValues { 
		double x;
		double y;
		public InterpolationValues(double x, double y) {
			this.x = x;
			this.y = y;
		}
	}

	@Override
	public String getIdentity() {
		return IDENTITY;
	}

	@Override
	public SummaryStatsCollector getCollector() {
		return new SummaryStatsCollector() {
			long binNum;
			int intervalSecs;
			
			LinkedList<InterpolationValues> vals = new LinkedList<InterpolationValues>();
			@Override
			public void setBinParams(int intervalSecs, long binNum) {
				this.intervalSecs = intervalSecs;
				this.binNum = binNum;
			}
			
			@Override
			public boolean haveEventsBeenAdded() {
				return vals.size() > 0;
			}
			
			@Override
			public double getStat() {
				int size = vals.size();
				double[] xa = new double[size];
				double[] ya = new double[size];
				for(int i = 0; i < size; i++) {
					InterpolationValues iv = vals.get(i);
					xa[i] = iv.x;
					ya[i] = iv.y;
				}
				try { 
					PolynomialSplineFunction fn = new LinearInterpolator().interpolate(xa, ya);
					return fn.value(binNum*intervalSecs + intervalSecs/2);
				} catch(Exception ex) { 
					logger.debug("Exception when computing value; note that this may be perfectly acceptable; so we return the first value ", ex);
					return ya[0];
				}
			}
			
			@Override
			public void addEvent(Event e) {
                Instant ts = e.getEventTimeStamp();
                double time = e.getEpochSeconds() + ts.getNano() / 1000000000.0;
				vals.add(new InterpolationValues(time, e.getSampleValue().getValue().doubleValue()));
			}
		};
	}
}
