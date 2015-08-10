package org.epics.archiverappliance.retrieval.postprocessors;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;

/**
 * 
 * <code>Statistics</code> is a post processor which provides a set of statistical numbers for a specific bin.
 * The order of parameters is: mean, std, min, max, count.
 *
 * @author <a href="mailto:jaka.bobnar@cosylab.com">Jaka Bobnar</a>
 *
 */
public class Statistics extends SummaryStatsPostProcessor {

    public static final String IDENTITY = "stats";
    private static Logger logger = Logger.getLogger(Statistics.class.getName());
    
    @Override
    public String getIdentity() {
        return IDENTITY;
    }
    
    @Override
    public int getElementCount() {
        return 5;
    }
    
    @Override
    public boolean isProvidingVectorData() {
        return true;
    }

    @Override
    public SummaryStatsVectorCollector getCollector() {
        return new SummaryStatsVectorCollector() {
            SummaryStatistics stats = new SummaryStatistics();
            @Override
            public void setBinParams(int intervalSecs, long binNum) {
            }
            
            @Override
            public boolean haveEventsBeenAdded() {
                return stats.getN() > 0;
            }
            
            @Override
            public List<Double> getVectorValues() {
                List<Double> list = new ArrayList<>(5);
                list.add(stats.getMean());
                list.add(stats.getStandardDeviation());
                list.add(stats.getMin());
                list.add(stats.getMax());
                list.add((double)stats.getN());
                return list;
            }
            
            @Override
            public double getStat() {
                return Double.NaN;
            }
                        
            @Override
            public void addEvent(Event e) {
                double val = e.getSampleValue().getValue().doubleValue();
                if(!Double.isNaN(val)) { 
                    stats.addValue(val);
                } else { 
                    logger.warn("Skipping NAN");
                }
            }
        };
    }   
}
