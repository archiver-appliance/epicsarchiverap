package org.epics.archiverappliance.retrieval.postprocessors;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * 
 * <code>Optimized</code> expects one parameter at initialization, which is the number of requested points.
 * If there are less samples in the time interval than requested (with a certain deadband), all samples 
 * will be returned. If there are more samples than requested, the samples will be collected into bins.
 * Mean, std, min, max and count of each bin is calculated and returned as a single sample. 
 *
 * @author <a href="mailto:jaka.bobnar@cosylab.com">Jaka Bobnar</a>
 *
 */
public class Optimized implements PostProcessor, PostProcessorWithConsolidatedEventStream, FillNoFillSupport {

    private static final Logger Logger = LogManager.getLogger(Optimized.class);
    private static final int DEFAULT_NUMBER_OF_POINTS = 1000;
    private static final String IDENTITY = "optimized";
    
    private int numEvents;
    private ArrayListEventStream allEvents;
    private ArrayListEventStream transformedRawEvents;
    private int numberOfPoints = DEFAULT_NUMBER_OF_POINTS;
    
    private final Statistics statisticsPostProcessor = new Statistics(){
        @Override
        public SummaryStatsVectorCollector getCollector() {
            return new SummaryStatsVectorCollector() {
                private SummaryStatistics stats = new SummaryStatistics();
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
                    if (numEvents < numberOfPoints) {
                        allEvents.add(e);
                    }
                    numEvents++;
                    double val = e.getSampleValue().getValue().doubleValue();
                    if(!Double.isNaN(val)) { 
                        stats.addValue(val);
                    } else { 
                        Logger.warn("Skipping NAN");
                    }
                }
            };
        }   
    };    
        
    @Override
    public void initialize(String userarg, String pvName) throws IOException {
        if(userarg != null && userarg.contains("_")) {
            String[] userparams = userarg.split("_");
            String numberStr = userparams[1];
            numberOfPoints = Integer.parseInt(numberStr);
        } 
        numEvents = 0;
    }
    
    @Override
    public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) {
        int intervalSecs = (int) ((end.toEpochMilli() - start.toEpochMilli()) / (1000 * numberOfPoints));
        try {
            statisticsPostProcessor.initialize(getIdentity() + "_" + Integer.toString(intervalSecs),pvName);
        } catch (IOException e) {
            Logger.error("Error initializing the optimized post processor.",e);
        }
        return statisticsPostProcessor.estimateMemoryConsumption(pvName,typeInfo,start,end,req);
    }
    
    @Override
    public void doNotInheritValuesFromPrevioisBins() {
        statisticsPostProcessor.doNotInheritValuesFromPrevioisBins();
    }

    @Override
    public boolean zeroOutEmptyBins() {
        return statisticsPostProcessor.zeroOutEmptyBins();
    }

    @Override
    public EventStream getConsolidatedEventStream() {
        if (numEvents > allEvents.size()) {
            return statisticsPostProcessor.getConsolidatedEventStream();
        } else {
            return new ArrayListCollectorEventStream(transformedRawEvents);
        }
    }

    @Override
    public long getStartBinEpochSeconds() {
        return statisticsPostProcessor.getStartBinEpochSeconds();
    }

    @Override
    public long getEndBinEpochSeconds() {
        return statisticsPostProcessor.getEndBinEpochSeconds();
    }

    @Override
    public LinkedList<TimeSpan> getBinTimestamps() {
        return statisticsPostProcessor.getBinTimestamps();
    }

    @Override
    public String getIdentity() {
        return IDENTITY;
    }

    @Override
    public String getExtension() {
        if(numberOfPoints == DEFAULT_NUMBER_OF_POINTS) {
            return getIdentity();
        } else {
            return getIdentity() + "_" + Integer.toString(numberOfPoints);
        }
    }

    @Override
    public Callable<EventStream> wrap(final Callable<EventStream> callable) {
        return new Callable<EventStream>() {
            public EventStream call() throws Exception {
                if (allEvents == null) {
                    EventStream strm = callable.call();
                    RemotableEventStreamDesc org = (RemotableEventStreamDesc)strm.getDescription();
                    RemotableEventStreamDesc desc = new RemotableEventStreamDesc(org);
                    allEvents = new ArrayListEventStream(numberOfPoints, desc);
                }
                Callable<EventStream> stCall = statisticsPostProcessor.wrap(callable);
                EventStream stream = stCall.call();
                if (numEvents > allEvents.size()) {
                    return stream;
                } else {
                    transformedRawEvents = new ArrayListEventStream(allEvents.size(),allEvents.getDescription());
                    for (Event e : allEvents) {
                        transformedRawEvents.add(DBR2PBTypeMapping.getPBClassFor(e.getDBRType()).getSerializingConstructor().newInstance(e));
                    }
                    return transformedRawEvents;
                }
            }
        };
    }  
}
