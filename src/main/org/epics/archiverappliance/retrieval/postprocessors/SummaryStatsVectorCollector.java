package org.epics.archiverappliance.retrieval.postprocessors;

import java.util.List;

/**
 * 
 * <code>SummaryStatsCombinedCollector</code> is a collector, which provides a set of numbers 
 * (statistical results) rather than just a single value. 
 *
 * @author <a href="mailto:jaka.bobnar@cosylab.com">Jaka Bobnar</a>
 *
 */
public interface SummaryStatsVectorCollector extends SummaryStatsCollector {
    /**
     * @return the list of values in the order specific to the post processor
     */
    List<Double> getVectorValues();
}
