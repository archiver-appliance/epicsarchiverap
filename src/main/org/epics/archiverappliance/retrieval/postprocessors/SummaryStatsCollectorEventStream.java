package org.epics.archiverappliance.retrieval.postprocessors;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.ChangeInYearsException;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;
import org.epics.archiverappliance.retrieval.postprocessors.SummaryStatsPostProcessor.SummaryValue;

/**
 * An event stream over a list of SummaryStatsCollectors; typically used by post processors that return consolidated results.
 *
 * @author mshankar
 * @see PostProcessorWithConsolidatedEventStream
 */
public class SummaryStatsCollectorEventStream implements EventStream, RemotableOverRaw {
	private static Logger logger = Logger.getLogger(SummaryStatsCollectorEventStream.class.getName());
	private final RemotableEventStreamDesc desc;
	private LinkedHashMap<Long, SummaryValue> consolidatedData;
	private long firstBin;
	private long lastBin;
	private int intervalSecs;
	private boolean inheritValuesFromPreviousBins;
	private boolean zeroOutEmptyBins;
	private Iterator<Event> theOneAndOnlyIterator;
	private final boolean vectorType;
	private final ArchDBRTypes dbrType;
	public SummaryStatsCollectorEventStream(long firstBin, long lastBin, int intervalSecs, RemotableEventStreamDesc desc, LinkedHashMap<Long, SummaryValue> consolidatedData, boolean inheritValuesFromPreviousBins, boolean zeroOutEmptyBins, boolean vectorType, int elementCount) {
	    this.vectorType = vectorType;
		this.firstBin = firstBin;
		this.lastBin = lastBin;
		this.intervalSecs = intervalSecs;
		this.desc = new RemotableEventStreamDesc(desc);
		// Summaries of scalars are always double. That's what commons.math returns.
		this.dbrType = vectorType ? ArchDBRTypes.DBR_WAVEFORM_DOUBLE : ArchDBRTypes.DBR_SCALAR_DOUBLE;
		this.desc.setArchDBRType(dbrType);
		if (vectorType) {
		    this.desc.setElementCount(elementCount);
		}
		this.consolidatedData = consolidatedData;
		this.inheritValuesFromPreviousBins = inheritValuesFromPreviousBins;
		this.zeroOutEmptyBins = zeroOutEmptyBins;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public Iterator<Event> iterator() {
		if(theOneAndOnlyIterator != null) { 
			return theOneAndOnlyIterator;
		} else { 
			theOneAndOnlyIterator = new SummaryStatsCollectorEventStreamIterator(); 
			return theOneAndOnlyIterator;
		}
	}

	@Override
	public RemotableEventStreamDesc getDescription() {
		return desc;
	}

	private class SummaryStatsCollectorEventStreamIterator implements Iterator<Event> {
		ArrayListEventStream strm = new ArrayListEventStream(consolidatedData.size(), desc);
		short currentYear = -1;
		int currentEvent = 0;
		int totalEvents = -1;

		SummaryStatsCollectorEventStreamIterator() {
			SummaryValue summaryValue = null;
			boolean foundValue = false;
			int nanos = ((intervalSecs % 2) == 0) ? 0 : 500000000;
			
			if(consolidatedData.isEmpty()) { 
				logger.info("We not seem to have any events");
				totalEvents = 0;
				return;
			}
			
			Set<Long> bins = consolidatedData.keySet();
			if(firstBin == 0) { 
				firstBin = Collections.min(bins); 
			}
			if(lastBin == Long.MAX_VALUE) { 
				lastBin = Collections.max(bins);
			}
			
			for(long binNum = firstBin; binNum <= lastBin; binNum++) {
				if(consolidatedData.containsKey(binNum)) {
					summaryValue = consolidatedData.get(binNum);
					foundValue = true;
				} else { 
					if(inheritValuesFromPreviousBins) { 
						if(foundValue)  { 
							logger.debug("Inheriting previous value for bin " + binNum);
							if(SummaryStatsCollectorEventStream.this.zeroOutEmptyBins) { 
								summaryValue = new SummaryValue(0.0, 0, false);
							}
						}
					} else { 
						foundValue = false;
						logger.debug("Skipping inheriting previous value for bin " + binNum);
					}
				}
				if(foundValue) { 
					long epochSeconds = binNum*intervalSecs + intervalSecs/2;
					POJOEvent pojoEvent;
					if (vectorType) {
					    pojoEvent = new POJOEvent(dbrType,
                                TimeUtils.convertFromEpochSeconds(epochSeconds, nanos), 
                                new VectorValue<>(summaryValue.values), 
                                0, summaryValue.severity);
					} else {
    					pojoEvent = new POJOEvent(dbrType,
    							TimeUtils.convertFromEpochSeconds(epochSeconds, nanos), 
    							new ScalarValue<Double>(summaryValue.value), 
    							0, summaryValue.severity);
					}
					DBRTimeEvent pbevent = (DBRTimeEvent) pojoEvent.makeClone();
					if(summaryValue.connectionChanged) { 
						pbevent.addFieldValue("connectionChange", "true");
					}
					
					if(summaryValue.additionalCols != null && !summaryValue.additionalCols.isEmpty()) { 
						for(String addnName : summaryValue.additionalCols.keySet()) { 
							String addnValue = summaryValue.additionalCols.get(addnName);
							pbevent.addFieldValue(addnName, addnValue);
						}
					}
					
					strm.add(pbevent);
					if(currentYear == -1) { 
						// Initialize the current year as the year of the first bin with a value it it.
						currentYear = TimeUtils.computeYearForEpochSeconds(epochSeconds);
						SummaryStatsCollectorEventStream.this.desc.setYear(currentYear);
					}
				}
			}
			totalEvents = strm.size();
		}
		
		@Override
		public boolean hasNext() {
			return currentEvent < totalEvents;
		}

		@Override
		public Event next() {
			Event next = strm.get(currentEvent);
			short eventYear = TimeUtils.computeYearForEpochSeconds(next.getEpochSeconds());
			if(eventYear != currentYear) { 
				logger.info("Detected a change in years eventYear " + eventYear + " and currentYear is " + eventYear);
				SummaryStatsCollectorEventStream.this.desc.setYear(eventYear);
				short tempCurrentYear = currentYear;
				currentYear = eventYear;
				throw new ChangeInYearsException(tempCurrentYear, eventYear);
			}
			currentEvent++;
			return next;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		} 
	}
}

