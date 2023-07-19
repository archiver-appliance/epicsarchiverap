package org.epics.archiverappliance.retrieval.postprocessors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfo;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Returns raw data for the previous two weeks and uses FirstSamplePP for the rest.
 * @author mshankar
 *
 */
public class TwoWeekRaw implements PostProcessor, TimeSpanDependentProcessing {
	public static final String TWOWEEK_IDENTITY = "twoweek";
	private FirstSamplePP sparsifiedData = new FirstSamplePP();
	private DefaultRawPostProcessor rawData = new DefaultRawPostProcessor();
	private static Logger logger = LogManager.getLogger(TwoWeekRaw.class.getName());
	private String pvName;

	@Override
	public void initialize(String userarg, String pvName) throws IOException {
		logger.debug("Initializing TwoWeekRaw for " + pvName);
		this.pvName = pvName;
		sparsifiedData.initialize(null, pvName);
		rawData.initialize(null, pvName);
	}

	@Override
	public List<TimeSpanDependentProcessor> generateTimeSpanDependentProcessors(List<TimeSpan> timeSpans) {
		long twoWeeksAgo = TimeUtils.convertToEpochSeconds(TimeUtils.now()) - 2 * 7 * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
		LinkedList<TimeSpanDependentProcessor> ret = new LinkedList<TimeSpanDependentProcessor>();
		for(TimeSpan timeSpan : timeSpans) { 
			long startSeconds =  TimeUtils.convertToEpochSeconds(timeSpan.getStartTime());
			long endSeconds = TimeUtils.convertToEpochSeconds(timeSpan.getEndTime());
			if(endSeconds <= twoWeeksAgo) {
				// Everything is sparsified
				logger.debug("Returning all sparsified data for pv " + pvName);
				ret.add(new TimeSpanDependentProcessor(timeSpan, sparsifiedData));
			} else if(startSeconds < twoWeeksAgo) {
				Instant twoWeeksAgoTs = TimeUtils.convertFromEpochSeconds(twoWeeksAgo, 0);
				logger.debug("Returning sparsified+raw data for pv " + pvName + " breaking at " + TimeUtils.convertToHumanReadableString(twoWeeksAgoTs));
				ret.add(new TimeSpanDependentProcessor(new TimeSpan(timeSpan.getStartTime(), twoWeeksAgoTs), sparsifiedData));
				ret.add(new TimeSpanDependentProcessor(new TimeSpan(twoWeeksAgoTs, timeSpan.getEndTime()), rawData));
			} else { 
				logger.debug("Returning all raw data for pv " + pvName);
				ret.add(new TimeSpanDependentProcessor(timeSpan, rawData));
			}
			
		}
		return ret;
	}

	@Override
	public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) {
		long twoWeeksAgo = TimeUtils.convertToEpochSeconds(TimeUtils.now()) - 2 * 7 * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
		long startSeconds =  TimeUtils.convertToEpochSeconds(start);
		long endSeconds = TimeUtils.convertToEpochSeconds(end);
		if(endSeconds <= twoWeeksAgo) {
			// Everything is sparsified
			return sparsifiedData.estimateMemoryConsumption(pvName, typeInfo, start, end, req);
		} else if(startSeconds < twoWeeksAgo) {
			Instant twoWeeksAgoTs = TimeUtils.convertFromEpochSeconds(twoWeeksAgo, 0);
			return sparsifiedData.estimateMemoryConsumption(pvName, typeInfo, start, twoWeeksAgoTs, req) 
					+ rawData.estimateMemoryConsumption(pvName, typeInfo, twoWeeksAgoTs, end, req);
		} else { 
			return rawData.estimateMemoryConsumption(pvName, typeInfo, start, end, req);
		}
	}

	@Override
	public Callable<EventStream> wrap(Callable<EventStream> callable) {
		return callable;
	}

	@Override
	public String getIdentity() {
		return TWOWEEK_IDENTITY;
	}

	@Override
	public String getExtension() {
		return TWOWEEK_IDENTITY;
	}

}
