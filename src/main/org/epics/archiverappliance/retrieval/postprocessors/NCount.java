package org.epics.archiverappliance.retrieval.postprocessors;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

/**
 * 
 * <code>NCount</code> is a post processor which returns number of samples in 
 * a selected time span.
 *
 * @author <a href="mailto:jaka.bobnar@cosylab.com">Jaka Bobnar</a>
 *
 */
public class NCount implements PostProcessor, PostProcessorWithConsolidatedEventStream {
	
	private static final String IDENTITY = "ncount";
	
	private static Logger logger = Logger.getLogger(NCount.class.getName());
	
	private long startTime;
	private long endTime;
	private ArrayListEventStream data;
	private int count = 0;
	private boolean countAddedToStream = false;
	private String pvName;
	
	@Override
	public String getIdentity() {
		return IDENTITY;
	}

	@Override
	public String getExtension() {
		return this.getIdentity();
	}

	@Override
	public void initialize(String userarg, String pvName) throws IOException {
		this.pvName = pvName;
	}

	@Override
	public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Timestamp start, Timestamp end, HttpServletRequest req) {
		this.startTime = start.getTime();
		this.endTime = end.getTime();
		return (long) typeInfo.getComputedStorageRate();
	}

	@Override
	public Callable<EventStream> wrap(final Callable<EventStream> callable) {
		return new Callable<EventStream>() {
			@Override
			public EventStream call() throws Exception {
				Timestamp previousEventTimestamp = new Timestamp(1);
				try(EventStream strm = callable.call()) {
					RemotableEventStreamDesc org = (RemotableEventStreamDesc)strm.getDescription();
					RemotableEventStreamDesc desc = new RemotableEventStreamDesc(org);
					desc.setArchDBRType(ArchDBRTypes.DBR_SCALAR_INT);
					if(data == null) { 
						data = new ArrayListEventStream(1,desc);
					}
					for(Event e : strm) {
						if(e.getEventTimeStamp().after(previousEventTimestamp)) { 
							previousEventTimestamp = e.getEventTimeStamp();
						} else {
							if(logger.isDebugEnabled()) { 
								logger.debug("Skipping older event " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()) + " previous " + TimeUtils.convertToHumanReadableString(previousEventTimestamp));
							}
							continue;
						}
						long s = e.getEventTimeStamp().getTime();
						if (s < startTime || s > endTime) {
							logger.debug("Skipping event that is out of selected boundaries. Time: " + TimeUtils.convertToHumanReadableString(s));
						} else {
    						count++;
						}
					}
					return new ArrayListEventStream(0,desc);
				}
			}
		};
	}	
	
	@Override
	public LinkedList<TimeSpan> getBinTimestamps() {
		LinkedList<TimeSpan> list = new LinkedList<>();
		if (data == null) return list;
		for (int i = 0; i < data.size() - 1; i++) {
			list.add(new TimeSpan(data.get(i).getEventTimeStamp(),data.get(i+1).getEventTimeStamp()));
		}
		return list;
	}
	
	@Override
	public EventStream getConsolidatedEventStream() {
		if(!countAddedToStream) { 
			if(data == null) { 
				RemotableEventStreamDesc desc = new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_INT, pvName, TimeUtils.computeYearForEpochSeconds(startTime/1000));				
				data = new ArrayListEventStream(1,desc);
			}
			data.add(new POJOEvent(ArchDBRTypes.DBR_SCALAR_INT,new Timestamp(startTime),new ScalarValue<>(count),0,0));
			countAddedToStream = true;
		}
		return data;
	}
	
	@Override
	public long getEndBinEpochSeconds() {
		return TimeUtils.convertToEpochSeconds(new Timestamp(endTime));
	}
	
	@Override
	public long getStartBinEpochSeconds() {
		return TimeUtils.convertToEpochSeconds(new Timestamp(startTime));
	}
}
