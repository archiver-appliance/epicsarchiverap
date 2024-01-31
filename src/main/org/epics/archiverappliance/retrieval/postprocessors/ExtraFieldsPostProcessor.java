package org.epics.archiverappliance.retrieval.postprocessors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.Callable;

/**
 * A post processor that looks for fields
 * @author mshankar
 *
 */
public class ExtraFieldsPostProcessor implements PostProcessor {
	private static Logger logger = LogManager.getLogger(ExtraFieldsPostProcessor.class.getName());
	private static final String EXTRA_FIELDS = "extraFields";
	private String fieldName;
	
	public ExtraFieldsPostProcessor(String extraFieldName) {
		this.fieldName = extraFieldName;
		logger.debug("Extrafields processor for " + this.fieldName);
	}

	@Override
	public void initialize(String userarg, String pvName) throws IOException {
	}

	@Override
    public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) {
		// We do not expect the extra fields to change that much.
		// The engine adds an entry once a day and we use that to estimate
		int estimatedDays = 1 + ((int) (TimeUtils.convertToEpochSeconds(end) - TimeUtils.convertToEpochSeconds(start))/86400);
		float storageRate = typeInfo.getComputedStorageRate();
		// Add a fudge factor of 2 for java 
		long estimatedMemoryConsumption = (long) storageRate*estimatedDays*2;
		return estimatedMemoryConsumption;
	}

	@Override
	public Callable<EventStream> wrap(final Callable<EventStream> callable) {
		return new Callable<EventStream>() {
			@Override
			public EventStream call() throws Exception {
				try(EventStream strm = callable.call()) {
					ArrayListEventStream buf = new ArrayListEventStream(0, (RemotableEventStreamDesc) strm.getDescription());
					for(Event e : strm) {
						DBRTimeEvent tev = (DBRTimeEvent) e;
						// TODO Make sure we only return values that are actual changes.
						if(tev.hasFieldValues()) {
							HashMap<String, String> extraFields = tev.getFields();
							if(extraFields.containsKey(fieldName)) {
								POJOEvent newEvent = new POJOEvent(tev.getDBRType(), tev.getEventTimeStamp(), tev.getFieldValue(fieldName), tev.getStatus(), tev.getSeverity());
								buf.add(newEvent.makeClone());
							}
						}
					}
					return buf;
				}
			}
		};
	}

	@Override
	public String getIdentity() {
		return EXTRA_FIELDS;
	}

	@Override
	public String getExtension() {
		return EXTRA_FIELDS;
	}

}
