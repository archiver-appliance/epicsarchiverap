package org.epics.archiverappliance.retrieval.postprocessors;

import edu.stanford.slac.archiverappliance.PB.data.PBParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Callable;

/**
 * The intent is to mimic ADEL; this is principally targeted at decimation
 * Like ADEL, it applies only to scalar numbers.
 * We have some notion of a "last" value that was written out.
 * The very first sample we get we write out and that becomes the "last" value.
 * <pre>
 * Diff = Math.abs(current value - last value)
 * If Diff &ge; ADEL
 *     Write out value
 *     Update last known value
 * </pre>
 * <ol>
 * <li>If either current value or previous value is NAN/INF, unconditionally write out current value and update last known value</li>
 * <li>Also need to to write out when alarm severity changes (MD)</li>
 * <li>Also need to to write out when connection state changes (connect/disconnect) (MD)</li>
 * </ol>
 * @author mshankar
 *
 */
public class DeadBand implements PostProcessor {
	private static Logger logger = LogManager.getLogger(DeadBand.class.getName());
	private static final double DEFAULT_ADEL=1.0;
	private double adelValue = DEFAULT_ADEL;
	private Event lastEventWrittenOut = null;

	
	@Override
	public void initialize(String userarg, String pvName) throws IOException {
		if(userarg != null && userarg.contains("_")) {
			String[] userparams = userarg.split("_");
			String intervalStr = userparams[1];
			adelValue = Double.parseDouble(intervalStr);
			logger.debug("DeadBand, using user supplied ADEL of " + adelValue);
		} else {
			logger.debug("DeadBand, using the default ADEL of  " + adelValue);
		}
	}

	@Override
    public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) {
		float storageRate = typeInfo.getComputedStorageRate();
		long numSeconds = TimeUtils.convertToEpochSeconds(end) - TimeUtils.convertToEpochSeconds(start);
		// Add a fudge factor of 2 for java 
		long estimatedMemoryConsumption = (long) (storageRate*numSeconds*2);
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
						try { 
							// The very first sample we get we write out and that becomes the "last" value.
							if(lastEventWrittenOut == null) {
								Event clone = e.makeClone();
								lastEventWrittenOut = clone;
								buf.add(clone);
								logger.debug("Writing the very first sample out at " + TimeUtils.convertToISO8601String(clone.getEventTimeStamp()));
								continue;
							}
							
							double currentVALue = e.getSampleValue().getValue().doubleValue();
							double lastVALueWrittenOut = lastEventWrittenOut.getSampleValue().getValue().doubleValue();
							
							if(Double.isInfinite(currentVALue) || Double.isNaN(currentVALue) 
									|| Double.isInfinite(lastVALueWrittenOut) || Double.isNaN(lastVALueWrittenOut)
									) {
								// If either current value or previous value is NAN/INF, unconditionally write out current value and update last known value 
								Event clone = e.makeClone();
								lastEventWrittenOut = clone;
								buf.add(clone);
								logger.debug("Writing out from infinity/NAN at " + TimeUtils.convertToISO8601String(clone.getEventTimeStamp()));
								continue;
							}
							
							DBRTimeEvent currDBRTimeEvent = (DBRTimeEvent)e;
							int currentSeverity = currDBRTimeEvent.getSeverity();
							DBRTimeEvent previousDBRTimeEvent = (DBRTimeEvent)lastEventWrittenOut;
							int previousSeverity = previousDBRTimeEvent.getSeverity();
							if(currentSeverity != previousSeverity) {
								// If alarm severity changes, write out current value and update last known value 
								Event clone = e.makeClone();
								lastEventWrittenOut = clone;
								buf.add(clone);
								logger.debug("Writing out from severity change at " + TimeUtils.convertToISO8601String(clone.getEventTimeStamp()));
								continue;								
							}
							
							boolean currentConnectionChangeStatus = currDBRTimeEvent.hasFieldValues() && currDBRTimeEvent.getFields().containsKey("cnxlostepsecs"); 
							boolean previousConnectionChangeStatus = previousDBRTimeEvent.hasFieldValues() && previousDBRTimeEvent.getFields().containsKey("cnxlostepsecs");
							if(currentConnectionChangeStatus != previousConnectionChangeStatus) {
								// If connection state changes changes, write out current value and update last known value 
								Event clone = e.makeClone();
								lastEventWrittenOut = clone;
								buf.add(clone);
								logger.debug("Writing out from connection state change at " + TimeUtils.convertToISO8601String(clone.getEventTimeStamp()));
								continue;								
							}


							// If Diff >= ADEL
							//    Write out value
							//    Update last known value
							double diff = Math.abs(currentVALue - lastVALueWrittenOut);
							if (diff > adelValue) { 
								Event clone = e.makeClone();
								lastEventWrittenOut = clone;
								buf.add(clone);
								logger.debug("Writing out from magnitude change at " + TimeUtils.convertToISO8601String(clone.getEventTimeStamp()));
								continue;
							}
						} catch(PBParseException ex) { 
							logger.error("Skipping possible corrupted event for pv " + strm.getDescription());
						}
					}

					return buf;
				}
			}

		};
	}

	@Override
	public String getIdentity() {
		return "deadBand";
	}

	@Override
	public String getExtension() {
		if(adelValue == DEFAULT_ADEL) {
			return "deadBand";
		} else {
			return "deadBand_" + Double.toString(adelValue);
		}
	}
}
