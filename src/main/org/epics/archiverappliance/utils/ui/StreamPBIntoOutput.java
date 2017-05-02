/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.ui;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo.Builder;
import edu.stanford.slac.archiverappliance.PB.data.PartionedTime;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;

/**
 * Utility class with method to push an EventStream into an OutputStream
 * @author mshankar
 *
 */
public class StreamPBIntoOutput {
	private static final Logger logger = Logger.getLogger(StreamPBIntoOutput.class);
	
	
	/**
	 * Push the events in st into the output stream os.
	 * This is highly simplistic and does not accommodate year transitions, pv transitions and such.
	 * @param st EventStream 
	 * @param os OutputStream
	 * @param start The start time; could be null in which case we begin at the very beginning
	 * @param end The end time; could be null in which case we end at the very end
	 * @return totalEvents  &emsp; 
	 * @throws IOException  &emsp; 
	 */
	public static int streamPBIntoOutputStream(EventStream st, OutputStream os, Timestamp start, Timestamp end) throws IOException {
		long startTimeInEpochSeconds = 0;
		if(start != null)
			startTimeInEpochSeconds = TimeUtils.convertToEpochSeconds(start);
		long endTimeInEpochSeconds = Long.MAX_VALUE;
		if(end != null)
			endTimeInEpochSeconds = TimeUtils.convertToEpochSeconds(end);
		
		// Write the PB header.
		assert(st instanceof RemotableOverRaw);
		RemotableEventStreamDesc desc = ((RemotableOverRaw)st).getDescription();
		assert(desc != null);
		short previousYear = -1;

		int totalEvents = 0;
		try {
			for(Event e : st) {
				previousYear = writeHeader(previousYear, os, desc, e, startTimeInEpochSeconds);
				long epochSeconds = e.getEpochSeconds();
				if(epochSeconds >= startTimeInEpochSeconds && epochSeconds <= endTimeInEpochSeconds) {
					ByteArray val = e.getRawForm();
					os.write(val.data, val.off, val.len);
					os.write(LineEscaper.NEWLINE_CHAR);
					totalEvents++;
				} else {
					if(logger.isDebugEnabled()) { 
						logger.debug("Skipping event" 
								 + " with timestamp " + TimeUtils.convertToISO8601String(TimeUtils.convertFromEpochSeconds(epochSeconds, 0))
								 + " and start " + TimeUtils.convertToISO8601String(TimeUtils.convertFromEpochSeconds(startTimeInEpochSeconds, 0))
								 + " with end " + TimeUtils.convertToISO8601String(TimeUtils.convertFromEpochSeconds(endTimeInEpochSeconds, 0))
								);
					}
				}
			}
			
			if(totalEvents == 0 && previousYear != -1) {
				// If we did not write any events, we should at least write the header indicating that the PV is valid 
				// Otherwise we see some exceptions in PBOverHTTPStoragePlugin that we really want to watch.
				// If we already wrote the header out, then previous year should be something other than -1.
				logger.debug("Writing a header for an empty event stream for pv " + desc.getPvName());
				previousYear = writeHeader(previousYear, os, desc, null, startTimeInEpochSeconds);
			}
			
		} finally {
			try { st.close(); } catch(Throwable t) {}					
		}
		return totalEvents;
	}


	private static short writeHeader(short previousYear, OutputStream os, RemotableEventStreamDesc desc, Event e, long startTimeInEpochSeconds) throws IOException {
		short currentYear = -1;
		if(e != null) {
			if(e instanceof PartionedTime) {
				currentYear = ((PartionedTime)e).getYear();
			} else {
				currentYear = TimeUtils.computeYearForEpochSeconds(e.getEpochSeconds());
			}
		} else {
			currentYear = TimeUtils.computeYearForEpochSeconds(startTimeInEpochSeconds);
		}
		
		if(previousYear == -1 || previousYear != currentYear) {
			if(previousYear != -1) os.write(LineEscaper.NEWLINE_CHAR); // Blank line indicates a new chunk in the event stream; skip if this is the first chunk
			Builder builder = PayloadInfo.newBuilder()
					.setPvname(desc.getPvName())
					.setType(desc.getArchDBRType().getPBPayloadType())
					.setYear(currentYear);
			desc.mergeInto(builder);
			PayloadInfo infoWithFields = builder.build();
			byte[] headerBytes = LineEscaper.escapeNewLines(infoWithFields.toByteArray());
			os.write(headerBytes);
			os.write(LineEscaper.NEWLINE_CHAR);
		}
		return currentYear;
	}
	
	/**
	 * Write a header only - this is sometimes used to communicate the latest copy of the meta-fields (EGU etc) from the engine to the client even if we have no data in the engine.
	 * @param os OutputStream
	 * @param desc  RemotableEventStreamDesc 
	 * @throws IOException  &emsp; 
	 */
	public static void writeHeaderOnly(OutputStream os, RemotableEventStreamDesc desc) throws IOException { 
		Builder builder = PayloadInfo.newBuilder()
				.setPvname(desc.getPvName())
				.setType(desc.getArchDBRType().getPBPayloadType())
				.setYear(desc.getYear());
		desc.mergeInto(builder);
		PayloadInfo infoWithFields = builder.build();
		byte[] headerBytes = LineEscaper.escapeNewLines(infoWithFields.toByteArray());
		os.write(headerBytes);
		os.write(LineEscaper.NEWLINE_CHAR);
	}
}
