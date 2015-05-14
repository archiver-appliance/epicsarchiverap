/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;


import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.etl.ETLBulkStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.search.FileEventStreamSearch;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;

/**
 * An EventStream that is backed by a single PB file.
 * You can only get one iterator out of this event stream. This condition is also checked for.
 * This is typically used with/after PlainPBFileNameUtility.getFilesWithData
 * @author mshankar
 *
 */
public class FileBackedPBEventStream implements EventStream, RemotableOverRaw, ETLBulkStream {
	private static Logger logger = Logger.getLogger(FileBackedPBEventStream.class.getName());
	private String pvName;
	private Path path = null;
	private long startFilePos = 0;
	private long endFilePos = 0;
	private Timestamp startTime = null;
	private Timestamp endTime = null;
	private boolean positionBoundaries = true;
	private ArchDBRTypes type;
	private FileBackedPBEventStreamIterator theIterator = null;
	private RemotableEventStreamDesc desc;
	private PayloadInfo info = null;
	
	public FileBackedPBEventStream(String pvname, Path path, ArchDBRTypes type) throws IOException {
		this.pvName = pvname;
		this.path = path;
		this.type = type;
		this.startFilePos = 0L;
		this.endFilePos = Files.size(path);
		this.positionBoundaries = true;
	}
	
	public FileBackedPBEventStream(String pvname, Path path, ArchDBRTypes type, long startPosition, long endPosition) throws IOException {
		this.pvName = pvname;
		this.path = path;
		this.type = type;
		this.startFilePos = startPosition;
		this.endFilePos = endPosition;
		this.positionBoundaries = true;
	}
	
	public FileBackedPBEventStream(String pvname, Path path, ArchDBRTypes dbrtype, Timestamp startTime, Timestamp endTime, boolean skipSearch) throws IOException {
		this.pvName = pvname;
		this.path = path;
		this.type = dbrtype;
		this.startFilePos = 0L;
		this.endFilePos = Files.size(path);
		if(skipSearch) {
			// We filter events as we are processing the stream...
			this.positionBoundaries = false;
			this.startTime = startTime;
			this.endTime = endTime;
		} else {
			// We use a search to locate the boundaries of the data and the constrain based on position.
			seekToTimes(path, dbrtype, startTime, endTime);
		}
	}


	@Override
	public Iterator<Event> iterator() {
		try {
			if(theIterator != null) {
				logger.error("We can only support one iterator per FileBackedPBEventStream. This one already has an iterator created.");
				return null;
			}
			
			if(info == null) {
				readPayLoadInfo();
			}
			
			if(this.positionBoundaries) {
				theIterator = new FileBackedPBEventStreamPositionBasedIterator(path, startFilePos, endFilePos, desc.getYear(), type);
			} else {
				theIterator = new FileBackedPBEventStreamTimeBasedIterator(path, startTime, endTime, desc.getYear(), type);
			}
			return theIterator;

		} catch (IOException ex) {
			logger.error(ex.getMessage(), ex);
			return null;
		}
	}

	@Override
	public void close() {
		if(theIterator!=null) {
			try {
				theIterator.close();
			} catch (IOException e) {
				logger.error("Exception closing stream", e);
			}
			theIterator = null;
		}
	}
	

	@Override
	public RemotableEventStreamDesc getDescription() {
		try {
			if(info == null) {
				readPayLoadInfo();
			}
		} catch(IOException ex) {
			logger.error("Exception reading payload info for pv " + pvName + " from path " + path.toString(), ex);
		}

		return desc;
	}
	
	private void readPayLoadInfo() throws IOException {
		try(LineByteStream lis = new LineByteStream(path, 0L)) {
			byte[] payloadLine = LineEscaper.unescapeNewLines(lis.readLine());
			info = PayloadInfo.parseFrom(payloadLine);
			desc = new RemotableEventStreamDesc(pvName, info);
			desc.setSource(path.toString());
			if(!this.pvName.equals(info.getPvname())) {
				logger.error("File " + path.toAbsolutePath().toString() + " is being used to read data for pv " + this.pvName + " but it actually contains data for pv " + info.getPvname());
			}
			if(!this.type.equals(ArchDBRTypes.valueOf(info.getType()))) {
				throw new Exception("File " + path.toAbsolutePath().toString() + " contains " + ArchDBRTypes.valueOf(info.getType()).toString() + " we are expecting " + this.type.toString());
			}
			if(startFilePos == 0L) {
				// We add the -1 here to make sure we include the first line.
				startFilePos = lis.getCurrentPosition()-1;
				logger.debug("Setting start position after header " + startFilePos);
			}
		} catch(Throwable t) {
			logger.error("Exception determing header information from file " + path.toAbsolutePath().toString(), t);
			throw new IOException(t);
		}
	}

	public String getPvName() {
		return pvName;
	}
	
	private void seekToTimes(Path path, ArchDBRTypes dbrtype, Timestamp startTime, Timestamp endTime) throws IOException {
		int startSecondsIntoYear = TimeUtils.convertToYearSecondTimestamp(startTime).getSecondsintoyear();
		int endSecondsIntoYear = TimeUtils.convertToYearSecondTimestamp(endTime).getSecondsintoyear();
		YearSecondTimestamp startYTS = TimeUtils.convertToYearSecondTimestamp(startTime); 
		YearSecondTimestamp endYTS = TimeUtils.convertToYearSecondTimestamp(endTime); 

		readPayLoadInfo();

		long endPosition = Files.size(path);
		boolean endfound = false;
		long startPosition = 0;
		boolean startfound = false;

		try { 
			if(info.getYear() == endYTS.getYear()) {
				FileEventStreamSearch bsend = new FileEventStreamSearch(path, startFilePos);
				endfound = bsend.seekToTime(dbrtype, endSecondsIntoYear);
				if(endfound) {
					endPosition = bsend.getFoundPosition();
					
					DBR2PBTypeMapping mapping = DBR2PBTypeMapping.getPBClassFor(this.type);;
					Constructor<? extends DBRTimeEvent> unmarshallingConstructor = mapping.getUnmarshallingFromByteArrayConstructor();
					ByteArray nextLine = new ByteArray(LineByteStream.MAX_LINE_SIZE);
					try(LineByteStream lis = new LineByteStream(path, endPosition)) {
						// The seekToTime call will have positioned the pointer to the last known event before the endSecondsIntoYear
						// We'll skip two lines to get past the last known event before the endSecondsIntoYear and the event itself.
						// We do have the ArchDBRType; so we can parse the pb messages and use time based iteration just for this part.
						// Jud Gaudenz pointed out a test case for this; so we not use time based iteration for this part..
						lis.seekToFirstNewLine();
						lis.readLine(nextLine);
						while(!nextLine.isEmpty()) { 
							DBRTimeEvent event = (DBRTimeEvent) unmarshallingConstructor.newInstance(this.desc.getYear(), nextLine);
							if(event.getEventTimeStamp().after(endTime) || event.getEventTimeStamp().equals(endTime)) { 
								break;
							} else { 
								if(logger.isDebugEnabled()) { 
									logger.debug("Going past event at " + TimeUtils.convertToHumanReadableString(event.getEventTimeStamp()) 
											+ " when seeking end position for PV " + pvName +
											" at " 
											+ TimeUtils.convertToHumanReadableString(endTime)
											);
								}
							}
							endPosition = lis.getCurrentPosition();
							lis.readLine(nextLine);
						}
					} catch(Exception ex) { 
						logger.error("Exception seeking to the end position for pv " + this.pvName, ex);
					}
				}
			}

			if(info.getYear() == startYTS.getYear()) {
				FileEventStreamSearch bsstart = new FileEventStreamSearch(path, startFilePos);
				startfound = bsstart.seekToTime(dbrtype, startSecondsIntoYear);
				if(startfound) {
					startPosition = bsstart.getFoundPosition();
				}
			}
		} catch(IOException ex) { 
			logger.error("Exception seeking to times for pv " + pvName + " in file " + path.toAbsolutePath().toString() , ex);
		}

		logger.debug("Found start position " + startPosition + " and end position " + endPosition + " in " + path.toString());
		if(startfound && endfound) {
			// We are assuming that the max span of a PB file is a year and the caller will use TimeUtils.breakIntoYearlyTimeSpans or equivalent to make multiple calls into this method.
			this.startFilePos = startPosition;
			this.endFilePos = endPosition;
			this.positionBoundaries = true;
		} else {
			logger.debug("Did not find the start and end positions for pv " + pvName + " in file " + path.toAbsolutePath().toString() + ". Switching to using a time based iterator");
			// We filter events as we are processing the stream...
			this.positionBoundaries = false;
			this.startTime = startTime;
			this.endTime = endTime;
		}
	}

	@Override
	public Event getFirstEvent(BasicContext context) throws IOException {
		PBFileInfo fileInfo = new PBFileInfo(path, false);
		return fileInfo.firstEvent;
	}

	@Override
	public ReadableByteChannel getByteChannel(BasicContext context) throws IOException {
		PBFileInfo fileInfo = new PBFileInfo(path, false);
		SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ);
		channel.position(fileInfo.getActualDataStartsHere());
		return channel;
	}
}

