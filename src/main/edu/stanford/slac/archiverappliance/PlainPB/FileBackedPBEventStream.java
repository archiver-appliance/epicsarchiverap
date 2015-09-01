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

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.search.FileEventStreamSearch;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;

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
	private PBFileInfo fileInfo = null;
	
	/**
	 * Used when we want to include data from the entire file. 
	 * @param pvname
	 * @param path
	 * @param type
	 * @throws IOException
	 */
	public FileBackedPBEventStream(String pvname, Path path, ArchDBRTypes type) throws IOException {
		this.pvName = pvname;
		this.path = path;
		this.type = type;
		this.startFilePos = 0L;
		this.endFilePos = Files.size(path);
		this.positionBoundaries = true;
	}
	
	/**
	 * Used when we know the file locations of the start and end. Really only used in one utility...
	 * @param pvname
	 * @param path
	 * @param type
	 * @param startPosition
	 * @param endPosition
	 * @throws IOException
	 */
	public FileBackedPBEventStream(String pvname, Path path, ArchDBRTypes type, long startPosition, long endPosition) throws IOException {
		this.pvName = pvname;
		this.path = path;
		this.type = type;
		this.startFilePos = startPosition;
		this.endFilePos = endPosition;
		this.positionBoundaries = true;
	}
	
	/**
	 * Used when we know the start and end times. There are six cases here; see the FileBackedIteratorTest for more details.
	 * For performance reasons, we want to use the location based iterator as much as possible.
	 * But in case of issues, we do not want to not return data. So, fall back to a time based iterator
	 * @param pvname
	 * @param path
	 * @param dbrtype
	 * @param startTime
	 * @param endTime
	 * @param skipSearch
	 * @throws IOException
	 */
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
			
			if(fileInfo == null) {
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
			if(fileInfo == null) {
				readPayLoadInfo();
			}
		} catch(IOException ex) {
			logger.error("Exception reading payload info for pv " + pvName + " from path " + path.toString(), ex);
		}

		return desc;
	}
	
	private void readPayLoadInfo() throws IOException {
		try { 
			fileInfo = new PBFileInfo(path);
			desc = new RemotableEventStreamDesc(pvName, fileInfo.getInfo());
			desc.setSource(path.toString());
			if(!this.pvName.equals(fileInfo.getPVName())) {
				logger.error("File " + path.toAbsolutePath().toString() + " is being used to read data for pv " + this.pvName + " but it actually contains data for pv " + fileInfo.getPVName());
			}
			if(!this.type.equals(fileInfo.getType())) {
				throw new Exception("File " + path.toAbsolutePath().toString() + " contains " + fileInfo.getType().toString() + " we are expecting " + this.type.toString());
			}
			if(startFilePos == 0L) {
				// We add the -1 here to make sure we include the first line.
				startFilePos = fileInfo.getPositionOfFirstSample()-1;
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
	
	/**
	 * Determine the iterator to be used for this query based on the query start and end times and the first and last sample times.
	 * @param path
	 * @param dbrtype
	 * @param queryStartTime
	 * @param queryEndTime
	 * @throws IOException
	 */
	private void seekToTimes(Path path, ArchDBRTypes dbrtype, Timestamp queryStartTime, Timestamp queryEndTime) throws IOException {
		readPayLoadInfo();
		
		long queryStartEpoch = TimeUtils.convertToEpochSeconds(queryStartTime);
		long queryEndEpoch = TimeUtils.convertToEpochSeconds(queryEndTime);

		if(fileInfo.getLastEvent() == null) { 
			logger.warn("Cannot determine last event; defaulting to a time based iterator " + path.toAbsolutePath().toString());
			this.positionBoundaries = false;
			this.startTime = queryStartTime;
			this.endTime = queryEndTime;
		}

		long firstSampleEpoch = TimeUtils.convertToEpochSeconds(fileInfo.getFirstEvent().getEventTimeStamp());
		long lastSampleEpoch = TimeUtils.convertToEpochSeconds(fileInfo.getLastEvent().getEventTimeStamp());
		
		if(queryStartEpoch < firstSampleEpoch && queryEndEpoch < firstSampleEpoch) {
			logger.debug("Case 1 - this file should not be included in request");
			this.positionBoundaries = false;
			this.startTime = queryStartTime;
			this.endTime = queryEndTime;
		} else if(queryStartEpoch < firstSampleEpoch && queryEndEpoch >= firstSampleEpoch && queryEndEpoch < lastSampleEpoch) { 
			logger.debug("Case 2 - start at the beginning and lookup the end");
			long endPosition = seekToEndTime(path, dbrtype, queryStartTime, queryEndTime);
			if(endPosition != -1) { 
				this.positionBoundaries = true;
				this.startFilePos = fileInfo.getPositionOfFirstSample() - 1;
				this.endFilePos = endPosition;
			} else { 
				logger.warn("Case 2 - did not find the end  for pv " + pvName + " in file " + path.toAbsolutePath().toString() + ". Switching to using a time based iterator");
				this.positionBoundaries = false;
				this.startTime = queryStartTime;
				this.endTime = queryEndTime;
			}
		} else if(queryStartEpoch <= firstSampleEpoch && queryEndEpoch > lastSampleEpoch) { 
			logger.debug("Case 3 - we need all of the data in this file");
			this.positionBoundaries = true;
			this.startFilePos = fileInfo.getPositionOfFirstSample() - 1;
			this.endFilePos = Files.size(path);
		} else if(queryStartEpoch >= firstSampleEpoch && queryEndEpoch < lastSampleEpoch) { 
			logger.debug("Case 4 - Lookup start and end");
			long endPosition = seekToEndTime(path, dbrtype, queryStartTime, queryEndTime);
			long startPosition = seekToStartTime(path, dbrtype, queryStartTime, queryEndTime);
			if(startPosition != -1 && endPosition != -1) {
				this.positionBoundaries = true;
				this.startFilePos = startPosition;
				this.endFilePos = endPosition;
			} else { 
				logger.warn("Case 4 - did not find the either the start " + startPosition + " or end " + endPosition + " for pv " + pvName + " in file " + path.toAbsolutePath().toString() + ". Switching to using a time based iterator");
				this.positionBoundaries = false;
				this.startTime = queryStartTime;
				this.endTime = queryEndTime;
			}
		} else if(queryStartEpoch >= firstSampleEpoch && queryEndEpoch > lastSampleEpoch) { 
			logger.debug("Case 5 - lookup the start and go all the way to the end");
			long startPosition = seekToStartTime(path, dbrtype, queryStartTime, queryEndTime);
			if(startPosition != -1) { 
				this.positionBoundaries = true;
				this.startFilePos = startPosition;
				this.endFilePos = Files.size(path);
			} else { 
				logger.warn("Case 5 - did not find the start for pv " + pvName + " in file " + path.toAbsolutePath().toString() + ". Switching to using a time based iterator");
				this.positionBoundaries = false;
				this.startTime = queryStartTime;
				this.endTime = queryEndTime;
			}
		} else if(queryStartEpoch > lastSampleEpoch && queryEndEpoch > lastSampleEpoch) { 
			logger.debug("Case 6 - we only the last sample");
			this.positionBoundaries = true;
			this.startFilePos = fileInfo.getPositionOfLastSample() - 1;
			this.endFilePos = Files.size(path);
		} else {
			logger.error("Unexpected case in seekToTimes for pv " + pvName 
					+ " in file " + path.toAbsolutePath().toString()
					+ " Query start " + TimeUtils.convertToISO8601String(queryStartTime)
					+ " Query end " + TimeUtils.convertToISO8601String(queryEndTime)
					+ " First sample " + TimeUtils.convertToISO8601String(firstSampleEpoch)
					+ " Last sample " + TimeUtils.convertToISO8601String(lastSampleEpoch)
					+ ". Switching to using a time based iterator");
			this.positionBoundaries = false;
			this.startTime = queryStartTime;
			this.endTime = queryEndTime;
		}
	}
	
	
	private long seekToEndTime(Path path, ArchDBRTypes dbrtype, Timestamp queryStartTime, Timestamp queryEndTime) throws IOException { 
		long endPosition = -1;
		YearSecondTimestamp queryEndYTS = TimeUtils.convertToYearSecondTimestamp(queryEndTime); 
		int queryEndSecondsIntoYear = TimeUtils.convertToYearSecondTimestamp(queryEndTime).getSecondsintoyear();
		if(fileInfo.getInfo().getYear() == queryEndYTS.getYear()) {
			FileEventStreamSearch bsend = new FileEventStreamSearch(path, startFilePos);
			boolean endfound = bsend.seekToTime(dbrtype, queryEndSecondsIntoYear);
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
						if(event.getEventTimeStamp().after(queryEndTime) || event.getEventTimeStamp().equals(queryEndTime)) { 
							break;
						} else { 
							if(logger.isDebugEnabled()) { 
								logger.debug("Going past event at " + TimeUtils.convertToHumanReadableString(event.getEventTimeStamp()) 
										+ " when seeking end position for PV " + pvName +
										" at " 
										+ TimeUtils.convertToHumanReadableString(queryEndTime)
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
		
		return endPosition;
	}
	
	private long seekToStartTime(Path path, ArchDBRTypes dbrtype, Timestamp queryStartTime, Timestamp queryEndTime) throws IOException { 
		int queryStartSecondsIntoYear = TimeUtils.convertToYearSecondTimestamp(queryStartTime).getSecondsintoyear();
		YearSecondTimestamp queryStartYTS = TimeUtils.convertToYearSecondTimestamp(queryStartTime);
		long startPosition = -1;

		if(fileInfo.getInfo().getYear() == queryStartYTS.getYear()) {
			FileEventStreamSearch bsstart = new FileEventStreamSearch(path, startFilePos);
			boolean startfound = bsstart.seekToTime(dbrtype, queryStartSecondsIntoYear);
			if(startfound) {
				startPosition = bsstart.getFoundPosition();
			}
		}
		return startPosition;
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
		channel.position(fileInfo.getPositionOfFirstSample());
		return channel;
	}
}

