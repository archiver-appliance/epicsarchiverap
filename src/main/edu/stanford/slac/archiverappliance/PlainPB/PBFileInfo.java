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
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.data.PBParseException;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;

/**
 * Gets some information about PB files.
 * Important information includes the first and last event.
 * 
 * @author mshankar
 *
 */
public class PBFileInfo {
	private static final Logger logger = Logger.getLogger(PBFileInfo.class);
	PayloadInfo info;
	DBRTimeEvent firstEvent = null;
	DBRTimeEvent lastEvent = null;
	long positionOfFirstSample = 0L;
	long positionOfLastSample = 0;
	
	public static class MissingHeaderException extends IOException {
		public MissingHeaderException() {}
	}
	
	public PBFileInfo(Path path) throws IOException {
		this(path, true);
	}

	public PBFileInfo(Path path, boolean lookupLastEvent) throws IOException {
		try(LineByteStream lis = new LineByteStream(path)) {
			byte[] payloadLine = LineEscaper.unescapeNewLines(lis.readLine());
			if (payloadLine == null) {
				throw new MissingHeaderException();
			}
			
			info = PayloadInfo.parseFrom(payloadLine);
			positionOfFirstSample = lis.getCurrentPosition();
			
			// This will be corrected as long as there are any samples found (and lookupLastEvent is true).
			// Otherwise, the fileSize value will make anyone reading from this offset not find any events
			// (that we know of at this time).
			positionOfLastSample = fileSize;

			ArchDBRTypes type = ArchDBRTypes.valueOf(info.getType());
			Constructor<? extends DBRTimeEvent> unmarshallingConstructor = DBR2PBTypeMapping.getPBClassFor(type).getUnmarshallingFromByteArrayConstructor();
			// Let's read the first line
			lis.seekToFirstNewLine();
			byte[] firstLine = lis.readLine();
			if(firstLine != null) {
				firstEvent = (DBRTimeEvent) unmarshallingConstructor.newInstance(getDataYear(), new ByteArray(firstLine));
				if(lookupLastEvent) {
					// If we do not have a first line, we probably do not have a last line
					this.lookupLastEvent(path, lis, unmarshallingConstructor);
				}
			} else {
				logger.debug("File " + path.toAbsolutePath().toString() + " does not seem to have any first line?");
			}
		}
		catch (MissingHeaderException e) {
			throw e;
		}
		catch (Exception e) {
			logger.warn("Exception determing header information from file " + path.toAbsolutePath().toString(), e);
			throw (e instanceof IOException) ? (IOException)e : new IOException(e);
		}
	}
	
	public String getPVName() {
		return info.getPvname();
	}
	
	public short getDataYear() {
		return (short) info.getYear();
	}
	
	public ArchDBRTypes getType() {
		return ArchDBRTypes.valueOf(info.getType());
	}
	
	public PayloadInfo getInfo() {
		return info;
	}

	public DBRTimeEvent getFirstEvent() {
		return firstEvent;
	}

	public DBRTimeEvent getLastEvent() {
		return lastEvent;
	}
	
	public long getFirstEventEpochSeconds() {
		return (firstEvent != null) ? firstEvent.getEpochSeconds() : 0;
	}
	
	public long getLastEventEpochSeconds() {
		return (lastEvent != null) ? lastEvent.getEpochSeconds() : 0;
	}

	public long getPositionOfFirstSample() {
		return positionOfFirstSample;
	}
	
	public long getPositionOfLastSample() { 
		return positionOfLastSample;
	}
	
	
	/**
	 * Checks the payload info and makes sure we are using appropriate files.
	 * This assumes that the lis is positioned at the start and subsequently positions the lis just past the first line.
	 * So if we need to position the lis elsewhere, the caller needs to do that manually after this call.
	 * @param lis
	 * @throws IOException
	 */
	public static void checkPayloadInfo(LineByteStream lis, String pvName, ArchDBRTypes type) throws IOException {
		byte[] payloadLine = LineEscaper.unescapeNewLines(lis.readLine());
		PayloadInfo info = PayloadInfo.parseFrom(payloadLine);
		if(!pvName.equals(info.getPvname())) {
			logger.error("File " + lis.getAbsolutePath() + " is being used to read data for pv " + pvName + " but it actually contains data for pv " + info.getPvname());
		}
		if(!type.equals(ArchDBRTypes.valueOf(info.getType()))) {
			throw new IOException("File " + lis.getAbsolutePath() + " contains " + ArchDBRTypes.valueOf(info.getType()).toString() + " we are expecting " + type.toString());
		}
	}
	
	
	private void lookupLastEvent(Path path, LineByteStream lis, Constructor<? extends DBRTimeEvent> unmarshallingConstructor) throws Exception { 
		// Look for the right-most newline except the last character.
		// If there is none, there is no last event.
		if (!lis.seekToBeforeLastLine()) {
			return;
		}
		long lastLinePos = lis.getCurrentPosition();
		
		// Read the line following this newline.
		// If we have an incomplete line, we will try to look for the previous line.
		byte[] lastLine = lis.readLine();
		if (lastLine == null) {
			// If no previous line, there is no last event.
			if (!lis.seekToBeforePreviousLine(lastLinePos - 1)) {
				return;
			}
			
			// Try to read this line now. Since we have found an incomplete
			// line at the end, this one will normally be complete.
			lastLinePos = lis.getCurrentPosition();
			lastLine = lis.readLine();
			if (lastLine == null) {
				return;
			}
		}
		
		// Parse the line. We expect success since it is a complete line.
		try { 
			lastEvent = (DBRTimeEvent) unmarshallingConstructor.newInstance(getDataYear(), new ByteArray(lastLine));
			lastEvent.getEventTimeStamp();
			positionOfLastSample = lastLinePos;
		} catch (PBParseException ex) {
			logger.warn(path.toString() + " has a corrupt last complete line");
			lastEvent = null;
		}
	}
}
