package edu.stanford.slac.archiverappliance.PlainPB;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.etl.ETLBulkStream;
import org.epics.archiverappliance.etl.ETLContext;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;

/**
 * Companion class to PlainPBStoragePlugin that handles the appending of event streams in a partition-aware fashion.
 * This is used both by the engine and by ETL.
 * 
 * @author mshankar
 *
 */
public class AppendDataStateData {
	private static Logger logger = Logger.getLogger(AppendDataStateData.class.getName());

	private PartitionGranularity partitionGranularity;
	private String rootFolder;

	private OutputStream os = null;
	// These two pieces of information (previousYear and previousEpochSeconds) are from the store using the last known sample when we appending to an existing stream.
	// See the creation and use of the PBFileInfo object below.
	private short previousYear = -1;
	private short currentEventsYear = -1;
	private long previousEpochSeconds = -1;
	private Timestamp lastKnownTimeStamp = new Timestamp(0);
	private long nextPartitionFirstSecond = 0;
	private String previousFileName = null;

	private String desc;
	private CompressionMode compressionMode = CompressionMode.NONE;
	private PVNameToKeyMapping pv2key;

	/**
	 * @param partitionGranularity - partitionGranularity of the PB plugin.
	 * @param rootFolder - RootFolder of the PB plugin
	 * @param desc - Desc for logging purposes
	 * @param lastKnownTimestamp - This is probably the most important argument here. This is the last known timestamp in this storage. If null, we assume time(0) for the last known timestamp.
	 *  
	 */
	public AppendDataStateData(PartitionGranularity partitionGranularity, String rootFolder, String desc, Timestamp lastKnownTimestamp, CompressionMode compressionMode, PVNameToKeyMapping pv2key) {
		this.partitionGranularity = partitionGranularity;
		this.rootFolder = rootFolder;
		this.desc = desc;
		this.compressionMode = compressionMode;
		this.pv2key = pv2key;
		if(lastKnownTimestamp != null) { 
			this.lastKnownTimeStamp = lastKnownTimestamp;
			this.previousEpochSeconds = TimeUtils.convertToEpochSeconds(lastKnownTimeStamp);
			this.previousYear = TimeUtils.computeYearForEpochSeconds(previousEpochSeconds);
		}
	}
	
	/**
	 * Append data into PB files honoring partition boundaries switching into new partitions as we cross the boundary.
	 * <ol>
	 * <li>We make sure timestamp monotonicity is maintained.</li>
	 * <li>We generate clean partitions.</li>
	 * </ol>
	 * @param pvName
	 * @param stream
	 * @param extension
	 * @param extensionToCopyFrom
	 * @return
	 * @throws IOException
	 */
	public int partitionBoundaryAwareAppendData(BasicContext context, String pvName, EventStream stream, String extension, String extensionToCopyFrom) throws IOException {
		try {
			int eventsAppended = 0;
			for(Event event : stream) {
				long epochSeconds = event.getEpochSeconds();
				if(shouldISkipEventBasedOnTimeStamps(event)) continue;

				Path pvPath = null;
				shouldISwitchPartitions(context, pvName, extension,epochSeconds, pvPath);
				
				if(this.os == null) {
					pvPath = preparePartition(pvName, stream, context,extension, extensionToCopyFrom, epochSeconds,pvPath);
				}
				
				// We check for monotonicity in timestamps again as we had some fresh data from an existing file.
				if(shouldISkipEventBasedOnTimeStamps(event)) continue;

				// The raw form is already escaped for new lines
				// We can simply write it as is.
				ByteArray val = event.getRawForm();
				this.os.write(val.data, val.off, val.len);
				this.os.write(LineEscaper.NEWLINE_CHAR);
				
				this.previousEpochSeconds = epochSeconds;
				this.previousYear = this.currentEventsYear;
				this.lastKnownTimeStamp = event.getEventTimeStamp();
				eventsAppended++;
				// logger.debug("Done appending event " + TimeUtils.convertToISO8601String(event.getEventTimeStamp()) + " into " + previousFileName + " of len " + val.len);
			}
			return eventsAppended;
		} catch(Throwable t) {
			logger.error("Exception appending data for PV " + pvName, t);
			throw new IOException(t);
		} finally {
			if(this.os != null) { try { this.os.close(); this.os = null; } catch(Throwable t) { logger.error("Exception closing os", t); } }
			try { stream.close(); } catch (Throwable t) {} 
		}
	}

	/**
	 * Prepare a new partition.
	 * @param pvName
	 * @param stream
	 * @param context
	 * @param extension
	 * @param extensionToCopyFrom
	 * @param epochSeconds
	 * @param pvPath
	 * @return
	 * @throws IOException
	 */
	private Path preparePartition(String pvName, EventStream stream, BasicContext context, String extension, String extensionToCopyFrom, long epochSeconds, Path pvPath) throws IOException {
		if(pvPath == null) {
			Path nextPath = PlainPBPathNameUtility.getFileName(this.rootFolder, pvName, epochSeconds, extension, this.partitionGranularity, true, context.getPaths(), compressionMode, this.pv2key);
			pvPath = nextPath;
		}
		
		if(!Files.exists(pvPath)) {
			if(extensionToCopyFrom != null && !extensionToCopyFrom.contentEquals("")) {
				// If the file has not been created yet and if we have an extension to copy from
				// We check for the file with the extensionToCopyFrom
				// If that exists, we make a copy of that
				// This is an attempt to not lose data during ETL appends. 
				// We make a copy of the original file if it exists, append to the copy and then do an atomic move.
				// Should we should use path's resolve here?
				Path pathToCopyFrom = context.getPaths().get(pvPath.toAbsolutePath().toString().replace(extension, extensionToCopyFrom));
				if(Files.exists(pathToCopyFrom)) {
					logger.debug("Making a backup from " + pathToCopyFrom.toAbsolutePath().toString() + " to file " + pvPath.toAbsolutePath().toString() + " when appending data for pv " + pvName);
					Files.copy(pathToCopyFrom, pvPath);
					// We still have to create an os so that the logic can continue.
					updateStateBasedOnExistingFile(pvName, pvPath, stream);
					
					
				} else {
					logger.debug("File to copy from " + pathToCopyFrom.toAbsolutePath().toString() + " does not exist when appending data for pv " + pvName);
					createNewFileAndWriteAHeader(pvName, pvPath, stream, false);
				}
			} else {
				logger.debug("File to copy from is not specified and the file " + pvPath.toAbsolutePath().toString() + " does not exist when appending data for pv " + pvName);
				createNewFileAndWriteAHeader(pvName, pvPath, stream, false);
			}
		} else {
			if(logger.isDebugEnabled()) { logger.debug(desc + ": Appending to existing PB file " + pvPath.toAbsolutePath().toString() + " for PV " + pvName + " for year " + this.currentEventsYear); }
			updateStateBasedOnExistingFile(pvName, pvPath, stream);
		}
		return pvPath;
	}

	/**
	 * Should we switch to a new partition? If so, return the new partition, else return the current partition.
	 * @param context
	 * @param pvName
	 * @param extension
	 * @param epochSeconds
	 * @param currentPath
	 * @return
	 * @throws IOException
	 */
	private Path shouldISwitchPartitions(BasicContext context, String pvName, String extension, long epochSeconds, Path currentPath) throws IOException {
		if(epochSeconds >= this.nextPartitionFirstSecond) {
			Path nextPath = PlainPBPathNameUtility.getFileName(this.rootFolder, pvName, epochSeconds, extension, this.partitionGranularity, true, context.getPaths(), compressionMode, this.pv2key);
			this.nextPartitionFirstSecond = TimeUtils.getNextPartitionFirstSecond(epochSeconds, this.partitionGranularity);
			if(logger.isDebugEnabled()) {
				if(this.previousFileName != null) {
					logger.debug(desc + ": Encountering a change in partitions in the event stream. " 
							+ "Closing out " + this.previousFileName 
							+ " to make way for " + nextPath 
							+ " Next partition is to be switched at " + TimeUtils.convertToISO8601String(TimeUtils.convertFromEpochSeconds(this.nextPartitionFirstSecond, 0)));
				} else {
					logger.debug(desc + ": New partition into file " + nextPath 
							+ " Next partition is to be switched at " + TimeUtils.convertToISO8601String(TimeUtils.convertFromEpochSeconds(this.nextPartitionFirstSecond, 0)));
				}
			}
			// Simply closing the current stream should be good enough for the roll over to work.
			if(this.os != null) try { this.os.close(); } catch(Throwable t) {}
			// Set this to null outside the try/catch so that we are using a new file even if the close fails.
			this.os = null;
			return nextPath;
		}
		return currentPath;
	}

	/**
	 * Tell appendData if we should skip this event based on the last known event, current year of the destination file etc...
	 * @param state
	 * @param event
	 * @return
	 */
	private boolean shouldISkipEventBasedOnTimeStamps(Event event) {
		long epochSeconds = event.getEpochSeconds();
		this.currentEventsYear = TimeUtils.computeYearForEpochSeconds(epochSeconds);
		Timestamp currentTimeStamp = event.getEventTimeStamp();
		int compare = currentTimeStamp.compareTo(this.lastKnownTimeStamp);
		if(compare <= 0) {
			// Attempt at insisting that the source of this event stream sticks to the contract and gives us ascending times.
			// This takes nanos into account as well.
			logger.debug(desc + ": Skipping data with a timestamp " + TimeUtils.convertToISO8601String(TimeUtils.convertFromEpochSeconds(epochSeconds, 0))
					+ "older than the previous timstamp " + TimeUtils.convertToISO8601String(TimeUtils.convertFromEpochSeconds(this.previousEpochSeconds, 0)));
			return true;
		}
		
		if(epochSeconds < this.previousEpochSeconds) {
			// Attempt at insisting that the source of this event stream sticks to the contract and gives us ascending times.
			logger.debug(desc + ": Skipping data with a timestamp " + TimeUtils.convertToISO8601String(TimeUtils.convertFromEpochSeconds(epochSeconds, 0))
					+ "older than the previous timstamp " + TimeUtils.convertToISO8601String(TimeUtils.convertFromEpochSeconds(this.previousEpochSeconds, 0)));
			return true;
		}
		if(this.currentEventsYear < this.previousYear) {
			// Same test as above.
			logger.debug("Skipping data from a year " + this.currentEventsYear
					+ "older than the previous year " + this.previousYear);
			return true;
		}
		
		return false;
	}
	
	/**
	 * If we have an existing file, then this loads a PBInfo, validates the PV name and then updates the appendDataState
	 * @param state
	 * @param pvName
	 * @param pvPath
	 * @throws IOException
	 */
	private void updateStateBasedOnExistingFile(String pvName, Path pvPath, EventStream stream) throws IOException {
		PBFileInfo info;
		try {
			info = new PBFileInfo(pvPath);
		} catch (PBFileInfo.MissingHeaderException ex) {
			// handle incomplete header - truncate the file and write the header
			logger.warn("Restarting PB file " + pvPath + " due to incomplete header");
			createNewFileAndWriteAHeader(pvName, pvPath, stream, true);
			return;
		}
		
		if (info.getPositionOfDataEnd() != info.getFileSize()) {
			// Fix corruption at the end.
			logger.warn("Truncating incomplete data in PB file " + pvPath);
			try (SeekableByteChannel channel = Files.newByteChannel(pvPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
				channel.truncate(info.getPositionOfDataEnd());
			}
		}
		
		if(!info.getPVName().equals(pvName)) throw new IOException("Trying to append data for " + pvName + " to a file " + pvPath + " that has data for " + info.getPVName());
		this.previousYear = info.getDataYear();
		this.previousEpochSeconds = info.getLastEventEpochSeconds();
		if(info.getLastEvent() != null) this.lastKnownTimeStamp = info.getLastEvent().getEventTimeStamp();
		this.os = new BufferedOutputStream(Files.newOutputStream(pvPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND));
		this.previousFileName = pvPath.getFileName().toString();
	}
	
	/**
	 * In cases where we create a new file, this method is used to create an empty file and write out an header.
	 * @param state
	 * @param pvName
	 * @param pvPath
	 * @param stream
	 * @throws IOException
	 */
	private void createNewFileAndWriteAHeader(String pvName, Path pvPath, EventStream stream, boolean allow_existing) throws IOException {
		if(!allow_existing && Files.exists(pvPath)) throw new IOException("Trying to write a header into a file that exists " + pvPath.toAbsolutePath().toString());
		if(logger.isDebugEnabled()) logger.debug(desc + ": Writing new PB file" + pvPath.toAbsolutePath().toString() 
				+ " for PV " + pvName 
				+ " for year " + this.currentEventsYear 
				+ " of type " + stream.getDescription().getArchDBRType() 
				+ " of PBPayload " + stream.getDescription().getArchDBRType().getPBPayloadType());
		this.os = new BufferedOutputStream(Files.newOutputStream(pvPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
		byte[] headerBytes = LineEscaper.escapeNewLines(PayloadInfo.newBuilder()
				.setPvname(pvName)
				.setType(stream.getDescription().getArchDBRType().getPBPayloadType())
				.setYear(this.currentEventsYear)
				.build().toByteArray());
		this.os.write(headerBytes);
		this.os.write(LineEscaper.NEWLINE_CHAR);
		this.previousFileName = pvPath.getFileName().toString();
	}
	
	/**
	 * Append data in bulk skipping some of the per event checks.
	 * @param pvName
	 * @param bulkStream
	 * @throws IOException
	 */
	public boolean bulkAppend(String pvName, ETLContext context, ETLBulkStream bulkStream, String extension, String extensionToCopyFrom) throws IOException {
		Event firstEvent = bulkStream.getFirstEvent(context);
		if(this.shouldISkipEventBasedOnTimeStamps(firstEvent)) {
			logger.debug("The bulk append functionality works only if we the first event fits cleanly in the current stream.");
			return false;
		}
		
		Path pvPath = null;
		if(this.os == null) {
			pvPath = preparePartition(pvName, bulkStream, context,extension, extensionToCopyFrom, firstEvent.getEpochSeconds(),pvPath);
		}

		// Close the current stream first and set it to null.
		if(this.os != null) try { this.os.close(); } catch(Throwable t) {}
		this.os = null;
		
		// The preparePartition should have created the needed file; so we only append
		try(ByteChannel destChannel = Files.newByteChannel(pvPath, StandardOpenOption.APPEND); ReadableByteChannel srcChannel = bulkStream.getByteChannel(context)) {
			logger.debug("ETL bulk appends for pv " + pvName);
			ByteBuffer buf = ByteBuffer.allocate(1024*1024);
			while (srcChannel.read(buf) > 0) {
				buf.flip();
				do {
					destChannel.write(buf);
				} while (buf.remaining() > 0);
				buf.clear();
			}
		}

		try { 
			// Update the last known timestamp and the like...
			updateStateBasedOnExistingFile(pvName, pvPath, bulkStream);
		} finally { 
			// Close the current stream first and set it to null.
			if(this.os != null) try { this.os.close(); } catch(Throwable t) {}
			this.os = null;
		}
		return true;
	}	
}