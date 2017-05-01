/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;

/**
 * A utility class with a bunch of methods that operate on the path names used by the PlainPB storage plugin.
 * 
 * @author mshankar
 *
 */
public class PlainPBPathNameUtility {
	private static final Logger logger = Logger.getLogger(PlainPBPathNameUtility.class);

	public static Path getPathNameForTime(PlainPBStoragePlugin plugin, String pvName, long epochSeconds, ArchPaths paths, PVNameToKeyMapping pv2key) throws IOException {
		return getPathNameForTime(plugin.getRootFolder(), pvName, epochSeconds, plugin.getPartitionGranularity(), paths, plugin.getCompressionMode(), pv2key);
	}
	public static Path getPathNameForTime(String rootFolder, String pvName, long epochSeconds, PartitionGranularity partitionGranularity, ArchPaths paths, CompressionMode compressionMode, PVNameToKeyMapping pv2key) throws IOException {
		return getFileName(rootFolder, pvName, epochSeconds, ".pb", partitionGranularity, false, paths, compressionMode, pv2key);
	}

	public static Path getSparsifiedPathNameForTime(PlainPBStoragePlugin plugin, String pvName, long epochSeconds, ArchPaths paths, PVNameToKeyMapping pv2key) throws IOException {
		return getSparsifiedPathNameForTime(plugin.getRootFolder(), pvName, epochSeconds, plugin.getPartitionGranularity(), paths, plugin.getCompressionMode(), pv2key);
	}
	public static Path getSparsifiedPathNameForTime(String rootFolder, String pvName, long epochSeconds, PartitionGranularity partitionGranularity, ArchPaths paths, CompressionMode compressionMode, PVNameToKeyMapping pv2key) throws IOException {
		return getFileName(rootFolder, pvName, epochSeconds, ".pbs", partitionGranularity, false, paths, compressionMode, pv2key);
	}
	
	public static Path getPathNameForTime(String rootFolder, String pvName, long epochSeconds, boolean useReducedData, PartitionGranularity partitionGranularity, ArchPaths paths, CompressionMode compressionMode, PVNameToKeyMapping pv2key) throws IOException {
		if(useReducedData) {
			// Check to see if we have sparsified data
			Path sparsifiedPathName = getFileName(rootFolder, pvName, epochSeconds, ".pbs", partitionGranularity, false, paths, compressionMode, pv2key);
			if(Files.exists(sparsifiedPathName)) return sparsifiedPathName;
			logger.info("User is ok with sparsified data for PV " + pvName + " however, we do not have a sparsified version of the file " + sparsifiedPathName.toAbsolutePath().toString());
			return getPathNameForTime(rootFolder, pvName, epochSeconds, partitionGranularity, paths, CompressionMode.NONE, pv2key);
		} else {
			// User does not want reduced data; so we return the raw data.
			return getPathNameForTime(rootFolder, pvName, epochSeconds, partitionGranularity, paths, CompressionMode.NONE, pv2key);
		}
	}
	
	/**
	 * The PlainPBStorage plugin has a naming scheme that provides much information.
	 * This class encapsulates the potential start and end times of a particular chunk.
	 */
	static class StartEndTimeFromName {
		DateTime pathDataStartTime;
		DateTime pathDataEndTime;
		long chunkStartEpochSeconds;
		long chunkEndEpochSeconds;
		
		/**
		 * Determine the chunk start anf end times from the name
		 * @param pvName Name of the PV. 
		 * @param pathName The name of the file (without the directory part).
		 * @param pvFinalNameComponent The substring of the PV that contributes to the file name. For example for a PV ABC:DEF, we convert to rootFolder/ABC/DEF:2012.... This is the DEF part of this pv name.
		 * @param granularity Partition granularity of the file.
		 * @throws IOException &emsp;
		 */
		StartEndTimeFromName(String pvName, String pathName, String pvFinalNameComponent, PartitionGranularity granularity) throws IOException {
			String afterpvname = pathName.substring(pvFinalNameComponent.length());
			// logger.debug("After pvName, name of the file is " + afterpvname);
			String justtheTimeComponent = afterpvname.split("\\.")[0];
			// logger.debug("Just the time component is " + justtheTimeComponent);
			String[] timecomponents = justtheTimeComponent.split("_");

			switch(granularity) {
			case PARTITION_YEAR: {
				if(timecomponents.length != 1) {
					throw new IOException("We cannot mix and match partitions in a folder. Skipping " + pathName + " when including yearly data for PV " + pvName);
				}
				int year = Integer.parseInt(timecomponents[0]);
				// logger.debug("year: " + year + " for " + fileName);
				pathDataStartTime = new DateTime(year, 1, 1, 0, 0, 0, DateTimeZone.UTC);
				pathDataEndTime = pathDataStartTime.plusYears(1).minusSeconds(1);
				break;
			}
			case PARTITION_MONTH: {
				if(timecomponents.length != 2) {
					throw new IOException("We cannot mix and match partitions in a folder. Skipping " + pathName + " when including monthly data for PV " + pvName);
				}
				int year = Integer.parseInt(timecomponents[0]);
				int month = Integer.parseInt(timecomponents[1]);
				
				pathDataStartTime = new DateTime(year, month, 1, 0, 0, 0, DateTimeZone.UTC);
				pathDataEndTime = pathDataStartTime.plusMonths(1).minusSeconds(1);
				break;
			}
			case PARTITION_DAY: {
				if(timecomponents.length != 3) {
					throw new IOException("We cannot mix and match partitions in a folder. Skipping " + pathName + " when including daily data for PV " + pvName);
				}
				int year = Integer.parseInt(timecomponents[0]);
				int month = Integer.parseInt(timecomponents[1]);
				int day = Integer.parseInt(timecomponents[2]);
				
				pathDataStartTime = new DateTime(year, month, day, 0, 0, 0, DateTimeZone.UTC);
				pathDataEndTime = new DateTime(year, month, day, 23, 59, 59, DateTimeZone.UTC);
				break;
			}
			case PARTITION_HOUR: {
				if(timecomponents.length != 4) {
					throw new IOException("We cannot mix and match partitions in a folder. Skipping " + pathName + " when including hourly data for PV " + pvName);
				}
				int year = Integer.parseInt(timecomponents[0]);
				int month = Integer.parseInt(timecomponents[1]);
				int day = Integer.parseInt(timecomponents[2]);
				int hour = Integer.parseInt(timecomponents[3]);
				
				pathDataStartTime = new DateTime(year, month, day, hour, 0, 0, DateTimeZone.UTC);
				pathDataEndTime = new DateTime(year, month, day, hour, 59, 59, DateTimeZone.UTC);
				break;
			}
			case PARTITION_5MIN:
			case PARTITION_15MIN:
			case PARTITION_30MIN: {
				if(timecomponents.length != 5) {
					throw new IOException("We cannot mix and match partitions in a folder. Skipping " + pathName + " when including minutely data for PV " + pvName + " and granularity " + granularity.toString());
				}
				int year = Integer.parseInt(timecomponents[0]);
				int month = Integer.parseInt(timecomponents[1]);
				int day = Integer.parseInt(timecomponents[2]);
				int hour = Integer.parseInt(timecomponents[3]);
				int min = Integer.parseInt(timecomponents[4]);
				
				pathDataStartTime = new DateTime(year, month, day, hour, min, 0, DateTimeZone.UTC);
				pathDataEndTime = new DateTime(year, month, day, hour, min+granularity.getApproxMinutesPerChunk()-1, 59, DateTimeZone.UTC);
				break;
			}
			default:
				throw new UnsupportedOperationException();
			}
			// Convert the datetime to epoch seconds for convenience.
			chunkStartEpochSeconds = pathDataStartTime.getMillis()/1000;
			chunkEndEpochSeconds = pathDataEndTime.getMillis()/1000;
		}
	}

	/**
	 * Given a parent folder, this method returns a list of all the paths with data that falls within the specified timeframe. 
	 * We assume the pathnames follow the syntax used by the PlainPBStorage plugin.
	 * The alg for matching is based on this
	 *  <pre>
	 *       --------
	 *  [ ] [|] [ ] [|] [ ]
	 *  </pre>
	 *  For the chunks that are eliminated, either the end time of the chunk is less than the start time or the start time of the chunk is greater than the end time.
	 * @param parentFolder
	 * @param pvName
	 * @param start
	 * @param end
	 * @param granularity
	 * @return
	 */
	public static Path[] getPathsWithData(ArchPaths archPaths, String rootFolder, final String pvName, final Timestamp startts, final Timestamp endts, final String extension, final PartitionGranularity granularity, final CompressionMode compressionMode, PVNameToKeyMapping pv2key) throws IOException {
		final long reqStartEpochSeconds = TimeUtils.convertToEpochSeconds(startts);
		final long reqEndEpochSeconds = TimeUtils.convertToEpochSeconds(endts);
		String pvFinalNameComponent = getFinalNameComponent(pvName, pv2key);
		
		ArrayList<Path> retVal = new ArrayList<Path>();
		try(DirectoryStream<Path> paths = getDirectoryStreamsForPV(archPaths, rootFolder, pvName, extension, granularity, compressionMode, pv2key)) {
			for(Path path : paths) {
				String name = path.getFileName().toString();
				try {
					StartEndTimeFromName pathNameTimes = new StartEndTimeFromName(pvName, name, pvFinalNameComponent, granularity);

					if((pathNameTimes.chunkEndEpochSeconds < reqStartEpochSeconds) || (pathNameTimes.chunkStartEpochSeconds > reqEndEpochSeconds))  {
						logger.debug("File " + name + " did not match the times requested");
						continue;
					}
					logger.debug("File " + name + " matched the times requested");
					retVal.add(path);
				} catch(IOException ex) {
					logger.warn("Skipping file " + name + " when geting FilesWithData. Exception", ex);
					continue;
				}
			}
		} catch(NoSuchFileException nex) {
			logger.debug("Most likely the parent folder for this pv does not exist. Returning an empty list");
		}
		
		Collections.sort(retVal, new Comparator<Path>() {
			@Override
			public int compare(Path o1, Path o2) {
				return o1.getFileName().toString().compareTo(o2.getFileName().toString());
			}
		});
		
		return retVal.toArray(new Path[0]);
	}
	
	
	/**
	 * The PlainPB storage plugin partitions files according to time and partition granularity.
	 * At any particular point in time, we are only writing to one partition, the "current" partition. 
	 * For ETL, we need to know the partitions that are not being written into; that is, all the previous partitions.
	 * This is typically everything except the file for the current partition
	 * @param rootFolder - The root folder for the plugin
	 * @param pvName - The name of the PV
	 * @param currentTime - The time that we are running ETL for. To prevent border conditions, caller can add a buffer if needed.
	 * @param extension - The file extension.
	 * @param granularity - The granularity of this store.
	 * @return
	 */
	public static Path[] getPathsBeforeCurrentPartition(ArchPaths archPaths, String rootFolder, final String pvName, final Timestamp currentTime, final String extension, final PartitionGranularity granularity, final CompressionMode compressionMode, PVNameToKeyMapping pv2key) throws IOException {
		final long reqStartEpochSeconds = 1;
		final long reqEndEpochSeconds = TimeUtils.getPreviousPartitionLastSecond(TimeUtils.convertToEpochSeconds(currentTime), granularity);
		if(logger.isDebugEnabled()) logger.debug(pvName + ": Looking for files in " + rootFolder + " with data before " + TimeUtils.convertToISO8601String(reqEndEpochSeconds));
		
		return getPathsWithData(
				archPaths,
				rootFolder, 
				pvName, 
				TimeUtils.convertFromEpochSeconds(reqStartEpochSeconds, 0),
				TimeUtils.convertFromEpochSeconds(reqEndEpochSeconds, 0),
				extension,
				granularity,
				compressionMode,
				pv2key
				);
	}
	
	/**
	 * This method returns all the paths that could contain data for a PV sorted according to the name (which in our case should translate to time).
	 * @param rootFolder
	 * @param pvName
	 * @param extension
	 * @param granularity
	 * @return
	 * @throws IOException
	 */
	public static Path[] getAllPathsForPV(ArchPaths archPaths, String rootFolder, final String pvName, final String extension, final PartitionGranularity granularity, final CompressionMode compressionMode, PVNameToKeyMapping pv2key) throws IOException {
		ArrayList<Path> retval = new ArrayList<Path>();
		try(DirectoryStream<Path> paths = getDirectoryStreamsForPV(archPaths, rootFolder, pvName, extension, granularity, compressionMode, pv2key)) {
			for(Path path : paths) {
				retval.add(path);
			}


			Collections.sort(retval, new Comparator<Path>() {
				@Override
				public int compare(Path o1, Path o2) {
					return o1.getFileName().compareTo(o2.getFileName());
				}
			});
		} catch(NoSuchFileException nex) {
			logger.debug("Most likely the parent folder for this pv does not exist. Returning an empty list");
		}
		
		return retval.toArray(new Path[0]);
	}

	
	
	/**
	 * If a PV changes infrequently, we often will not have a sample in the given time frame.
	 * The getData contract asks that we return the most recent known sample; even if this sample's timestamp is before the requested start/end time.
	 * The way we do this is to ask for the file that potentially has most recent data before the start time.  
	 * We take advantage of the sorting nature of getAllPathsForPV and work our way from the back
	 * @param rootFolder
	 * @param pvName
	 * @param currentTime
	 * @param extension
	 * @param granularity
	 * @return
	 */
	public static Path getMostRecentPathBeforeTime(ArchPaths archPaths, String rootFolder, final String pvName, final Timestamp startts, final String extension, final PartitionGranularity granularity, final CompressionMode compressionMode, PVNameToKeyMapping pv2key) throws Exception {
		if(logger.isDebugEnabled()) logger.debug(pvName + ": Looking for most recent file before " + TimeUtils.convertToISO8601String(startts));
		Path[] paths = getAllPathsForPV(archPaths, rootFolder, pvName, extension, granularity, compressionMode, pv2key);
		if(paths == null || paths.length == 0) return null;
		
		long reqStartEpochSeconds = TimeUtils.convertToEpochSeconds(startts);
		String pvFinalNameComponent = getFinalNameComponent(pvName, pv2key);

		for(int i = paths.length-1; i >=0; i--) {
			Path path = paths[i];
			String name = path.getFileName().toString();
			try {
				StartEndTimeFromName fileNameTimes = new StartEndTimeFromName(pvName, name, pvFinalNameComponent, granularity);

				if(fileNameTimes.chunkStartEpochSeconds < reqStartEpochSeconds)  {
					logger.debug("File " + name + " is the latest chunk with data for pv " + pvName);
					return path;
				}
			} catch(IOException ex) {
				logger.warn("Skipping file " + name + " when geting FilesWithData. Exception", ex);
			}
		}
		
		if(logger.isDebugEnabled()) logger.debug(pvName + ": Did not find any file with data before " + TimeUtils.convertToISO8601String(startts));
		return null;
	}

	/**
	 * If a PV changes infrequently, we often will not have a sample in the given time frame.
	 * The getData contract asks that we return the most recent known sample; even if this sample's timestamp is before the requested start/end time.
	 * Another way we do this is to return the last event in the partition which ends before the start time.  
	 * We take advantage of the sorting nature of getAllPathsForPV and work our way from the back
	 * @param rootFolder
	 * @param pvName
	 * @param currentTime
	 * @param extension
	 * @param granularity
	 * @return
	 */
	public static Path getPreviousPartitionBeforeTime(ArchPaths archPaths, String rootFolder, final String pvName, final Timestamp startts, final String extension, final PartitionGranularity granularity, final CompressionMode compressionMode, PVNameToKeyMapping pv2key) throws Exception {
		if(logger.isDebugEnabled()) logger.debug(pvName + ": Looking for previous partition before " + TimeUtils.convertToISO8601String(startts));
		Path[] paths = getAllPathsForPV(archPaths, rootFolder, pvName, extension, granularity, compressionMode, pv2key);
		if(paths == null || paths.length == 0) return null;
		
		long reqStartEpochSeconds = TimeUtils.convertToEpochSeconds(startts);
		String pvFinalNameComponent = getFinalNameComponent(pvName, pv2key);

		for(int i = paths.length-1; i >=0; i--) {
			Path path = paths[i];
			String name = path.getFileName().toString();
			try {
				StartEndTimeFromName fileNameTimes = new StartEndTimeFromName(pvName, name, pvFinalNameComponent, granularity);

				if(fileNameTimes.chunkEndEpochSeconds < reqStartEpochSeconds)  {
					logger.debug("File " + name + " is the previous partition chunk with data for pv " + pvName);
					return path;
				}
			} catch(IOException ex) {
				logger.warn("Skipping file " + name + " when geting getPreviousPartitionBeforeTime. Exception", ex);
			}
		}
		
		if(logger.isDebugEnabled()) logger.debug(pvName + ": Did not find any previous partitions before " + TimeUtils.convertToISO8601String(startts));
		return null;
	}

	
	/**
	 * This method returns the path for a given pv for a given time based on the partitionGranularity
	 * @param rootFolder
	 * @param pvName
	 * @param epochSeconds
	 * @param extension
	 * @param partitionGranularity
	 * @return
	 */
	static Path getFileName(String rootFolder, String pvName, long epochSeconds, String extension, PartitionGranularity partitionGranularity, boolean createParentFolder, ArchPaths paths, CompressionMode compressionMode, PVNameToKeyMapping pv2key) throws IOException {
		String partitionNameComponent = TimeUtils.getPartitionName(epochSeconds, partitionGranularity);
		String pvKey = pv2key.convertPVNameToKey(pvName);
		String pvPathComponent = pvKey + partitionNameComponent + extension;
		switch(compressionMode) {
		case NONE: {
			Path path = paths.get(createParentFolder, rootFolder, pvPathComponent);
			return path;
		}
		case ZIP_PER_PV: {
			String zipPathComponent = pvKey + "_pb.zip!";
			Path path = paths.get(createParentFolder, rootFolder, zipPathComponent, pvPathComponent);
			return path;
		}
		default:
			throw new IOException("Unsupported compression mode " + compressionMode);
		}
	}
	
	/**
	 * A pv is mapped to a path that can span folders.
	 * This method returns the final name component of the pv -> folder/file mapping so that we can use in searching in the folder.
	 * @param pvName
	 * @return
	 */
	private static String getFinalNameComponent(String pvName, PVNameToKeyMapping pv2key) {
		Path pvPathAlone = Paths.get(pv2key.convertPVNameToKey(pvName));
		String pvFinalNameComponent = pvPathAlone.getFileName().toString();
		return pvFinalNameComponent;
	}

	/**
	 * A pv is mapped to a path that can span folders.
	 * This method returns the parent path of the pv; we search for pv data in this folder.
	 * 
	 * @param paths
	 * @param rootFolder
	 * @param pvName
	 * @param extension
	 * @param granularity
	 * @param compressionMode
	 * @return
	 */
	private static Path getParentPath(ArchPaths paths, String rootFolder, final String pvName, final PartitionGranularity granularity, CompressionMode compressionMode, PVNameToKeyMapping pv2key) throws IOException {
		String pvKey = pv2key.convertPVNameToKey(pvName);
		boolean createParentFolder = false; // should we create parent folder if it does not exist
		switch(compressionMode) {
		case NONE: {
			Path path = paths.get(createParentFolder, rootFolder, pvKey);
			return path.getParent();
		}
		case ZIP_PER_PV: {
			String zipPathComponent = pvKey + "_pb.zip!";
			Path path = paths.get(createParentFolder, rootFolder, zipPathComponent, pvKey);
			return path.getParent();
		}
		default:
			throw new IOException("Unsupported compression mode " + compressionMode);
		}
	}	
	
	/**
	 * Determines the times for a chunk simply from the file name.
	 * Bear in mind there is no guarantee that the file has data in this range. For that, @see PBFileInfo.
	 * @param pvName
	 * @param pbFile
	 * @param partitionGranularity
	 * @return
	 * @throws IOException
	 */
	public static StartEndTimeFromName determineTimesFromFileName(String pvName, String finalNameComponent, PartitionGranularity partitionGranularity, PVNameToKeyMapping pv2key) throws IOException {
		String pvFinalNameComponent = getFinalNameComponent(pvName, pv2key);
		logger.debug(pvName + ": Determining start and end times for " + finalNameComponent);
		StartEndTimeFromName fileNameTimes = new StartEndTimeFromName(pvName, finalNameComponent, pvFinalNameComponent, partitionGranularity);
		return fileNameTimes;
	}
	
	
	/**
	 * Returns a NIO2 directory stream for the PV based on the extension and partition granularity.
	 * The returned directory stream is not sorted; if you have logic that depends on a certain order, please sort before processing.
	 * @param rootFolder
	 * @param pvName
	 * @param extension
	 * @param granularity
	 * @return
	 * @throws IOException
	 */
	private static DirectoryStream<Path> getDirectoryStreamsForPV(ArchPaths paths, String rootFolder, final String pvName, final String extension, final PartitionGranularity granularity, CompressionMode compressionMode, PVNameToKeyMapping pv2key) throws IOException {
		try {
			Path parentFolder = getParentPath(paths, rootFolder, pvName, granularity, compressionMode, pv2key);
			String pvFinalNameComponent = getFinalNameComponent(pvName, pv2key);
			String matchGlob = pvFinalNameComponent + "*" + extension;
			logger.debug(pvName + ": Looking for " + matchGlob + " in parentFolder " + parentFolder.toString());

			return Files.newDirectoryStream(parentFolder, matchGlob);
		} catch(NotDirectoryException nex) {
			logger.debug("Possibly empty zip file when looking for data for pv " + pvName, nex);
			// Return an empty directory stream in this case.
			return new DirectoryStream<Path>() {
				@Override
				public void close() throws IOException {
				}

				@Override
				public Iterator<Path> iterator() {
					ArrayList<Path> emptyPaths = new ArrayList<Path>();
					return emptyPaths.iterator();
				}
			};
		}
	}
}
