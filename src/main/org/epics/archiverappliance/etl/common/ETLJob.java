package org.epics.archiverappliance.etl.common;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLInfo;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.etl.StorageMetrics;


/**
 * We schedule a ETLPVLookupItems with the appropriate thread using an ETLJob 
 * @author mshankar
 *
 */
public class ETLJob implements Runnable {
	private static Logger logger = Logger.getLogger(ETLJob.class.getName());
	private ETLPVLookupItems lookupItem;
	private boolean currentlyRunning = false;
	private long ETLRunStartEpochSeconds = 0;
	private Timestamp runAsIfAtTime = null;
	private Exception exceptionFromLastRun = null;
	
	public ETLJob(ETLPVLookupItems lookupItem) { 
		this.lookupItem = lookupItem;
		this.runAsIfAtTime = null;
	}

	/**
	 * Mostly used by ETL unit tests.
	 * @param lookupItem
	 * @param runAsIfAtTime
	 */
	public ETLJob(ETLPVLookupItems lookupItem, Timestamp runAsIfAtTime) {
		this.lookupItem = lookupItem;
		this.runAsIfAtTime = runAsIfAtTime;
	}

	@Override
	public void run() {
		try { 
			exceptionFromLastRun = null;
			if(this.runAsIfAtTime == null) { 
				Timestamp processingTime = TimeUtils.convertFromEpochSeconds(TimeUtils.getCurrentEpochSeconds(), 0);
				this.processETL(processingTime);
			} else { 
				this.processETL(runAsIfAtTime);
			}
		} catch(Exception e) { 
			logger.error("Exception processing ETL for " + lookupItem.toString(), e);
			exceptionFromLastRun = e;
		}
	}

	/**
	 * Main ETL algorithm.
	 * @param processingTime - Typically we'd use TimeUtils.now() for this. However, we can also run as if it's a certain time by using this parameter.
	 *
	 */
	public void processETL(Timestamp processingTime) throws IOException {
		String pvName = lookupItem.getPvName();
		String jobDesc = lookupItem.toString();
		if (currentlyRunning) {
			logger.error("The previous ETL job ("+jobDesc+") that began at " + ((ETLRunStartEpochSeconds != 0) ? TimeUtils.convertToHumanReadableString(ETLRunStartEpochSeconds) : "Unknown") + " is still running");
			return;
		}

		long time4getETLStreams=0;
		long time4checkSizes=0;
		long time4prepareForNewPartition=0;
		long time4appendToETLAppendData=0;
		long time4commitETLAppendData=0;
		long time4markForDeletion=0;
		long time4runPostProcessors=0;
		long time4executePostETLTasks=0;

		// We create a brand new context for each run.
		try(ETLContext etlContext = new ETLContext()) {
			currentlyRunning=true;
			ETLRunStartEpochSeconds = TimeUtils.getCurrentEpochSeconds();
			
			long pvETLStartEpochMilliSeconds = TimeUtils.getCurrentEpochMilliSeconds();

			if(logger.isDebugEnabled()) {
				logger.debug("Processing ETL for pv " + lookupItem.getPvName() + " from " + lookupItem.getETLSource().getDescription() + lookupItem.getETLDest().getDescription() + " as if it is " + TimeUtils.convertToHumanReadableString(processingTime));
			}

			ETLSource curETLSource = lookupItem.getETLSource();
			ETLDest curETLDest = lookupItem.getETLDest();
			assert(curETLSource != null);
			assert(curETLDest != null);
			
			StorageMetrics destMetrics = null;
			if ( curETLDest instanceof StorageMetrics) { 
				destMetrics = (StorageMetrics) curETLDest;
			}

			// Process each ETLInfo element in the list of ETLInfo
			// elements containing information about event streams
			// (e.g., files) ready for ETL for the current time and
			// PV name being processed.
			long time1=System.currentTimeMillis();
			long totalSrcBytes = 0;
			List<ETLInfo> ETLInfoList = curETLSource.getETLStreams(pvName, processingTime, etlContext);
			time4getETLStreams=time4getETLStreams+System.currentTimeMillis()-time1;
			if (ETLInfoList != null) {
				// Here we collect items which we will be moving and deleting respectively.
				List<ETLInfo> movedList = new LinkedList<ETLInfo>();
				List<ETLInfo> deleteList = new LinkedList<ETLInfo>();
				
				for (ETLInfo infoItem : ETLInfoList) {
					if(logger.isDebugEnabled()) {
						logger.debug("Processing ETLInfo with key = " + infoItem.getKey() + " for PV " + pvName + "itemInfo partitionGranularity = " + infoItem.getGranularity().toString());
					}
					
					long checkSzStart = System.currentTimeMillis();
					long sizeOfSrcStream = infoItem.getSize();
					totalSrcBytes += sizeOfSrcStream;
					if(sizeOfSrcStream > 0 && destMetrics != null) { 
						long freeSpace = destMetrics.getUsableSpace(lookupItem.getMetricsForLifetime());
						long freeSpaceClearance = lookupItem.getFreeSpaceClearance();
						// We leave space for at least freeSpaceClearance in the dest so that:
						// - You can login and have some room to repair damage coming in from an out of space condition.
						// - We avoid writing incomplete samples at the end (which would later be truncated).
						// - We avoid write operations taking a long time (presumably due to the system looking for free space).
						long estimatedSpaceNeeded = sizeOfSrcStream + freeSpaceClearance;
						if(freeSpace < estimatedSpaceNeeded) { 
							logger.error("No space on dest when moving ETLInfo with key = " + infoItem.getKey() + " for PV " + pvName + "itemInfo partitionGranularity = " + infoItem.getGranularity().toString() + " as we estimate we need " +  estimatedSpaceNeeded + " bytes but we only have " + freeSpace);
							if (handleFailedItem(infoItem, deleteList)) {
								continue;
							} else {
								break;
							}
						}
					}
					long checkSzEnd = System.currentTimeMillis();
					time4checkSizes=time4checkSizes+(checkSzEnd-checkSzStart);

					try(EventStream stream = infoItem.getEv()) {
						long time2=System.currentTimeMillis();
						boolean partitionPrepareResult = curETLDest.prepareForNewPartition(pvName, infoItem.getFirstEvent(), infoItem.getType(), etlContext);
						time4prepareForNewPartition=time4prepareForNewPartition+System.currentTimeMillis()-time2;
						if(logger.isDebugEnabled()) {
							if(!partitionPrepareResult) logger.debug("Destination partition already prepared for PV " + pvName + " with key = " + infoItem.getKey());
						}
						long time3=System.currentTimeMillis();
						boolean status = curETLDest.appendToETLAppendData(pvName, stream, etlContext);
						time4appendToETLAppendData=time4appendToETLAppendData+System.currentTimeMillis()-time3;
						if (status) {
							if(logger.isDebugEnabled()) {
								logger.debug("Successfully appended ETLInfo with key = " + infoItem.getKey() + " for PV " + pvName + "itemInfo partitionGranularity = " + infoItem.getGranularity().toString());
							}
						} else {
							logger.warn("Processing ETLInfo with key = " + infoItem.getKey() + " for PV " + pvName + "itemInfo partitionGranularity = " + infoItem.getGranularity().toString());
						}
					} catch(IOException ex) {
						logger.error("Exception processing " + infoItem.getKey() + ": " + ex.getMessage());
						if (handleFailedItem(infoItem, deleteList)) {
							continue;
						} else {
							break;
						}
					}
					
					movedList.add(infoItem);
				}

				// Concatenate any append data for the current ETLDest
				// destination to this destination.
				boolean commitSuccessful = false;
				try {
					long time7 = System.currentTimeMillis();
					commitSuccessful = curETLDest.commitETLAppendData(pvName, etlContext);
					time4commitETLAppendData = time4commitETLAppendData+System.currentTimeMillis()-time7;
				} catch (IOException e) {
					logger.error("Exception from commitETLAppendData: " + e.getMessage());
				}
				
				// Decide which source items should be deleted.
				for (ETLInfo infoItem : movedList) {
					if (commitSuccessful || getEffectiveOutOfSpaceHandling() == OutOfSpaceHandling.DELETE_SRC_STREAMS_WHEN_OUT_OF_SPACE) {
						deleteList.add(infoItem);
					}
				}
				
				// Mark for deletion all the items for which we have decided to to so.
				for (ETLInfo infoItem : deleteList) { 
					logger.debug("mark for deletion itemInfo key= " + infoItem.getKey());
					long time4=System.currentTimeMillis();
					curETLSource.markForDeletion(infoItem, etlContext);
					time4markForDeletion=time4markForDeletion+System.currentTimeMillis()-time4;
				}
				
				try {
					long time5=System.currentTimeMillis();
					curETLDest.runPostProcessors(pvName, lookupItem.getDbrType(), etlContext);
					time4runPostProcessors=time4runPostProcessors+System.currentTimeMillis()-time5;
				}catch (Exception e) {
					logger.error("Exception running post processors for pv " + pvName, e);      
				}
				logger.debug("Executing post ETL tasks for this run");
				long time6=System.currentTimeMillis();
				etlContext.executePostETLTasks();
				time4executePostETLTasks=time4executePostETLTasks+System.currentTimeMillis()-time6;
				logger.debug("Done executing post ETL tasks for this run");

				long pvETLEndEpochMilliSeconds = TimeUtils.getCurrentEpochMilliSeconds();
				lookupItem.addETLDurationInMillis(pvETLStartEpochMilliSeconds, pvETLEndEpochMilliSeconds);
				lookupItem.addInfoAboutDetailedTime(time4getETLStreams, time4checkSizes, time4prepareForNewPartition, time4appendToETLAppendData, time4commitETLAppendData, time4markForDeletion, time4runPostProcessors, time4executePostETLTasks, totalSrcBytes);
			} else { 
				logger.debug("There were no ETL streams when running ETL for " + jobDesc);
			}
		} catch(IOException ex) {
			logger.error("IOException processing ETL for pv " + lookupItem.getPvName(), ex);       
		} finally {
			currentlyRunning=false;
		}
	}
	
	private OutOfSpaceHandling getEffectiveOutOfSpaceHandling() {
		OutOfSpaceHandling outOfSpaceHandling = lookupItem.getOutOfSpaceHandling();
		if (outOfSpaceHandling == OutOfSpaceHandling.DELETE_SRC_STREAMS_IF_FIRST_DEST_WHEN_OUT_OF_SPACE) {
			if (lookupItem.getLifetimeorder() == 0) {
				return OutOfSpaceHandling.DELETE_SRC_STREAMS_WHEN_OUT_OF_SPACE;
			} else {
				return OutOfSpaceHandling.SKIP_ETL_WHEN_OUT_OF_SPACE;
			}
		}
		return outOfSpaceHandling;
	}
	
	private boolean handleFailedItem(ETLInfo infoItem, List<ETLInfo> deleteList) {
		if(getEffectiveOutOfSpaceHandling() == OutOfSpaceHandling.DELETE_SRC_STREAMS_WHEN_OUT_OF_SPACE) {
			deleteList.add(infoItem);
			lookupItem.outOfSpaceChunkDeleted();
			return true;
		} else { // outOfSpaceHandling == OutOfSpaceHandling.SKIP_ETL_WHEN_OUT_OF_SPACE
			return false;
		}
	}

	/**
	 * Was there an exception in the last ETL run for this job
	 * Mostly used by unit tests.
	 * @return
	 */
	public Exception getExceptionFromLastRun() {
		return exceptionFromLastRun;
	}
}
