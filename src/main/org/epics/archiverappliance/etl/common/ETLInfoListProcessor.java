package org.epics.archiverappliance.etl.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLInfo;
import org.epics.archiverappliance.etl.ETLInfoListStatistics;
import org.epics.archiverappliance.etl.StorageMetrics;

import java.io.IOException;
import java.util.List;

public abstract class ETLInfoListProcessor {

    private static final Logger logger = LogManager.getLogger(ETLInfoListProcessor.class.getName());

    public static boolean deleteSrcStreamWhenOutOfSpace(
            List<ETLInfo> infoItems,
            OutOfSpaceHandling outOfSpaceHandling,
            List<ETLInfo> movedList,
            ETLStage etlStage,
            String pvName) {

        if (outOfSpaceHandling == OutOfSpaceHandling.DELETE_SRC_STREAMS_WHEN_OUT_OF_SPACE) {
            logger.error("Not enough space on dest. Deleting src streams "
                    + infoItems.stream().map(ETLInfo::getKey).toList());
            movedList.addAll(infoItems);
            etlStage.outOfSpaceChunkDeleted();
            return true;
        } else if (outOfSpaceHandling == OutOfSpaceHandling.SKIP_ETL_WHEN_OUT_OF_SPACE) {
            logger.warn("Not enough space on dest. Skipping ETL this time for pv " + pvName);
            return false;
        } else {
            // By default, we use the DELETE_SRC_STREAMS_IF_FIRST_DEST_WHEN_OUT_OF_SPACE...
            if (etlStage.getLifetimeorder() == 0) {
                logger.error("Not enough space on dest. Deleting src stream "
                        + infoItems.stream().map(ETLInfo::getKey).toList());
                movedList.addAll(infoItems);
                etlStage.outOfSpaceChunkDeleted();
                return true;
            } else {
                logger.warn("Not enough space on dest. Skipping ETL this time for pv " + pvName);
                return false;
            }
        }
    }

    public static boolean notEnoughFreeSpace(
            long sizeOfSrcStream, ETLDest destMetrics, ETLStage etlStage, String key, String pvName)
            throws IOException {

        if (sizeOfSrcStream > 0 && destMetrics instanceof StorageMetrics) {
            long freeSpace = ((StorageMetrics) destMetrics).getUsableSpace(etlStage.getMetricsForLifetime());
            long freeSpaceBuffer = 1024 * 1024;
            // We leave space for at lease freeSpaceBuffer in the dest so that you can login and have some
            // room to repair damage coming in from an out of space condition.
            long estimatedSpaceNeeded = sizeOfSrcStream + freeSpaceBuffer;
            if (freeSpace < estimatedSpaceNeeded) {

                logger.error("No space on dest when moving ETLInfo with key = " + key
                        + " for PV " + pvName + " as we estimate we need "
                        + estimatedSpaceNeeded + " bytes but we only have " + freeSpace);
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    public static void checkAppendStatus(String pvName, boolean status, String key, PartitionGranularity granularity) {
        if (status) {
            if (logger.isDebugEnabled()) {
                logger.debug("Successfully appended ETLInfo with key = " + key
                        + " for PV " + pvName + "itemInfo partitionGranularity = "
                        + granularity);
            }
        } else {
            logger.warn("Invalid status when processing ETLInfo with key = " + key
                    + " for PV " + pvName + "itemInfo partitionGranularity = "
                    + granularity);
        }
    }

    ETLDest curETLDest;

    public ETLInfoListProcessor(ETLDest curETLDest) {
        this.curETLDest = curETLDest;
    }

    public ETLDest getCurETLDest() {
        return curETLDest;
    }

    protected abstract ETLInfoListStatistics process(
            List<ETLInfo> etlInfoList, String pvName, List<ETLInfo> movedList, ETLStage etlStage, ETLContext etlContext)
            throws IOException;
}
