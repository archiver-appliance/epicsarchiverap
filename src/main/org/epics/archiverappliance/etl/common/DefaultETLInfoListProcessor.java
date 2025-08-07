package org.epics.archiverappliance.etl.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLInfo;
import org.epics.archiverappliance.etl.ETLInfoListStatistics;

import java.io.IOException;
import java.util.List;

public class DefaultETLInfoListProcessor extends ETLInfoListProcessor {
    private static final Logger logger = LogManager.getLogger(DefaultETLInfoListProcessor.class.getName());

    public DefaultETLInfoListProcessor(ETLDest curETLDest) {
        super(curETLDest);
    }

    @Override
    protected ETLInfoListStatistics process(
            List<ETLInfo> etlInfoList, String pvName, List<ETLInfo> movedList, ETLStage etlStage, ETLContext etlContext)
            throws IOException {

        long totalSrcBytes = 0;
        long time4checkSizes = 0;
        long time4prepareForNewPartition = 0;
        long time4appendToETLAppendData = 0;
        for (ETLInfo infoItem : etlInfoList) {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing ETLInfo with key = " + infoItem.getKey() + " for PV " + pvName
                        + "itemInfo partitionGranularity = "
                        + infoItem.getGranularity().toString() + " and size " + infoItem.getSize());
            }

            long checkSzStart = System.currentTimeMillis();
            long sizeOfSrcStream = infoItem.getSize();
            totalSrcBytes += sizeOfSrcStream;
            if (notEnoughFreeSpace(sizeOfSrcStream, curETLDest, etlStage, infoItem.getKey(), pvName)) {
                if (deleteSrcStreamWhenOutOfSpace(
                        List.of(infoItem), etlStage.getOutOfSpaceHandling(), movedList, etlStage, pvName)) {
                    continue;
                } else {
                    break;
                }
            }
            long checkSzEnd = System.currentTimeMillis();
            time4checkSizes = time4checkSizes + (checkSzEnd - checkSzStart);

            try (EventStream stream = infoItem.getEv()) {
                long time2 = System.currentTimeMillis();
                time4prepareForNewPartition = time4prepareForNewPartition + System.currentTimeMillis() - time2;
                long time3 = System.currentTimeMillis();
                boolean status = this.curETLDest.appendToETLAppendData(pvName, stream, etlContext);
                movedList.add(infoItem);
                time4appendToETLAppendData = time4appendToETLAppendData + System.currentTimeMillis() - time3;
                checkAppendStatus(pvName, status, infoItem.getKey(), infoItem.getGranularity());
            } catch (IOException ex) {
                // TODO What do we do in the case of exceptions? Do we remove the source still? Do we stop the
                // engine from recording this PV?
                logger.error("Exception processing " + infoItem.getKey(), ex);
            }
        }
        return new ETLInfoListStatistics(
                time4checkSizes, time4prepareForNewPartition, time4appendToETLAppendData, totalSrcBytes);
    }
}
