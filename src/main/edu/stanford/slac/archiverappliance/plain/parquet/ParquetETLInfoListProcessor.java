package edu.stanford.slac.archiverappliance.plain.parquet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLInfo;
import org.epics.archiverappliance.etl.ETLInfoListStatistics;
import org.epics.archiverappliance.etl.common.ETLInfoListProcessor;
import org.epics.archiverappliance.etl.common.ETLPVLookupItems;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class ParquetETLInfoListProcessor extends ETLInfoListProcessor {
    private static final Logger logger = LogManager.getLogger(ParquetETLInfoListProcessor.class.getName());

    ParquetETLInfoListProcessor(ETLDest etlDest) {
        super(etlDest);
    }

    private static List<List<ETLInfo>> determinePathsPerDestGranularity(
            List<ETLInfo> etlInfoList, PartitionGranularity destGranularity) {
        if (etlInfoList.isEmpty()) {
            return Collections.emptyList();
        }
        List<List<ETLInfo>> pathsPerDestGranularity = new ArrayList<>();
        pathsPerDestGranularity.add(new ArrayList<>());
        Instant nextPartitionFirstSecond = TimeUtils.getNextPartitionFirstSecond(
                etlInfoList.get(0).getFirstEvent().getEventTimeStamp(), destGranularity);
        for (ETLInfo infoItem : etlInfoList) {
            if (infoItem.getFirstEvent().getEventTimeStamp().isBefore(nextPartitionFirstSecond)) {
                pathsPerDestGranularity.get(pathsPerDestGranularity.size() - 1).add(infoItem);
            } else {
                pathsPerDestGranularity.add(new ArrayList<>(Collections.singletonList(infoItem)));
                nextPartitionFirstSecond = TimeUtils.getNextPartitionFirstSecond(
                        infoItem.getFirstEvent().getEventTimeStamp(), destGranularity);
            }
        }
        return pathsPerDestGranularity;
    }

    @Override
    protected ETLInfoListStatistics process(
            List<ETLInfo> etlInfoList,
            String pvName,
            List<ETLInfo> movedList,
            ETLPVLookupItems lookupItem,
            ETLContext etlContext)
            throws IOException {

        long totalSrcBytes = 0;
        long time4checkSizes = 0;
        long time4prepareForNewPartition = 0;
        long time4appendToETLAppendData = 0;
        PartitionGranularity destGranularity = getCurETLDest().getPartitionGranularity();
        List<List<ETLInfo>> pathsPerDestGranularity = determinePathsPerDestGranularity(etlInfoList, destGranularity);

        for (List<ETLInfo> etlInfosToCombine : pathsPerDestGranularity) {

            long checkSzStart = System.currentTimeMillis();
            long sizeOfSrcStreams =
                    etlInfoList.stream().mapToLong(ETLInfo::getSize).sum();
            String key =
                    etlInfosToCombine.stream().map(ETLInfo::getKey).toList().toString();
            if (notEnoughFreeSpace(sizeOfSrcStreams, getCurETLDest(), lookupItem, key, pvName)) {
                if (deleteSrcStreamWhenOutOfSpace(
                        etlInfosToCombine, lookupItem.getOutOfSpaceHandling(), movedList, lookupItem, pvName)) {

                    return new ETLInfoListStatistics(
                            time4checkSizes, time4prepareForNewPartition, time4appendToETLAppendData, totalSrcBytes);
                }
            }
            long checkSzEnd = System.currentTimeMillis();
            time4checkSizes = time4checkSizes + (checkSzEnd - checkSzStart);
            long appendDataStart = System.currentTimeMillis();
            List<Path> paths = etlInfosToCombine.stream()
                    .flatMap(item -> {
                        try {
                            return ((ParquetBackedPBEventFileStream) item.getEv()).getPaths().stream();
                        } catch (IOException e) {
                            logger.error("Failed to combine Parquet files: ", e);
                        }
                        return Stream.empty();
                    })
                    .toList();
            EventStream stream = new ParquetBackedPBEventFileStream(
                    pvName, paths, etlInfoList.get(0).getType(), null, null);

            boolean status = getCurETLDest().appendToETLAppendData(pvName, stream, etlContext);

            long appendDataEnd = System.currentTimeMillis();
            checkAppendStatus(pvName, status, key, etlInfosToCombine.get(0).getGranularity());

            time4appendToETLAppendData = time4appendToETLAppendData + (appendDataEnd - appendDataStart);
            movedList.addAll(etlInfosToCombine);
        }

        return new ETLInfoListStatistics(
                time4checkSizes, time4prepareForNewPartition, time4appendToETLAppendData, totalSrcBytes);
    }
}
