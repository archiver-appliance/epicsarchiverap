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
import org.epics.archiverappliance.etl.common.ETLStage;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * A specialized {@link ETLInfoListProcessor} optimized for processing Parquet files.
 * <p>
 * This processor overrides the default ETL behavior to efficiently combine multiple
 * source Parquet files into a single destination file. Instead of de-serializing
 * and re-serializing every event, it creates a {@link ParquetBackedPBEventFileStream}
 * that logically concatenates the source files. This significantly reduces I/O and
 * CPU overhead during the ETL process, as the data is streamed directly from the
 * source files to the destination without being fully parsed in the application.
 *
 * @see ETLInfoListProcessor
 * @see ParquetPlainFileHandler
 * @see ParquetBackedPBEventFileStream
 */
public class ParquetETLInfoListProcessor extends ETLInfoListProcessor {
    private static final Logger logger = LogManager.getLogger(ParquetETLInfoListProcessor.class.getName());

    ParquetETLInfoListProcessor(ETLDest etlDest) {
        super(etlDest);
    }

    /**
     * Groups a list of ETLInfo objects into sub-lists based on the destination's partition granularity.
     * <p>
     * This is necessary to ensure that data from multiple source files (e.g., hourly files)
     * are correctly combined into a single destination file (e.g., a daily file).
     *
     * @param etlInfoList       The sorted list of source file information to be processed.
     * @param destGranularity   The partition granularity of the ETL destination.
     * @return A list of lists, where each inner list contains ETLInfo objects belonging to the same destination partition.
     */
    private static List<List<ETLInfo>> determinePathsPerDestGranularity(
            List<ETLInfo> etlInfoList, PartitionGranularity destGranularity) {
        if (etlInfoList.isEmpty()) {
            return Collections.emptyList();
        }
        List<List<ETLInfo>> pathsPerDestGranularity = new ArrayList<>();
        pathsPerDestGranularity.add(new ArrayList<>());
        Instant nextPartitionFirstSecond = TimeUtils.getNextPartitionFirstSecond(
                etlInfoList.getFirst().getFirstEvent().getEventTimeStamp(), destGranularity);
        for (ETLInfo infoItem : etlInfoList) {
            if (infoItem.getFirstEvent().getEventTimeStamp().isBefore(nextPartitionFirstSecond)) {
                pathsPerDestGranularity.getLast().add(infoItem);
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
            List<ETLInfo> etlInfoList, String pvName, List<ETLInfo> movedList, ETLStage etlStage, ETLContext etlContext)
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
            if (notEnoughFreeSpace(sizeOfSrcStreams, getCurETLDest(), etlStage, key, pvName)
                    && deleteSrcStreamWhenOutOfSpace(
                            etlInfosToCombine, etlStage.getOutOfSpaceHandling(), movedList, etlStage, pvName)) {

                return new ETLInfoListStatistics(
                        time4checkSizes, time4prepareForNewPartition, time4appendToETLAppendData, totalSrcBytes);
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
                    pvName, paths, etlInfoList.getFirst().getType(), null, null);

            boolean status = getCurETLDest().appendToETLAppendData(pvName, stream, etlContext);

            long appendDataEnd = System.currentTimeMillis();
            checkAppendStatus(pvName, status, key, etlInfosToCombine.getFirst().getGranularity());

            time4appendToETLAppendData = time4appendToETLAppendData + (appendDataEnd - appendDataStart);
            movedList.addAll(etlInfosToCombine);
        }

        return new ETLInfoListStatistics(
                time4checkSizes, time4prepareForNewPartition, time4appendToETLAppendData, totalSrcBytes);
    }
}
