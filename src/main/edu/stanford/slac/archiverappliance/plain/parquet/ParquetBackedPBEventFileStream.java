package edu.stanford.slac.archiverappliance.plain.parquet;

import static edu.stanford.slac.archiverappliance.plain.parquet.ParquetInfo.fetchFileInfo;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.data.PartionedTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.proto.ProtoParquetReader;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.EmptyEventIterator;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

/**
 * An {@link org.epics.archiverappliance.EventStream} implementation that reads data from one or more Parquet files.
 * <p>
 * This class serves two primary purposes:
 * <ol>
 *     <li><b>Data Retrieval:</b> It can stream events from a list of Parquet files, applying time-based
 *     filters using Parquet's predicate pushdown for efficient querying.</li>
 *     <li><b>Optimized ETL:</b> It implements {@link ETLParquetFilesStream}, allowing it to act as a logical
 *     concatenation of multiple source files. The {@link ParquetETLInfoListProcessor} uses this capability
 *     to combine smaller Parquet files (e.g., hourly) into larger ones (e.g., daily) without fully
 *     deserializing and re-serializing the data, significantly improving ETL performance.</li>
 * </ol>
 *
 * @see ParquetPlainFileHandler
 * @see ParquetETLInfoListProcessor
 * @see ETLParquetFilesStream
 */
public class ParquetBackedPBEventFileStream implements ETLParquetFilesStream, RemotableOverRaw {
    private static final Logger logger = LogManager.getLogger(ParquetBackedPBEventFileStream.class.getName());
    private final String pvName;
    private final ArchDBRTypes type;
    private final List<Path> paths;
    private final Instant startTime;
    private final Instant endTime;
    private ParquetInfo firstFileInfo;
    private ParquetInfo lastFileInfo;
    private RemotableEventStreamDesc desc;

    public ParquetBackedPBEventFileStream(String pvName, Path path, ArchDBRTypes type) {
        this(pvName, List.of(path), type, null, null);
    }

    public ParquetBackedPBEventFileStream(String pvName, Path path, ArchDBRTypes type, ParquetInfo fileInfo) {
        this(pvName, List.of(path), type, null, null, fileInfo);
    }

    public ParquetBackedPBEventFileStream(
            String pvName, List<Path> paths, ArchDBRTypes type, Instant startTime, Instant endTime) {
        this.pvName = pvName;
        this.paths = paths;
        this.type = type;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public ParquetBackedPBEventFileStream(
            String pvName,
            List<Path> paths,
            ArchDBRTypes type,
            Instant startTime,
            Instant endTime,
            ParquetInfo fileInfo) {
        this.pvName = pvName;
        this.paths = paths;
        this.type = type;
        this.startTime = startTime;
        this.endTime = endTime;

        this.firstFileInfo = fileInfo;
    }

    /**
     * Adjusts the requested time range to fit within the actual boundaries of the data in the files.
     * This prevents attempts to read data outside the available range.
     *
     * @param startYst             The requested start time.
     * @param endYst               The requested end time.
     * @param firstFileEventTime   The timestamp of the very first event in the file sequence.
     * @param lastFileEventTime    The timestamp of the very last event in the file sequence.
     * @param firstBeforeEventTime The timestamp of the last event before the requested start time.
     * @return A {@link TimePeriod} representing the effective time range for filtering.
     */
    private static TimePeriod trimDates(
            YearSecondTimestamp startYst,
            YearSecondTimestamp endYst,
            YearSecondTimestamp firstFileEventTime,
            YearSecondTimestamp lastFileEventTime,
            YearSecondTimestamp firstBeforeEventTime) {
        // if yst start before file yst start reset seconds to 0 and year to file year
        YearSecondTimestamp timePeriodStartYst = startYst;
        if (startYst.compareTo(firstFileEventTime) < 0) {
            timePeriodStartYst = firstFileEventTime;
        } else if (startYst.compareTo(firstBeforeEventTime) > 0) {
            timePeriodStartYst = firstBeforeEventTime;
        }

        // if end yst after file yst, set endYst to last timestamp
        YearSecondTimestamp timePeriodEndYst = endYst;
        if (endYst.compareTo(lastFileEventTime) > 0) {
            timePeriodEndYst = lastFileEventTime;
        }

        return new TimePeriod(timePeriodStartYst, timePeriodEndYst);
    }

    @Override
    public ParquetInfo getFirstFileInfo() {
        if (firstFileInfo == null) {
            this.firstFileInfo = fetchFileInfo(paths.getFirst());
        }
        return this.firstFileInfo;
    }

    private ParquetInfo getLastFileInfo() {
        if (lastFileInfo == null) {
            this.lastFileInfo = fetchFileInfo(paths.getLast());
        }
        return this.lastFileInfo;
    }

    @Override
    public void close() throws IOException {
        /* Nothing to close */
    }

    /**
     * Creates the final event iterator that will read from the configured Parquet file readers.
     *
     * @param builders A list of configured ParquetReader builders.
     * @return An iterator over the events.
     * @throws IOException if an I/O error occurs.
     */
    private Iterator<Event> createEventIterator(List<ParquetReader.Builder<Object>> builders) throws IOException {
        return new ParquetBackedPBEventIterator(
                builders,
                DBR2PBTypeMapping.getPBClassFor(this.type).getUnmarshallingFromEpicsEventConstructor(),
                this.getDescription().getYear());
    }

    @Override
    public String toString() {
        return "ParquetBackedPBEventFileStream{" + "pvName='"
                + pvName + '\'' + ", type="
                + type + ", paths="
                + paths + ", startTime="
                + startTime + ", endTime="
                + endTime + ", firstFileInfo="
                + firstFileInfo + ", lastFileInfo="
                + lastFileInfo + ", desc="
                + desc + '}';
    }

    @Override
    public Iterator<Event> iterator() {
        var builders = paths.stream().map(LocalInputFile::new).map(ProtoParquetReader::builder);
        if (this.startTime != null && this.endTime != null) {
            YearSecondTimestamp startYst = TimeUtils.convertToYearSecondTimestamp(startTime);
            YearSecondTimestamp endYst = TimeUtils.convertToYearSecondTimestamp(endTime);
            // if no overlap in year return empty
            YearSecondTimestamp firstEventTime = ((PartionedTime) this.getFirstEvent()).getYearSecondTimestamp();
            YearSecondTimestamp lastEventTime = (getLastFileInfo().getLastEvent()).getYearSecondTimestamp();
            if (endYst.compareTo(firstEventTime) < 0) {
                return new EmptyEventIterator();
            }
            YearSecondTimestamp lastEventBeforeTime = getLastEventBeforeTime(startYst);
            builders = builders.map(
                    b -> b.withFilter(trimDates(startYst, endYst, firstEventTime, lastEventTime, lastEventBeforeTime)
                            .filter()));
        }
        try {
            return createEventIterator(builders.toList());
        } catch (IOException ex) {

            logger.error(ex.getMessage(), ex);
            return new EmptyEventIterator();
        }
    }

    /**
     * Finds the timestamp of the last event occurring at or before the given start time.
     * This is used to adjust the query window to include the sample immediately preceding the start time.
     * The expensive search is only performed if the stream consists of a single file.
     *
     * @param startYst The start timestamp of the user's query.
     * @return The timestamp of the event before {@code startYst}, or {@code startYst} if none is found.
     */
    private YearSecondTimestamp getLastEventBeforeTime(YearSecondTimestamp startYst) {
        YearSecondTimestamp lastEventBeforeTime;
        if (paths.size() == 1) {
            DBRTimeEvent event = getLastFileInfo().getLastEventBefore(startYst);
            if (event != null) {
                lastEventBeforeTime = event.getYearSecondTimestamp();
            } else {
                lastEventBeforeTime = startYst;
            }
        } else {
            lastEventBeforeTime = startYst;
        }
        return lastEventBeforeTime;
    }

    @Override
    public RemotableEventStreamDesc getDescription() {
        if (desc == null) {
            desc = new RemotableEventStreamDesc(this.pvName, getFirstFileInfo());
        }

        return desc;
    }

    @Override
    public List<Path> getPaths() {
        return this.paths;
    }

    @Override
    public Event getFirstEvent(BasicContext context) throws IOException {
        return this.getFirstEvent();
    }

    public Event getFirstEvent() {
        return getFirstFileInfo().getFirstEvent();
    }

    /**
     * A private helper record to encapsulate a time period and generate a corresponding Parquet filter.
     */
    private record TimePeriod(YearSecondTimestamp startYst, YearSecondTimestamp endYst) {
        /**
         * Creates a Parquet {@link FilterCompat.Filter} for the time period [startYst, endYst].
         *
         * @return The filter predicate.
         */
        FilterCompat.Filter filter() {

            FilterPredicate predicate = and(
                    // gtEq start
                    or(
                            and(
                                    eq(intColumn(ParquetInfo.ColumnName.SECONDS.key), startYst.getSecondsintoyear()),
                                    gtEq(intColumn(ParquetInfo.ColumnName.NANOSECONDS.key), startYst.getNano())),
                            gtEq(intColumn(ParquetInfo.ColumnName.SECONDS.key), startYst.getSecondsintoyear() + 1)),
                    // ltEq end
                    or(
                            lt(intColumn(ParquetInfo.ColumnName.SECONDS.key), endYst.getSecondsintoyear()),
                            and(
                                    eq(intColumn(ParquetInfo.ColumnName.SECONDS.key), endYst.getSecondsintoyear()),
                                    ltEq(intColumn(ParquetInfo.ColumnName.NANOSECONDS.key), endYst.getNano()))));

            return FilterCompat.get(predicate);
        }
    }
}
