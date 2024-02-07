package edu.stanford.slac.archiverappliance.plain.parquet;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.data.PartionedTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
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

import static edu.stanford.slac.archiverappliance.plain.parquet.ParquetInfo.fetchFileInfo;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

/**
 * ETL Parquet files stream, provides access to the list of parquet files for combination or streaming events.
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
            this.firstFileInfo = fetchFileInfo(paths.get(0));
        }
        return this.firstFileInfo;
    }

    private ParquetInfo getLastFileInfo() {
        if (lastFileInfo == null) {
            this.lastFileInfo = fetchFileInfo(paths.get(paths.size() - 1));
        }
        return this.lastFileInfo;
    }

    @Override
    public void close() throws IOException {
        /* Nothing to close */
    }

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
        var hadoopPaths = paths.stream().map(p -> new org.apache.hadoop.fs.Path(p.toUri()));
        var builders = hadoopPaths.map(ProtoParquetReader::builder);
        if (this.startTime != null && this.endTime != null) {
            YearSecondTimestamp startYst = TimeUtils.convertToYearSecondTimestamp(startTime);
            YearSecondTimestamp endYst = TimeUtils.convertToYearSecondTimestamp(endTime);
            // if no overlap in year return empty
            YearSecondTimestamp firstEventTime = ((PartionedTime) this.getFirstEvent()).getYearSecondTimestamp();
            YearSecondTimestamp lastEventTime =
                    ((PartionedTime) getLastFileInfo().getLastEvent()).getYearSecondTimestamp();
            if (endYst.compareTo(firstEventTime) < 0 || startYst.compareTo(lastEventTime) > 0) {
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

    /**
     * @param context BasicContext
     */
    @Override
    public Event getFirstEvent(BasicContext context) throws IOException {
        return this.getFirstEvent();
    }

    public Event getFirstEvent() {
        return getFirstFileInfo().getFirstEvent();
    }

    private record TimePeriod(YearSecondTimestamp startYst, YearSecondTimestamp endYst) {

        FilterCompat.Filter filter() {

            FilterPredicate predicate = and(
                    // gtEq start
                    or(
                            and(
                                    eq(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), startYst.getSecondsintoyear()),
                                    gtEq(intColumn(ParquetInfo.NANOSECONDS_COLUMN_NAME), startYst.getNano())),
                            gtEq(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), startYst.getSecondsintoyear() + 1)),
                    // ltEq end
                    or(
                            lt(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), endYst.getSecondsintoyear()),
                            and(
                                    eq(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), endYst.getSecondsintoyear()),
                                    ltEq(intColumn(ParquetInfo.NANOSECONDS_COLUMN_NAME), endYst.getNano()))));

            return FilterCompat.get(predicate);
        }
    }
}
