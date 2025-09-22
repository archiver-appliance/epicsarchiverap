package edu.stanford.slac.archiverappliance.plain.parquet;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.proto.ProtoParquetReader;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;

/**
 * An implementation of {@link FileInfo} for Parquet files.
 * <p>
 * This class is responsible for reading and interpreting the metadata and data
 * within a single Parquet file. It provides methods to access
 * file-level information such as the PV name, data year, and data type.
 * It also offers wrappers methods for retrieving specific events, such as the
 * first, last, or an event at a particular time, by leveraging Parquet's
 * internal metadata (like column statistics) and predicate pushdown filters.
 *
 * @see ParquetPlainFileHandler
 * @see FileInfo
 */
public class ParquetInfo extends FileInfo {
    /**
     * Default Parquet read options.
     */
    static final ParquetReadOptions baseOptions = (new ParquetReadOptions.Builder()).build();

    public enum MetaDataKey {
        PV_NAME("pvName"),
        YEAR("year"),
        TYPE("ArchDBRType");

        final String key;

        MetaDataKey(String key) {
            this.key = key;
        }
    }

    public enum ColumnName {
        SECONDS("secondsintoyear"),
        NANOSECONDS("nano");
        final String key;

        ColumnName(String key) {
            this.key = key;
        }
    }

    private static final Logger logger = LogManager.getLogger(ParquetInfo.class);
    final InputFile inputFile;
    String pvName;
    short dataYear;
    ArchDBRTypes archDBRTypes;
    ParquetMetadata footer;
    ParquetFileReader fileReader;
    boolean fetchedLastEvent = false;
    boolean fetchedFirstEvent = false;

    public ParquetInfo(Path pvPath, ParquetReadOptions readOptions) throws IOException {
        super();
        inputFile = new LocalInputFile(pvPath);
        fileReader = ParquetFileReader.open(inputFile, readOptions);
        footer = fileReader.getFooter();
        var metadata = footer.getFileMetaData();
        this.dataYear = Short.parseShort(metadata.getKeyValueMetaData().get(MetaDataKey.YEAR.key));
        this.pvName = metadata.getKeyValueMetaData().get(MetaDataKey.PV_NAME.key);

        this.archDBRTypes = ArchDBRTypes.valueOf(metadata.getKeyValueMetaData().get(MetaDataKey.TYPE.key));

        logger.debug(() -> String.format(
                "read file meta name %s year %s type %s first event %s last event %s",
                pvName,
                dataYear,
                archDBRTypes,
                getFirstEvent().getEventTimeStamp().toString(),
                getLastEvent().getEventTimeStamp()));
    }

    public ParquetInfo(Path pvPath) throws IOException {
        this(pvPath, baseOptions);
    }

    /**
     * Filter to search for events with result.secondsintoyear = seconds
     * and if nanos is not null result.nanos < nanos
     *
     * @param seconds seconds of filter
     * @param nanos   nanos of filter
     * @return filter
     */
    static FilterCompat.Filter getSecondsFilter(Integer seconds, Integer nanos) {
        FilterPredicate predicate = eq(intColumn(ColumnName.SECONDS.key), seconds);
        if (nanos != null) {
            predicate = and(predicate, lt(intColumn(ColumnName.NANOSECONDS.key), nanos));
        }
        return FilterCompat.get(predicate);
    }

    /**
     * Filter to search for events with start <= result.secondsintoyear < end.secondsintoyear
     * or result.secondsintoyear = end.secondsintoyear and 0 <= result.nanos < end.nanos
     *
     * @param start start of period
     * @param end   end of period
     * @return filter
     */
    static FilterCompat.Filter getSecondsPeriodFilter(Integer start, YearSecondTimestamp end) {
        FilterPredicate predicate = and(
                // gtEq start
                or(
                        and(
                                eq(intColumn(ColumnName.SECONDS.key), start),
                                gtEq(intColumn(ColumnName.NANOSECONDS.key), 0)),
                        gtEq(intColumn(ColumnName.SECONDS.key), start + 1)),
                // lt end
                or(
                        lt(intColumn(ColumnName.SECONDS.key), end.getSecondsintoyear()),
                        and(
                                eq(intColumn(ColumnName.SECONDS.key), end.getSecondsintoyear()),
                                ltEq(intColumn(ColumnName.NANOSECONDS.key), end.getNano()))));
        return FilterCompat.get(predicate);
    }

    /**
     * Create the fileinfo for a given path
     *
     * @param path Path to file
     * @return ParquetInfo
     */
    public static ParquetInfo fetchFileInfo(Path path) {
        try {
            return new ParquetInfo(path);
        } catch (IOException e) {
            logger.error("Exception reading payload info from path: {}, with exception {}", path, e);
        }
        return null;
    }

    /**
     * Get the last event from a file filtered by a max seconds into year
     *
     * @param hadoopInputFile file
     * @param max             seconds into year to get event from
     * @param nanos           optional filter for before some nanos
     * @return last event before end and after start
     * @throws IOException if reading file fails
     */
    private static DBRTimeEvent getEventAtSeconds(
            InputFile hadoopInputFile, Integer max, Integer nanos, ArchDBRTypes archDBRTypes, short year)
            throws IOException {
        return getLastEventInFilter(hadoopInputFile, getSecondsFilter(max, nanos), archDBRTypes, year);
    }

    /**
     * Get the last event from a file filtered by the time period start to end
     *
     * @param hadoopInputFile file
     * @param start           seconds into year of start timestamp
     * @param end             timestamp of end of time period
     * @return last event before end and after start
     * @throws IOException if reading file fails
     */
    private static DBRTimeEvent getLastEventPeriod(
            InputFile hadoopInputFile, Integer start, YearSecondTimestamp end, ArchDBRTypes archDBRTypes, short year)
            throws IOException {
        return getLastEventInFilter(hadoopInputFile, getSecondsPeriodFilter(start, end), archDBRTypes, year);
    }

    /**
     * Get the last event from a file filtered by an input filter
     *
     * @param hadoopInputFile file
     * @param filter          filter for the file
     * @return last event before end and after start
     * @throws IOException if reading file fails
     */
    private static DBRTimeEvent getLastEventInFilter(
            InputFile hadoopInputFile, FilterCompat.Filter filter, ArchDBRTypes archDBRTypes, short year)
            throws IOException {
        try (var reader =
                ProtoParquetReader.builder(hadoopInputFile).withFilter(filter).build()) {
            var value = reader.read();
            // Need clone because reader does some caching that breaks this somehow
            var prevValue = value != null ? ((Message.Builder) value).clone() : null;

            while (value != null) {
                prevValue = ((Message.Builder) value).clone();
                value = reader.read();
            }
            return constructEvent(prevValue, archDBRTypes, year);
        }
    }

    /**
     * Construct a DBRTimeEvent from a protobuf message builder
     *
     * @param event Input protobuf message
     * @return Corresponding DBRTimeEvent
     */
    private static DBRTimeEvent constructEvent(Message.Builder event, ArchDBRTypes archDBRTypes, short year) {
        if (event != null) {
            Constructor<? extends DBRTimeEvent> unmarshallingConstructor =
                    DBR2PBTypeMapping.getPBClassFor(archDBRTypes).getUnmarshallingFromEpicsEventConstructor();

            try {
                return unmarshallingConstructor.newInstance(year, event);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                logger.warn("Failed to construct event: {} with exception {}", event, e);
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "ParquetInfo{" + "pvName='"
                + pvName + '\'' + ", dataYear="
                + dataYear + ", archDBRTypes="
                + archDBRTypes + ", firstEvent="
                + firstEvent + ", lastEvent="
                + lastEvent + '}';
    }

    public String getPVName() {
        return this.pvName;
    }

    public short getDataYear() {
        return this.dataYear;
    }

    @Override
    public ArchDBRTypes getType() {
        return this.archDBRTypes;
    }

    private void getFirstEvent(InputFile hadoopInputFile) throws IOException {
        try (var reader = ProtoParquetReader.builder(hadoopInputFile).build()) {

            var value = reader.read();

            if (value != null) {
                firstEvent = constructEvent((Message.Builder) value, this.archDBRTypes, this.dataYear);
            }
        }
        this.fetchedFirstEvent = true;
    }

    /**
     * Get the last event in the corresponding input file
     *
     * @param hadoopInputFile Input file
     * @param footer          Previously read footer of the file
     * @throws IOException If problem reading the file
     */
    private void getLastEvent(InputFile hadoopInputFile, ParquetMetadata footer) throws IOException {
        Integer maxIntoYearSeconds = 0;
        for (BlockMetaData blockMetaData : footer.getBlocks()) {
            var secondsColumn = blockMetaData.getColumns().stream()
                    .filter(c -> c.getPath().toDotString().equals(ColumnName.SECONDS.key))
                    .findFirst();
            if (secondsColumn.isPresent()
                    && secondsColumn.get().getStatistics().compareMaxToValue(maxIntoYearSeconds) > 0) {
                maxIntoYearSeconds =
                        (Integer) secondsColumn.get().getStatistics().genericGetMax();
            }
        }
        this.lastEvent = getEventAtSeconds(hadoopInputFile, maxIntoYearSeconds, null, this.archDBRTypes, this.dataYear);
        this.fetchedLastEvent = true;
    }

    /**
     * Get the last event before the input timestamp of the input file.
     * Uses the footer to get the statistics on the file for filtering
     * out before searching for the event.
     *
     * @param before          timestamp before the event
     * @param hadoopInputFile file to search for event
     * @param footer          footer of file with statistics on file
     * @return last event before input timestamp
     */
    private DBRTimeEvent getLastEventBeforeTimestamp(
            InputFile hadoopInputFile, ParquetMetadata footer, YearSecondTimestamp before) throws IOException {
        Integer minOfSeconds = null;
        Integer maxBeforeSeconds = 0;
        for (BlockMetaData blockMetaData : footer.getBlocks()) {
            var secondsColumn = blockMetaData.getColumns().stream()
                    .filter(c -> c.getPath().toDotString().equals(ColumnName.SECONDS.key))
                    .findFirst();
            if (secondsColumn.isPresent()) {
                Statistics<Integer> statistics = secondsColumn.get().getStatistics();
                if (statistics.genericGetMax() > maxBeforeSeconds
                        && statistics.genericGetMax() < before.getSecondsintoyear()) {
                    maxBeforeSeconds = statistics.genericGetMax();
                } else if (statistics.genericGetMax() >= before.getSecondsintoyear()) {
                    minOfSeconds = statistics.genericGetMin();
                    if (minOfSeconds != before.getSecondsintoyear()) {
                        return getLastEventPeriod(
                                hadoopInputFile, minOfSeconds, before, this.archDBRTypes, this.dataYear);
                    }
                    return getEventAtSeconds(
                            hadoopInputFile,
                            maxBeforeSeconds,
                            getMaxBeforeNanos(before, maxBeforeSeconds),
                            this.archDBRTypes,
                            this.dataYear);
                }
            }
        }
        return getEventAtSeconds(
                hadoopInputFile,
                maxBeforeSeconds,
                getMaxBeforeNanos(before, maxBeforeSeconds),
                this.archDBRTypes,
                this.dataYear);
    }

    private static Integer getMaxBeforeNanos(YearSecondTimestamp before, Integer maxBeforeSeconds) {
        return maxBeforeSeconds == before.getSecondsintoyear() ? before.getNano() : null;
    }

    @Override
    public DBRTimeEvent getFirstEvent() {
        if (!fetchedFirstEvent) {
            try {
                getFirstEvent(inputFile);
            } catch (IOException e) {
                logger.error("Failed to get first event for file {}", inputFile, e);
            }
        }
        return this.firstEvent;
    }

    @Override
    public DBRTimeEvent getLastEvent() {
        if (!fetchedLastEvent) {
            try {
                getLastEvent(inputFile, footer);
            } catch (IOException e) {
                logger.error("Failed to get last event for file {}", inputFile, e);
            }
        }
        return this.lastEvent;
    }

    /**
     * Get the last event before the input timestamp
     *
     * @param before timestamp before the event
     * @return last event before input timestamp
     */
    DBRTimeEvent getLastEventBefore(YearSecondTimestamp before) {
        try {
            return this.getLastEventBeforeTimestamp(this.inputFile, this.footer, before);
        } catch (IOException e) {
            logger.error("Failed to get last event for file {}", inputFile, e);
        }
        return this.firstEvent;
    }

    /**
     * Gets the compression codec used for the file.
     *
     * @return The {@link CompressionCodecName} used.
     */
    public CompressionCodecName getCompressionCodecName() {
        return this.footer.getBlocks().getFirst().getColumns().getFirst().getCodec();
    }
}
