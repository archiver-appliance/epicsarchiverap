package edu.stanford.slac.archiverappliance.plain.parquet;

import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.proto.ProtoParquetReader;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

public class ParquetInfo extends FileInfo {
    static final String PV_NAME = "pvName";
    static final String YEAR = "year";
    static final String TYPE = "ArchDBRType";
    static final String SECONDS_COLUMN_NAME = "secondsintoyear";
    static final String NANOSECONDS_COLUMN_NAME = "nano";
    private static final Logger logger = LogManager.getLogger(ParquetInfo.class);
    final InputFile hadoopInputFile;
    String pvName;
    short dataYear;
    ArchDBRTypes archDBRTypes;
    ParquetMetadata footer;
    ParquetFileReader fileReader;
    boolean fetchedLastEvent = false;
    boolean fetchedFirstEvent = false;

    public ParquetInfo(Path pvPath, Configuration configuration) throws IOException {
        super();
        var hadoopPath = new org.apache.hadoop.fs.Path(pvPath.toUri());
        hadoopInputFile = HadoopInputFile.fromPath(hadoopPath, configuration);
        fileReader = ParquetFileReader.open(hadoopInputFile);
        footer = fileReader.getFooter();
        var metadata = footer.getFileMetaData();
        this.dataYear = Short.parseShort(metadata.getKeyValueMetaData().get(YEAR));
        this.pvName = metadata.getKeyValueMetaData().get(PV_NAME);

        this.archDBRTypes = ArchDBRTypes.valueOf(metadata.getKeyValueMetaData().get(TYPE));

        logger.debug(() -> String.format(
                "read file meta name %s year %s type %s first event %s last event %s",
                pvName,
                dataYear,
                archDBRTypes,
                getFirstEvent().getEventTimeStamp().toString(),
                getLastEvent().getEventTimeStamp()));
    }

    public ParquetInfo(Path pvPath) throws IOException {
        this(pvPath, new Configuration());
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
        FilterPredicate predicate = eq(intColumn(SECONDS_COLUMN_NAME), seconds);
        if (nanos != null) {
            predicate = and(predicate, lt(intColumn(NANOSECONDS_COLUMN_NAME), nanos));
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
                                eq(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), start),
                                gtEq(intColumn(ParquetInfo.NANOSECONDS_COLUMN_NAME), 0)),
                        gtEq(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), start + 1)),
                // lt end
                or(
                        lt(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), end.getSecondsintoyear()),
                        and(
                                eq(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), end.getSecondsintoyear()),
                                ltEq(intColumn(ParquetInfo.NANOSECONDS_COLUMN_NAME), end.getNano()))));
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
                    .filter(c -> c.getPath().toDotString().equals(SECONDS_COLUMN_NAME))
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
                    .filter(c -> c.getPath().toDotString().equals(SECONDS_COLUMN_NAME))
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
                            hadoopInputFile, maxBeforeSeconds, before.getNano(), this.archDBRTypes, this.dataYear);
                }
            }
        }
        return null;
    }

    @Override
    public DBRTimeEvent getFirstEvent() {
        if (!fetchedFirstEvent) {
            try {
                getFirstEvent(hadoopInputFile);
            } catch (IOException e) {
                logger.error("Failed to get first event for file {}", hadoopInputFile, e);
            }
        }
        return this.firstEvent;
    }

    @Override
    public DBRTimeEvent getLastEvent() {
        if (!fetchedLastEvent) {
            try {
                getLastEvent(hadoopInputFile, footer);
            } catch (IOException e) {
                logger.error("Failed to get last event for file {}", hadoopInputFile, e);
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
            return this.getLastEventBeforeTimestamp(this.hadoopInputFile, this.footer, before);
        } catch (IOException e) {
            logger.error("Failed to get last event for file {}", hadoopInputFile, e);
        }
        return this.firstEvent;
    }

    public CompressionCodecName getCompressionCodecName() {
        return this.footer.getBlocks().get(0).getColumns().get(0).getCodec();
    }
}
