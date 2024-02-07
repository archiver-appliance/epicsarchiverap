package edu.stanford.slac.archiverappliance.plain.parquet;

import com.google.protobuf.MessageOrBuilder;
import edu.stanford.slac.archiverappliance.plain.EventStreamIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.ParquetReader;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An implementation of {@link EventStreamIterator} that reads from a Parquet reader.
 */
public class ParquetBackedPBEventIterator implements EventStreamIterator {
    private static final Logger logger = LogManager.getLogger(ParquetBackedPBEventIterator.class.getName());
    private final Constructor<? extends DBRTimeEvent> unmarshallingConstructor;
    private final short year;
    /**
     * Collection of readers of parquet files, built when needed.
     */
    List<ParquetReader.Builder<Object>> readerBuilders;
    /**
     * The current reader.
     */
    private ParquetReader<Object> cachedReader;
    /**
     * The current event.
     */
    private Event cachedEvent;
    /**
     * The current reader index.
     */
    private int cachedReaderIndex;
    /**
     * A flag indicating if the current reader has been fully read.
     */
    private boolean cachedReaderFinished = false;
    /**
     * A flag indicating if the iterator has been fully read.
     */
    private boolean finished = false;

    ParquetBackedPBEventIterator(
            List<ParquetReader.Builder<Object>> readerBuilders,
            Constructor<? extends DBRTimeEvent> unmarshallingConstructor,
            short year) {
        this.readerBuilders = readerBuilders;
        this.unmarshallingConstructor = unmarshallingConstructor;
        this.year = year;
        try {
            this.cachedReader = readerBuilders.get(cachedReaderIndex).build();
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe.toString());
        }
    }

    /**
     * Caches the next event, and returns if it exists.
     *
     * @return true if there is a next event, false otherwise.
     */
    @Override
    public boolean hasNext() {
        if (cachedEvent != null) {
            return true;
        } else if (finished) {
            return false;
        } else if (cachedReaderFinished) {
            if (cachedReaderIndex + 1 < readerBuilders.size()) {
                cachedReaderIndex++;
                try {
                    cachedReader = readerBuilders.get(cachedReaderIndex).build();
                } catch (IOException ioe) {
                    close();
                    throw new IllegalStateException(ioe.toString());
                }
                cachedReaderFinished = false;
                return readerHasNext(cachedReader);
            } else {
                finished = true;
                return false;
            }
        } else {
            return readerHasNext(cachedReader);
        }
    }

    private boolean readerHasNext(ParquetReader<Object> reader) {
        try {

            MessageOrBuilder readEvent = (MessageOrBuilder) reader.read();
            if (readEvent == null) {
                cachedReaderFinished = true;
                return false;
            } else {
                cachedEvent = unmarshallingConstructor.newInstance(year, readEvent);
                return true;
            }

        } catch (Exception ioe) {
            close();
            throw new IllegalStateException(ioe.toString());
        }
    }

    /**
     * Closes the underlying <code>Reader</code> quietly.
     * This method is useful if you only want to process the first few
     * lines of a larger file. If you do not close the iterator
     * then the <code>Reader</code> remains open.
     * This method can safely be called multiple times.
     */
    public void close() {
        try {
            cachedReader.close();
        } catch (IOException ignored) {
        }
        finished = true;
        cachedEvent = null;
    }

    /**
     * Returns the next event in the wrapped <code>Reader</code>.
     *
     * @return the next event.
     */
    @Override
    public Event next() {
        return nextEvent();
    }

    /**
     * Returns the next line in the wrapped <code>Reader</code>.
     *
     * @return the next line from the input
     * @throws NoSuchElementException if there is no line to return
     */
    public Event nextEvent() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more lines");
        }
        Event currentEvent = cachedEvent;
        cachedEvent = null;
        return currentEvent;
    }
}
