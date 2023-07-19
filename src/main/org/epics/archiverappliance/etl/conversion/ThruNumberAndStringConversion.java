package org.epics.archiverappliance.etl.conversion;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.etl.ConversionException;
import org.epics.archiverappliance.etl.ConversionFunction;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.channelarchiver.HashMapEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.time.Instant;
import java.util.Iterator;

/**
 * Generic class for some standard type conversions.
 * Not all type conversions are supported; some type conversions may be completely (or even worse, incompletely) inaccurate for your use case.
 * Only a few of these have been tested and even those only incompletely.
 * In most cases, you should roll your own conversion function and then apply using the ETLDest interface.
 *
 * @author mshankar
 *
 */
public class ThruNumberAndStringConversion implements ConversionFunction {
    private final ArchDBRTypes destDBRType;
    private final Constructor<? extends DBRTimeEvent> serializingConstructor;

    public ThruNumberAndStringConversion(ArchDBRTypes destDBRType) {
        this.destDBRType = destDBRType;
        this.serializingConstructor =
                DBR2PBTypeMapping.getPBClassFor(destDBRType).getSerializingConstructor();
    }

    @Override
    public EventStream convertStream(
            final EventStream srcEventStream, Instant streamStartTime, Instant streamEndTime) throws IOException {
        return new ThruNumStrConversionWrapper(srcEventStream);
    }

    @Override
    public boolean shouldConvert(EventStream srcEventStream, Instant streamStartTime, Instant streamEndTime) {
        // Always convert in the case of type conversion.
        return true;
    }

    private final class ThruNumStrConversionWrapper implements EventStream {
        private final EventStream srcEventStream;
        Iterator<Event> theIterator;

        private ThruNumStrConversionWrapper(EventStream srcEventStream) {
            this.srcEventStream = srcEventStream;
            theIterator = srcEventStream.iterator();
        }

        @Override
        public void close() throws IOException {
            theIterator = null;
            srcEventStream.close();
        }

        @Override
        public Iterator<Event> iterator() {
            return new Iterator<Event>() {

                @Override
                public boolean hasNext() {
                    return theIterator.hasNext();
                }

                @Override
                public Event next() {
                    try {
                        DBRTimeEvent event = (DBRTimeEvent) theIterator.next();
                        HashMapEvent hashEvent = new HashMapEvent(destDBRType, event);
                        hashEvent.setValue(HashMapEvent.VALUE_FIELD_NAME, convert2DestType(event.getSampleValue()));
                        return serializingConstructor.newInstance(hashEvent);
                    } catch (Exception ex) {
                        throw new ConversionException(
                                "Exception during conversion of pv "
                                        + srcEventStream.getDescription().getPvName(),
                                ex);
                    }
                }

                private String convert2DestType(SampleValue sampleValue) {
                    return switch (destDBRType) {
                        case DBR_SCALAR_BYTE -> Byte.toString(
                                sampleValue.getValue().byteValue());
                        case DBR_SCALAR_DOUBLE -> Double.toString(
                                sampleValue.getValue().doubleValue());
                        case DBR_SCALAR_ENUM, DBR_SCALAR_SHORT -> Short.toString(
                                sampleValue.getValue().shortValue());
                        case DBR_SCALAR_FLOAT -> Float.toString(
                                sampleValue.getValue().floatValue());
                        case DBR_SCALAR_INT -> Integer.toString(
                                sampleValue.getValue().intValue());
                        case DBR_SCALAR_STRING -> sampleValue.getStringValue(0);
                        default -> throw new UnsupportedOperationException();
                    };
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public EventStreamDesc getDescription() {
            return new RemotableEventStreamDesc(
                    destDBRType,
                    srcEventStream.getDescription().getPvName(),
                    ((RemotableEventStreamDesc) srcEventStream.getDescription()).getYear());
        }
    }
}
