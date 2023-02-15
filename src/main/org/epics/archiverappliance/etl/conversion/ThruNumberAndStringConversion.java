package org.epics.archiverappliance.etl.conversion;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.Timestamp;
import java.util.Iterator;

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

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;

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
						DBRTimeEvent retEvent = (DBRTimeEvent) serializingConstructor.newInstance(hashEvent);
						return retEvent;
					} catch(Exception ex) { 
						throw new ConversionException("Exception during conversion of pv " + srcEventStream.getDescription().getPvName(), ex);
					}
				}

				private String convert2DestType(SampleValue sampleValue) {
					switch(destDBRType) {
					case DBR_SCALAR_BYTE:
						return Byte.toString(sampleValue.getValue().byteValue());
					case DBR_SCALAR_DOUBLE:
						return Double.toString(sampleValue.getValue().doubleValue());
					case DBR_SCALAR_ENUM:
						return Short.toString(sampleValue.getValue().shortValue());
					case DBR_SCALAR_FLOAT:
						return Float.toString(sampleValue.getValue().floatValue());
					case DBR_SCALAR_INT:
						return Integer.toString(sampleValue.getValue().intValue());
					case DBR_SCALAR_SHORT:
						return Short.toString(sampleValue.getValue().shortValue());
					case DBR_SCALAR_STRING:
						return sampleValue.getStringValue(0);
					case DBR_V4_GENERIC_BYTES:
						throw new UnsupportedOperationException(); 
					case DBR_WAVEFORM_BYTE:
						throw new UnsupportedOperationException(); 
					case DBR_WAVEFORM_DOUBLE:
						throw new UnsupportedOperationException(); 
					case DBR_WAVEFORM_ENUM:
						throw new UnsupportedOperationException(); 
					case DBR_WAVEFORM_FLOAT:
						throw new UnsupportedOperationException(); 
					case DBR_WAVEFORM_INT:
						throw new UnsupportedOperationException(); 
					case DBR_WAVEFORM_SHORT:
						throw new UnsupportedOperationException(); 
					case DBR_WAVEFORM_STRING:
						throw new UnsupportedOperationException(); 
					default:
						throw new UnsupportedOperationException(); 
					}
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}

		@Override
		public EventStreamDesc getDescription() {
			return new RemotableEventStreamDesc(destDBRType,
					srcEventStream.getDescription().getPvName(),
					((RemotableEventStreamDesc)srcEventStream.getDescription()).getYear());
		}
	}

	private ArchDBRTypes destDBRType;
	private Constructor<? extends DBRTimeEvent> serializingConstructor;

	public ThruNumberAndStringConversion(ArchDBRTypes destDBRType) { 
		this.destDBRType = destDBRType;
		this.serializingConstructor = DBR2PBTypeMapping.getPBClassFor(destDBRType).getSerializingConstructor();
	}

	@Override
	public EventStream convertStream(final EventStream srcEventStream, Timestamp streamStartTime, Timestamp streamEndTime) throws IOException {
		return new ThruNumStrConversionWrapper(srcEventStream); 
	}

	@Override
	public boolean shouldConvert(EventStream srcEventStream, Timestamp streamStartTime, Timestamp streamEndTime) throws IOException {
		// Always convert in the case of type conversion.
		return true;
	}
}
