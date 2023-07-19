package org.epics.archiverappliance.etl;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.etl.conversion.ThruNumberAndStringConversion;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationValueGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;

/**
 * Test various type conversion functions. 
 * Some type conversions are trivial; others are more complex.
 * We generate some sample events of a particular type; then convert to other dest types and convert back. 
 * @author mshankar
 *
 */
public class ThruNumberAndStringConversionFunctionTest {
	private static Logger logger = LogManager.getLogger(ThruNumberAndStringConversionFunctionTest.class.getName());


	@Test
	public void testThruNumberConversion() throws Exception {
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_INT, ArchDBRTypes.DBR_SCALAR_DOUBLE);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_ENUM, ArchDBRTypes.DBR_SCALAR_DOUBLE);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_FLOAT, ArchDBRTypes.DBR_SCALAR_DOUBLE);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_ENUM, ArchDBRTypes.DBR_SCALAR_INT);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_INT, ArchDBRTypes.DBR_SCALAR_ENUM);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_DOUBLE, ArchDBRTypes.DBR_SCALAR_ENUM);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_DOUBLE, ArchDBRTypes.DBR_SCALAR_INT);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_DOUBLE, ArchDBRTypes.DBR_SCALAR_FLOAT);
	}
	
	private void testThruNumberConversionForDBRType(ArchDBRTypes srcDBRType, ArchDBRTypes destDBRType) throws Exception {
		logger.info("Testing conversion from " + srcDBRType.toString() + " to " + destDBRType.toString());
		EventStream srcStream = generateDataForArchDBRType(srcDBRType);
		EventStream destStream = convertToType(srcStream, destDBRType);
		compareStreams(srcStream, destStream);
	}

	private EventStream generateDataForArchDBRType(ArchDBRTypes dbrType) throws Exception { 
		int numEvents = 500;
		ArrayListEventStream ret = new ArrayListEventStream(numEvents, new RemotableEventStreamDesc(dbrType, "test", TimeUtils.getCurrentYear()));
		int eventsAdded = 0;
		Constructor<? extends DBRTimeEvent> serializingConstructor = DBR2PBTypeMapping.getPBClassFor(dbrType).getSerializingConstructor();
		try(SimulationEventStream simstream = new SimulationEventStream(dbrType, new ValueGenerator(dbrType, numEvents))) {
			for(Event simEvent : simstream) {
				DBRTimeEvent genEvent = (DBRTimeEvent) serializingConstructor.newInstance(simEvent);
				if(eventsAdded % 1000 == 0) { 
					genEvent.addFieldValue("HIHI", "Test");
					genEvent.addFieldValue("LOLO", "13:40:12");
					if(eventsAdded % 2000 == 0) {
						genEvent.markAsActualChange();
					}
				}

				ret.add(genEvent);
				if(eventsAdded++ > numEvents) break;
			}
			return ret;
		}
	}
	
	private EventStream convertToType(EventStream srcStream, ArchDBRTypes destDBRType) throws IOException { 
		return new ThruNumberAndStringConversion(destDBRType).convertStream(srcStream, null, null);
	}
	
	private void compareStreams(EventStream srcStream, EventStream destStream) throws IOException {
		Iterator<Event> srcIt = srcStream.iterator();
		Iterator<Event> destIt = destStream.iterator();
		int eventCount = 0;
		while(srcIt.hasNext() || destIt.hasNext()) { 
			DBRTimeEvent srcEvent = (DBRTimeEvent) srcIt.next();
			DBRTimeEvent destEvent = (DBRTimeEvent) destIt.next();
			assertTrue("Compare timestamps failed at event " + eventCount, srcEvent.getEventTimeStamp().equals(destEvent.getEventTimeStamp()));
			assertTrue("Compare status failed at event " + eventCount, srcEvent.getStatus() == destEvent.getStatus());
			assertTrue("Compare severity failed at event " + eventCount, srcEvent.getSeverity() == destEvent.getSeverity());
			assertTrue("Compare value failed at event " + eventCount 
					+ " " + srcEvent.getSampleValue().getValue().doubleValue()
					+ " " + destEvent.getSampleValue().getValue().doubleValue()
					+ " " + Math.abs(srcEvent.getSampleValue().getValue().doubleValue() - destEvent.getSampleValue().getValue().doubleValue()), 
					Math.abs(srcEvent.getSampleValue().getValue().doubleValue() - destEvent.getSampleValue().getValue().doubleValue()) < 0.0005);
			assertTrue("Compare fields failed at event " + eventCount, compareMaps(srcEvent.getFields(), destEvent.getFields()));
			assertTrue("Compare fields changed failed at event " + eventCount, srcEvent.isActualChange() == destEvent.isActualChange());
			eventCount++;
		}
	}
	
	private static boolean compareMaps(HashMap<String, String> map1, HashMap<String, String> map2) { 
		Set<String> removedKeys = new HashSet<String>(map1.keySet());
		removedKeys.removeAll(map2.keySet());

		Set<String> addedKeys = new HashSet<String>(map2.keySet());
		addedKeys.removeAll(map1.keySet());

		Set<Entry<String, String>> changedEntries = new HashSet<Entry<String, String>>(map2.entrySet());
		changedEntries.removeAll(map1.entrySet());
		
		return removedKeys.isEmpty() && addedKeys.isEmpty() && changedEntries.isEmpty();
	}
	
	
	private class ValueGenerator implements SimulationValueGenerator {
		private int numSamples;
		private ArchDBRTypes dbrType;
		public ValueGenerator(ArchDBRTypes dbrType, int numSamples) { 
			this.numSamples = numSamples;
			this.dbrType = dbrType;
		}

		@Override
		public int getNumberOfSamples(ArchDBRTypes type) {
			return numSamples;
		}

		@Override
		public SampleValue getSampleValue(ArchDBRTypes type, int secondsIntoYear) {
			switch(dbrType) {
			case DBR_SCALAR_BYTE:
				return new ScalarValue<Byte>((byte) secondsIntoYear);
			case DBR_SCALAR_DOUBLE:
				return new ScalarValue<Double>((double) secondsIntoYear);
			case DBR_SCALAR_ENUM:
				return new ScalarValue<Short>((short) secondsIntoYear);
			case DBR_SCALAR_FLOAT:
				return new ScalarValue<Float>((float) secondsIntoYear);
			case DBR_SCALAR_INT:
				return new ScalarValue<Integer>((int) secondsIntoYear);
			case DBR_SCALAR_SHORT:
				return new ScalarValue<Short>((short) secondsIntoYear);
			case DBR_SCALAR_STRING:
				return new ScalarStringSampleValue(Integer.toString(secondsIntoYear));
			case DBR_V4_GENERIC_BYTES:
				return new ScalarStringSampleValue(Integer.toString(secondsIntoYear));
			case DBR_WAVEFORM_BYTE:
				return new VectorValue<Byte>(Collections.nCopies(10*secondsIntoYear, ((byte)(secondsIntoYear%255))));
			case DBR_WAVEFORM_DOUBLE:
				return new VectorValue<Double>(Collections.nCopies(10*secondsIntoYear, ((double)secondsIntoYear)));
			case DBR_WAVEFORM_ENUM:
				return new VectorValue<Short>(Collections.nCopies(10*secondsIntoYear, ((short)secondsIntoYear)));
			case DBR_WAVEFORM_FLOAT:
				return new VectorValue<Float>(Collections.nCopies(10*secondsIntoYear, ((float)secondsIntoYear)));
			case DBR_WAVEFORM_INT:
				return new VectorValue<Integer>(Collections.nCopies(10*secondsIntoYear, ((int)secondsIntoYear)));
			case DBR_WAVEFORM_SHORT:
				return new VectorValue<Short>(Collections.nCopies(10*secondsIntoYear, ((short)secondsIntoYear)));
			case DBR_WAVEFORM_STRING:
				return new VectorStringSampleValue(Collections.nCopies(10*secondsIntoYear, Integer.toString(secondsIntoYear)));
			default:
				throw new UnsupportedOperationException(); 
			}
		} 
		
	}
}
