package org.epics.archiverappliance.etl;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
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
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.AppendDataStateData;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

/**
 * Test the conversion implementation in the PlainPBStoragePlugin.
 * We generate a standard data set into a PB file, convert and make sure the data is as expected (timestamps remain the same, values are converted appropriately).
 *  
 * @author mshankar
 *
 */
public class PlainPBConversionTest {
	private static Logger logger = Logger.getLogger(PlainPBConversionTest.class.getName());
	PlainPBStoragePlugin storagePlugin;
	PBCommonSetup setup;

	@Test
	public void testPlainPBConversion() throws Exception {
		testConversionForGranularity(PartitionGranularity.PARTITION_HOUR, 24*60*60);
		testConversionForGranularity(PartitionGranularity.PARTITION_DAY, 7*24*60*60);
		testConversionForGranularity(PartitionGranularity.PARTITION_MONTH, 2*31*24*60*60);
	}
	
	private void testConversionForGranularity(PartitionGranularity granularity, int numEvents) throws Exception {
		storagePlugin = new PlainPBStoragePlugin();
		setup = new PBCommonSetup();
		setup.setUpRootFolder(storagePlugin, "PlainPBConversionTest", granularity);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_INT, ArchDBRTypes.DBR_SCALAR_DOUBLE, numEvents);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_ENUM, ArchDBRTypes.DBR_SCALAR_DOUBLE, numEvents);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_FLOAT, ArchDBRTypes.DBR_SCALAR_DOUBLE, numEvents);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_ENUM, ArchDBRTypes.DBR_SCALAR_INT, numEvents);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_INT, ArchDBRTypes.DBR_SCALAR_ENUM, numEvents);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_DOUBLE, ArchDBRTypes.DBR_SCALAR_ENUM, numEvents);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_DOUBLE, ArchDBRTypes.DBR_SCALAR_INT, numEvents);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_DOUBLE, ArchDBRTypes.DBR_SCALAR_FLOAT, numEvents);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_SHORT, ArchDBRTypes.DBR_SCALAR_INT, numEvents);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_SHORT, ArchDBRTypes.DBR_SCALAR_FLOAT, numEvents);
		testThruNumberConversionForDBRType(ArchDBRTypes.DBR_SCALAR_SHORT, ArchDBRTypes.DBR_SCALAR_DOUBLE, numEvents);
		testFailedConversionForDBRType(ArchDBRTypes.DBR_SCALAR_DOUBLE, ArchDBRTypes.DBR_WAVEFORM_STRING, numEvents);
		setup.deleteTestFolder();
		
	}

	private void testThruNumberConversionForDBRType(ArchDBRTypes srcDBRType, ArchDBRTypes destDBRType, int numEvents) throws Exception {
		logger.info("Testing conversion from " + srcDBRType.toString() + " to " + destDBRType.toString());
		String pvName = "PlainPBConversionTest_" + srcDBRType.toString() + "_" + destDBRType.toString();
		generateDataForArchDBRType(pvName, srcDBRType, numEvents);
		validateStream(pvName, numEvents, srcDBRType);
		Set<String> bflist = setup.listTestFolderContents();		
		convertToType(pvName, destDBRType);
		validateStream(pvName, numEvents, destDBRType);
		Set<String> aflist = setup.listTestFolderContents();
		assertTrue("The contents of the test folder have changed; probably something remained", bflist.equals(aflist));
	}
	
	private void testFailedConversionForDBRType(ArchDBRTypes srcDBRType, ArchDBRTypes destDBRType, int numEvents) throws Exception {
		logger.info("Testing failed conversion from " + srcDBRType.toString() + " to " + destDBRType.toString() + ". You could see an exception here; ignore it. It is expected");
		String pvName = "PlainPBConversionTest_" + srcDBRType.toString() + "_" + destDBRType.toString();
		generateDataForArchDBRType(pvName, srcDBRType, numEvents);
		
		validateStream(pvName, numEvents, srcDBRType);
		try {
			Logger appendDataLogger = Logger.getLogger(AppendDataStateData.class.getName());
			Level currLevel = appendDataLogger.getLevel();
			appendDataLogger.setLevel(Level.FATAL);
			convertToType(pvName, destDBRType);
			appendDataLogger.setLevel(currLevel);
		} catch(Exception ex) { 
			assertTrue("Expecting a Conversion Exception, instead got a " + ex, ex.getCause() instanceof ConversionException);
		}
		validateStream(pvName, numEvents, srcDBRType);
	}
	
	private void generateDataForArchDBRType(String pvName, ArchDBRTypes dbrType, int numEvents) throws Exception { 
		ArrayListEventStream ret = new ArrayListEventStream(numEvents, new RemotableEventStreamDesc(dbrType, pvName, TimeUtils.getCurrentYear()));
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
			try(BasicContext context = new BasicContext()) { 
				storagePlugin.appendData(context, pvName, ret);
			}
		}
	}
	
	private void convertToType(String pvName, ArchDBRTypes destDBRType) throws IOException { 
		try(BasicContext context = new BasicContext()) { 
			storagePlugin.convert(context, pvName, new ThruNumberAndStringConversion(destDBRType));
		}
	}
	
	private void validateStream(String pvName, int numEvents, ArchDBRTypes destDBRType) throws Exception {
		long expectedCurrentEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		int eventCount = 0;
		try(BasicContext context = new BasicContext()) { 
			Timestamp startTime = TimeUtils.minusDays(TimeUtils.now(), 2*266);
			Timestamp endTime = TimeUtils.plusDays(TimeUtils.now(), 2*266);
			List<Callable<EventStream>> callables = storagePlugin.getDataForPV(context, pvName, startTime, endTime);
			for(Callable<EventStream> callable : callables) { 
				try(EventStream strm = callable.call()) { 
					assertTrue("Expecting pvName to be " + pvName + " instead it is " + strm.getDescription().getPvName(), pvName.equals(strm.getDescription().getPvName()));
					assertTrue("Expecting DBR type to be " + destDBRType.toString() + " instead it is " + strm.getDescription().getArchDBRType(), strm.getDescription().getArchDBRType() == destDBRType);
					for(Event e : strm) { 
						DBRTimeEvent dbr = (DBRTimeEvent) e;
						long epochSeconds = dbr.getEpochSeconds();
						assertTrue("Timestamp is different at event count " + eventCount + " Expected " + TimeUtils.convertToHumanReadableString(expectedCurrentEpochSeconds) + " got " + TimeUtils.convertToHumanReadableString(epochSeconds), epochSeconds == expectedCurrentEpochSeconds);
						if(eventCount % 1000 == 0) { 
							assertTrue("Expecting field values at event count " + eventCount, dbr.hasFieldValues());
							assertTrue("Expecting HIHI as Test at " + eventCount, dbr.getFieldValue("HIHI").equals("Test"));
							assertTrue("Expecting LOLO as 13:40:12 at " + eventCount, dbr.getFieldValue("LOLO").equals("13:40:12"));
							if(eventCount % 2000 == 0) {
								assertTrue("Expecting field values to be actual change " + eventCount, dbr.isActualChange());
							}
						}
						expectedCurrentEpochSeconds++;
						eventCount++;
					}
				}
			}
		}
		assertTrue("Expecting some events " + eventCount, eventCount == numEvents);
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
