package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * Test the conversion implementation in the PlainPBStoragePlugin.
 * We generate a standard data set into a PB file, convert and make sure the data is as expected (timestamps remain the same, values are converted appropriately).
 *
 * @author mshankar
 *
 */
public class PlainPBConversionTest {
    private static final Logger logger = LogManager.getLogger(PlainPBConversionTest.class.getName());

    public static Stream<Arguments> providePlainPBConversion() {
        return Stream.of(
                        provideConversionForGranularity(PartitionGranularity.PARTITION_HOUR, 24 * 60 * 60),
                        provideConversionForGranularity(PartitionGranularity.PARTITION_DAY, 7 * 24 * 60 * 60),
                        provideConversionForGranularity(PartitionGranularity.PARTITION_MONTH, 2 * 31 * 24 * 60 * 60))
                .flatMap(a -> a);
    }

    private static Stream<Arguments> provideConversionForGranularity(PartitionGranularity granularity, int numEvents) {
        return Stream.of(
                        Arguments.of(
                                granularity, ArchDBRTypes.DBR_SCALAR_INT, ArchDBRTypes.DBR_SCALAR_DOUBLE, numEvents),
                        Arguments.of(
                                granularity,
                                ArchDBRTypes.DBR_SCALAR_ENUM,
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                numEvents),
                        Arguments.of(
                                granularity,
                                ArchDBRTypes.DBR_SCALAR_FLOAT,
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                numEvents),
                        Arguments.of(
                                granularity, ArchDBRTypes.DBR_SCALAR_ENUM, ArchDBRTypes.DBR_SCALAR_INT, numEvents),
                        Arguments.of(
                                granularity, ArchDBRTypes.DBR_SCALAR_INT, ArchDBRTypes.DBR_SCALAR_ENUM, numEvents),
                        Arguments.of(
                                granularity,
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                ArchDBRTypes.DBR_SCALAR_ENUM,
                                numEvents),
                        Arguments.of(
                                granularity, ArchDBRTypes.DBR_SCALAR_DOUBLE, ArchDBRTypes.DBR_SCALAR_INT, numEvents),
                        Arguments.of(
                                granularity,
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                ArchDBRTypes.DBR_SCALAR_FLOAT,
                                numEvents),
                        Arguments.of(
                                granularity, ArchDBRTypes.DBR_SCALAR_SHORT, ArchDBRTypes.DBR_SCALAR_INT, numEvents),
                        Arguments.of(
                                granularity,
                                ArchDBRTypes.DBR_SCALAR_SHORT,
                                ArchDBRTypes.DBR_SCALAR_FLOAT,
                                numEvents),
                        Arguments.of(
                                granularity,
                                ArchDBRTypes.DBR_SCALAR_SHORT,
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                numEvents));
    }

    @ParameterizedTest
    @MethodSource("providePlainPBConversion")
    @Tag("slow")
    public void testThruNumberConversionForDBRType(
            PartitionGranularity granularity,
            ArchDBRTypes srcDBRType,
            ArchDBRTypes destDBRType,
            int numEvents)
            throws Exception {
        PlainPBStoragePlugin storagePlugin = new PlainPBStoragePlugin();
        PBCommonSetup setup = new PBCommonSetup();
        setup.setUpRootFolder(storagePlugin, "PlainPBConversionTest", granularity);
        logger.info("Testing conversion from " + srcDBRType.toString() + " to " + destDBRType.toString());
        String pvName = "PlainPBConversionTest_" + srcDBRType + "_" + destDBRType;
        generateDataForArchDBRType(pvName, srcDBRType, numEvents,storagePlugin);
        validateStream(pvName, numEvents, srcDBRType, storagePlugin);
        Set<String> bflist = setup.listTestFolderContents();
        convertToType(pvName, destDBRType, storagePlugin);
        validateStream(pvName, numEvents, destDBRType, storagePlugin);
        Set<String> aflist = setup.listTestFolderContents();
        Assertions.assertEquals(
                bflist, aflist, "The contents of the test folder have changed; probably something remained");
    }

    public static Stream<Arguments> provideFailedConversionForDBRType() {
        return Stream.of(
                        Arguments.of(PartitionGranularity.PARTITION_HOUR, 24 * 60 * 60),
                        Arguments.of(PartitionGranularity.PARTITION_DAY, 7 * 24 * 60 * 60),
                        Arguments.of(PartitionGranularity.PARTITION_MONTH, 2 * 31 * 24 * 60 * 60));
    }

    @ParameterizedTest
    @MethodSource("provideFailedConversionForDBRType")
    public void testFailedConversionForDBRType(
            PartitionGranularity granularity, int numEvents)
            throws Exception {
        PlainPBStoragePlugin storagePlugin = new PlainPBStoragePlugin();
        PBCommonSetup setup = new PBCommonSetup();
        setup.setUpRootFolder(storagePlugin, "PlainPBConversionTest", granularity);
        logger.info("Testing failed conversion from " + ArchDBRTypes.DBR_SCALAR_DOUBLE + " to "
                + ArchDBRTypes.DBR_WAVEFORM_STRING + ". You could see an exception here; ignore it. It is expected");
        String pvName =
                "PlainPBConversionTest_" + ArchDBRTypes.DBR_SCALAR_DOUBLE + "_" + ArchDBRTypes.DBR_WAVEFORM_STRING;
        generateDataForArchDBRType(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, numEvents, storagePlugin);

        validateStream(pvName, numEvents, ArchDBRTypes.DBR_SCALAR_DOUBLE, storagePlugin);
        try {
            convertToType(pvName, ArchDBRTypes.DBR_WAVEFORM_STRING, storagePlugin);
        } catch (Exception ex) {
            Assertions.assertTrue(
                    ex.getCause() instanceof ConversionException,
                    "Expecting a Conversion Exception, instead got a " + ex);
        }
        validateStream(pvName, numEvents, ArchDBRTypes.DBR_SCALAR_DOUBLE, storagePlugin);
    }

    private void generateDataForArchDBRType(
            String pvName, ArchDBRTypes dbrType, int numEvents, PlainPBStoragePlugin storagePlugin) throws Exception {
        ArrayListEventStream ret = new ArrayListEventStream(
                numEvents, new RemotableEventStreamDesc(dbrType, pvName, TimeUtils.getCurrentYear()));
        int eventsAdded = 0;
        Constructor<? extends DBRTimeEvent> serializingConstructor =
                DBR2PBTypeMapping.getPBClassFor(dbrType).getSerializingConstructor();
        try (SimulationEventStream simstream =
                new SimulationEventStream(dbrType, new ValueGenerator(dbrType, numEvents))) {
            for (Event simEvent : simstream) {
                DBRTimeEvent genEvent = serializingConstructor.newInstance(simEvent);
                if (eventsAdded % 1000 == 0) {
                    genEvent.addFieldValue("HIHI", "Test");
                    genEvent.addFieldValue("LOLO", "13:40:12");
                    if (eventsAdded % 2000 == 0) {
                        genEvent.markAsActualChange();
                    }
                }
                ret.add(genEvent);
                if (eventsAdded++ > numEvents) break;
            }
            try (BasicContext context = new BasicContext()) {
                storagePlugin.appendData(context, pvName, ret);
            }
        }
    }

    private void convertToType(String pvName, ArchDBRTypes destDBRType, PlainPBStoragePlugin storagePlugin)
            throws IOException {
        try (BasicContext context = new BasicContext()) {
            storagePlugin.convert(context, pvName, new ThruNumberAndStringConversion(destDBRType));
        }
    }

    private void validateStream(
            String pvName, int numEvents, ArchDBRTypes destDBRType, PlainPBStoragePlugin storagePlugin)
            throws Exception {
        long expectedCurrentEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
        int eventCount = 0;
        try (BasicContext context = new BasicContext()) {
            Timestamp startTime = TimeUtils.minusDays(TimeUtils.now(), 2 * 266);
            Timestamp endTime = TimeUtils.plusDays(TimeUtils.now(), 2 * 266);
            List<Callable<EventStream>> callables = storagePlugin.getDataForPV(context, pvName, startTime, endTime);
            for (Callable<EventStream> callable : callables) {
                try (EventStream strm = callable.call()) {
                    Assertions.assertEquals(
                            pvName,
                            strm.getDescription().getPvName(),
                            "Expecting pvName to be " + pvName + " instead it is "
                                    + strm.getDescription().getPvName());
                    Assertions.assertSame(
                            strm.getDescription().getArchDBRType(),
                            destDBRType,
                            "Expecting DBR type to be " + destDBRType.toString() + " instead it is "
                                    + strm.getDescription().getArchDBRType());
                    for (Event e : strm) {
                        DBRTimeEvent dbr = (DBRTimeEvent) e;
                        long epochSeconds = dbr.getEpochSeconds();
                        Assertions.assertEquals(
                                epochSeconds,
                                expectedCurrentEpochSeconds,
                                "Timestamp is different at event count " + eventCount + " Expected "
                                        + TimeUtils.convertToHumanReadableString(expectedCurrentEpochSeconds) + " got "
                                        + TimeUtils.convertToHumanReadableString(epochSeconds));
                        if (eventCount % 1000 == 0) {
                            Assertions.assertTrue(
                                    dbr.hasFieldValues(), "Expecting field values at event count " + eventCount);
                            Assertions.assertEquals(
                                    "Test", dbr.getFieldValue("HIHI"), "Expecting HIHI as Test at " + eventCount);
                            Assertions.assertEquals(
                                    "13:40:12",
                                    dbr.getFieldValue("LOLO"),
                                    "Expecting LOLO as 13:40:12 at " + eventCount);
                            if (eventCount % 2000 == 0) {
                                Assertions.assertTrue(
                                        dbr.isActualChange(),
                                        "Expecting field values to be actual change " + eventCount);
                            }
                        }
                        expectedCurrentEpochSeconds++;
                        eventCount++;
                    }
                }
            }
        }
        Assertions.assertEquals(eventCount, numEvents, "Expecting some events " + eventCount);
    }

    private record ValueGenerator(ArchDBRTypes dbrType, int numSamples) implements SimulationValueGenerator {

        @Override
        public int getNumberOfSamples(ArchDBRTypes type) {
            return numSamples;
        }

        @Override
        public SampleValue getSampleValue(ArchDBRTypes type, int secondsIntoYear) {
            return switch (dbrType) {
                case DBR_SCALAR_BYTE -> new ScalarValue<Byte>((byte) secondsIntoYear);
                case DBR_SCALAR_DOUBLE -> new ScalarValue<Double>((double) secondsIntoYear);
                case DBR_SCALAR_ENUM -> new ScalarValue<Short>((short) secondsIntoYear);
                case DBR_SCALAR_FLOAT -> new ScalarValue<Float>((float) secondsIntoYear);
                case DBR_SCALAR_INT -> new ScalarValue<Integer>(secondsIntoYear);
                case DBR_SCALAR_SHORT -> new ScalarValue<Short>((short) secondsIntoYear);
                case DBR_SCALAR_STRING -> new ScalarStringSampleValue(Integer.toString(secondsIntoYear));
                case DBR_V4_GENERIC_BYTES -> new ScalarStringSampleValue(Integer.toString(secondsIntoYear));
                case DBR_WAVEFORM_BYTE -> new VectorValue<Byte>(
                        Collections.nCopies(10 * secondsIntoYear, ((byte) (secondsIntoYear % 255))));
                case DBR_WAVEFORM_DOUBLE -> new VectorValue<Double>(
                        Collections.nCopies(10 * secondsIntoYear, ((double) secondsIntoYear)));
                case DBR_WAVEFORM_ENUM -> new VectorValue<Short>(
                        Collections.nCopies(10 * secondsIntoYear, ((short) secondsIntoYear)));
                case DBR_WAVEFORM_FLOAT -> new VectorValue<Float>(
                        Collections.nCopies(10 * secondsIntoYear, ((float) secondsIntoYear)));
                case DBR_WAVEFORM_INT -> new VectorValue<Integer>(
                        Collections.nCopies(10 * secondsIntoYear, secondsIntoYear));
                case DBR_WAVEFORM_SHORT -> new VectorValue<Short>(
                        Collections.nCopies(10 * secondsIntoYear, ((short) secondsIntoYear)));
                case DBR_WAVEFORM_STRING -> new VectorStringSampleValue(
                        Collections.nCopies(10 * secondsIntoYear, Integer.toString(secondsIntoYear)));
                default -> throw new UnsupportedOperationException();
            };
        }
    }
}
