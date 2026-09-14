package edu.stanford.slac.archiverappliance.PB.data;

import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.HashMap;

class FieldValuesTest {

    @Test
    void testAddFieldValueAppendsIncrementally() throws Exception {
        executeForV3Types(event -> {
            event.addFieldValue("cnxlostepsecs", "12345");
            event.addFieldValue("cnxregainedepsecs", "12346");

            Assertions.assertTrue(event.hasFieldValues(), "Adding field values should turn on hasFieldValues");
            Assertions.assertFalse(event.isActualChange(), "addFieldValue defaults to isActualChange = false");

            HashMap<String, String> fields = event.getFields();
            Assertions.assertEquals(2, fields.size(), "Fields map should contain exactly 2 entries");
            Assertions.assertEquals("12345", fields.get("cnxlostepsecs"));
            Assertions.assertEquals("12346", fields.get("cnxregainedepsecs"));

            // Adding a third field incrementally
            event.addFieldValue("startup", "true");
            Assertions.assertEquals(3, event.getFields().size(), "Fields map should now contain 3 entries");
        });
    }

    @Test
    void testSetFieldValuesOverwritesExistingFields() throws Exception {
        executeForV3Types(event -> {
            HashMap<String, String> initialValues = new HashMap<>();
            initialValues.put("HIHI", "10.0");
            initialValues.put("LOLO", "-10.0");

            event.setFieldValues(initialValues, false);
            Assertions.assertTrue(event.hasFieldValues());
            Assertions.assertEquals(2, event.getFields().size());

            // setFieldValues represents a complete state update, so it must overwrite
            HashMap<String, String> newValues = new HashMap<>();
            newValues.put("DESC", "My PV Description");

            event.setFieldValues(newValues, true);
            Assertions.assertTrue(
                    event.isActualChange(), "Setting with markAsActualChange=true should update the flag");

            HashMap<String, String> resultingFields = event.getFields();
            Assertions.assertEquals(
                    1, resultingFields.size(), "setFieldValues should overwrite previous fields entirely");
            Assertions.assertEquals("My PV Description", resultingFields.get("DESC"));
            Assertions.assertFalse(resultingFields.containsKey("HIHI"));
        });
    }

    @Test
    void testDoubleStoring() throws Exception {
        executeForV3Types(event -> {
            HashMap<String, String> values = new HashMap<>();
            values.put("HOPR", "100.0");
            values.put("LOPR", "0.0");

            event.setFieldValues(values, false);
            int sizeAfterFirstSet = event.getRawForm().len;

            // Set the exact same fields again. This should not increase the payload size.
            event.setFieldValues(values, false);
            int sizeAfterSecondSet = event.getRawForm().len;

            Assertions.assertEquals(sizeAfterFirstSet, sizeAfterSecondSet);
        });
    }

    @Test
    void testSerializationAndUnmarshalling() throws Exception {
        for (ArchDBRTypes dbrType : ArchDBRTypes.values()) {
            if (!dbrType.isV3Type()) continue;

            short year = TimeUtils.getCurrentYear();
            BoundaryConditionsSimulationValueGenerator valuegenerator =
                    new BoundaryConditionsSimulationValueGenerator();

            try (SimulationEventStream simstream = new SimulationEventStream(
                    dbrType, valuegenerator, TimeUtils.getStartOfYear(year), TimeUtils.getEndOfYear(year), 1)) {

                PBTypeSystem pbTypeSystem = new PBTypeSystem();
                Constructor<? extends DBRTimeEvent> constructorFromDBRTimeEvent =
                        pbTypeSystem.getSerializingConstructor(dbrType);
                Constructor<? extends DBRTimeEvent> constructorFromBytes =
                        pbTypeSystem.getUnmarshallingFromByteArrayConstructor(dbrType);

                Event ev = simstream.iterator().next();
                DBRTimeEvent pbEvent = constructorFromDBRTimeEvent.newInstance(ev);

                HashMap<String, String> values = new HashMap<>();
                values.put("EGU", "Volts");
                values.put("PREC", "3");
                pbEvent.setFieldValues(values, true);

                ByteArray rawBytes = pbEvent.getRawForm();
                DBRTimeEvent unmarshalledEvent = constructorFromBytes.newInstance(year, rawBytes);

                Assertions.assertTrue(unmarshalledEvent.hasFieldValues());
                Assertions.assertTrue(unmarshalledEvent.isActualChange());
                Assertions.assertEquals("Volts", unmarshalledEvent.getFieldValue("EGU"));
                Assertions.assertEquals("3", unmarshalledEvent.getFieldValue("PREC"));
            }
        }
    }

    /**
     * Helper interface to run assertions on a fresh DBRTimeEvent instance.
     */
    private interface EventTest {
        void test(DBRTimeEvent event) throws Exception;
    }

    private void executeForV3Types(EventTest testLogic) throws Exception {
        PBTypeSystem pbTypeSystem = new PBTypeSystem();
        BoundaryConditionsSimulationValueGenerator valuegenerator = new BoundaryConditionsSimulationValueGenerator();
        short year = TimeUtils.getCurrentYear();

        for (ArchDBRTypes dbrType : ArchDBRTypes.values()) {
            if (!dbrType.isV3Type()) continue;

            try (SimulationEventStream simstream = new SimulationEventStream(
                    dbrType, valuegenerator, TimeUtils.getStartOfYear(year), TimeUtils.getEndOfYear(year), 1)) {

                Constructor<? extends DBRTimeEvent> constructorFromDBRTimeEvent =
                        pbTypeSystem.getSerializingConstructor(dbrType);
                Event rawEvent = simstream.iterator().next();
                DBRTimeEvent event = constructorFromDBRTimeEvent.newInstance(rawEvent);

                testLogic.test(event);
            }
        }
    }
}
