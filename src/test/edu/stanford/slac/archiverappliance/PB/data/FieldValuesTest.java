package edu.stanford.slac.archiverappliance.PB.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    private static final Logger logger = LogManager.getLogger(FieldValuesTest.class.getName());

    @Test
    void testFieldValues() throws Exception {
        for (ArchDBRTypes dbrType : ArchDBRTypes.values()) {
            if (!dbrType.isV3Type()) continue;
            logger.info("Testing setting and getting of field values for DBR_type: " + dbrType.name());

            short year = TimeUtils.getCurrentYear();
            BoundaryConditionsSimulationValueGenerator valuegenerator =
                    new BoundaryConditionsSimulationValueGenerator();
            short currentYear = TimeUtils.getCurrentYear();
            try (SimulationEventStream simstream = new SimulationEventStream(
                    dbrType,
                    valuegenerator,
                    TimeUtils.getStartOfYear(currentYear),
                    TimeUtils.getEndOfYear(currentYear),
                    1)) {
                PBTypeSystem pbTypeSystem = new PBTypeSystem();
                Constructor<? extends DBRTimeEvent> constructorFromDBRTimeEvent =
                        pbTypeSystem.getSerializingConstructor(dbrType);
                Constructor<? extends DBRTimeEvent> constructorFromBytes =
                        pbTypeSystem.getUnmarshallingFromByteArrayConstructor(dbrType);
                int fieldCount = 1;
                for (Event ev : simstream) {
                    // This should get the PB event
                    DBRTimeEvent pb0 = constructorFromDBRTimeEvent.newInstance(ev);
                    String fieldName = "HIHI";
                    String fieldValue = "0.0";
                    pb0.addFieldValue(fieldName, fieldValue);
                    ByteArray raw1 = pb0.getRawForm();
                    DBRTimeEvent pb1DbrTimeEvent = constructorFromBytes.newInstance(year, raw1);
                    Assertions.assertTrue(
                            pb1DbrTimeEvent.hasFieldValues(), "Adding 1 field value does not turn hasFieldValues on ");
                    Assertions.assertEquals(
                            pb1DbrTimeEvent.getFieldValue(fieldName),
                            fieldValue,
                            "Adding 1 field value yields different results "
                                    + pb1DbrTimeEvent.getFieldValue(fieldName));
                    Assertions.assertFalse(
                            pb1DbrTimeEvent.isActualChange(),
                            "Adding 1 field value default of isActualChange is true ");
                    Assertions.assertTrue(
                            pb1DbrTimeEvent.getFields().containsKey(fieldName),
                            "Adding 1 field value getFieldNames does not contain field ");
                    pb1DbrTimeEvent.markAsActualChange();
                    Assertions.assertTrue(
                            pb1DbrTimeEvent.isActualChange(),
                            "Adding 1 field value after marking as actual change isActualChange is false ");

                    // Test adding multiple fields at the same time.
                    DBRTimeEvent pbm0 = constructorFromDBRTimeEvent.newInstance(ev);
                    HashMap<String, String> values = new HashMap<>();
                    values.put("HIHI", "0.0");
                    values.put("LOLO", "-1.0");
                    values.put("LOPR", "1000.0");
                    values.put("HOPR", "-10000.0");
                    pbm0.setFieldValues(values, false);
                    Assertions.assertTrue(
                            pbm0.hasFieldValues(), "Adding multiple field values does not turn hasFieldValues on ");
                    Assertions.assertFalse(
                            pbm0.isActualChange(),
                            "Adding multiple field values after marking as cached isActualChange is true ");
                    Assertions.assertTrue(
                            compareMaps(values, pbm0.getFields()),
                            "Adding multiple field values yields different results ");

                    fieldCount++;
                    if (fieldCount > 10) {
                        break;
                    }
                }
            }
        }
    }

    private static boolean compareMaps(HashMap<String, String> map1, HashMap<String, String> map2) {
        if (map1.size() != map2.size()) {
            logger.error("The sizes are different");
            return false;
        }
        for (String key : map1.keySet()) {
            if (!map2.containsKey(key)) {
                logger.error("Map2 does not contain " + key);
                return false;
            }
            if (!map1.get(key).equals(map2.get(key))) {
                logger.error("Map1 has " + map1.get(key) + " and map2 has " + map2.get(key) + " for key " + key);
                return false;
            }
        }

        return true;
    }
}
