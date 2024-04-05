package edu.stanford.slac.archiverappliance.PB.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class FieldValuesTest {
    private static Logger logger = LogManager.getLogger(FieldValuesTest.class.getName());

    @Test
    public void testFieldValues() throws Exception {
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
                    assertTrue(
                            "Adding 1 field value does not turn hasFieldValues on ", pb1DbrTimeEvent.hasFieldValues());
                    assertTrue(
                            "Adding 1 field value yields different results " + pb1DbrTimeEvent.getFieldValue(fieldName),
                            pb1DbrTimeEvent.getFieldValue(fieldName).equals(fieldValue));
                    assertTrue(
                            "Adding 1 field value default of isActualChange is true ",
                            !pb1DbrTimeEvent.isActualChange());
                    assertTrue(
                            "Adding 1 field value getFieldNames does not contain field ",
                            pb1DbrTimeEvent.getFields().containsKey(fieldName));
                    pb1DbrTimeEvent.markAsActualChange();
                    assertTrue(
                            "Adding 1 field value after marking as actual change isActualChange is false ",
                            pb1DbrTimeEvent.isActualChange());

                    // Test adding multiple fields at the same time.
                    DBRTimeEvent pbm0 = constructorFromDBRTimeEvent.newInstance(ev);
                    HashMap<String, String> values = new HashMap<String, String>();
                    values.put("HIHI", "0.0");
                    values.put("LOLO", "-1.0");
                    values.put("LOPR", "1000.0");
                    values.put("HOPR", "-10000.0");
                    pbm0.setFieldValues(values, false);
                    assertTrue("Adding multiple field values does not turn hasFieldValues on ", pbm0.hasFieldValues());
                    assertTrue(
                            "Adding multiple field values after marking as cached isActualChange is true ",
                            !pbm0.isActualChange());
                    assertTrue(
                            "Adding multiple field values yields different results ",
                            compareMaps(values, pbm0.getFields()));

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
