package org.epics.archiverappliance.engine.pv;

import org.epics.pva.data.PVADouble;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.PVAStructureArray;
import org.epics.pva.data.PVAny;
import org.epics.pva.data.nt.PVATimeStamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

public class FieldValuesCacheTest {
    @Test
    public void testGetCurrentFieldValues() throws Exception {
        var pvaStructure = new PVAStructure(
                "pvaStructure",
                "struct_name",
                new PVAString("string0", "String0"),
                new PVAString("string1", "String1"));
        var fieldValuesCache = new FieldValuesCache(pvaStructure, false);
        var expectedMap = new HashMap<>();
        expectedMap.put("string0", "String0");
        expectedMap.put("string1", "String1");
        Assertions.assertEquals(expectedMap, fieldValuesCache.getCurrentFieldValues());

        ((PVAString) pvaStructure.get("string0")).set("String2");
        var bitSet = new BitSet();
        bitSet.set(1, true);
        fieldValuesCache.updateFieldValues(pvaStructure, bitSet);
        fieldValuesCache.getUpdatedFieldValues(false, new ArrayList<>());
        expectedMap.put("string0", "String2");
        Assertions.assertEquals(expectedMap, fieldValuesCache.getCurrentFieldValues());
    }

    @Test
    public void testTimeStampSubStructure() throws Exception {
        var timeStamp = new PVATimeStamp(Instant.now());
        var pvaStructure = new PVAStructure(
                "pvaStructure",
                "struct_name",
                new PVAString("string0", "String0"),
                new PVAString("string1", "String1"),
                timeStamp,
                new PVAStructure("a", "b", timeStamp));
        var fieldValuesCache = new FieldValuesCache(pvaStructure, false);
        var expectedMap = new HashMap<>();
        expectedMap.put("string0", "String0");
        expectedMap.put("string1", "String1");
        expectedMap.put("a.timeStamp", timeStamp.format());
        expectedMap.put("timeStamp", timeStamp.format());
        Assertions.assertEquals(expectedMap, fieldValuesCache.getCurrentFieldValues());

        timeStamp.set(Instant.now());
        var bitSet = new BitSet();
        bitSet.set(3, true);
        fieldValuesCache.updateFieldValues(pvaStructure, bitSet);
        fieldValuesCache.getUpdatedFieldValues(false, new ArrayList<>());
        expectedMap.put("a.timeStamp", timeStamp.format());
        Assertions.assertEquals(expectedMap, fieldValuesCache.getCurrentFieldValues());
    }

    @Test
    public void testAnySubStructure() throws Exception {
        var pvaStructure = new PVAStructure(
                "pvaStructure",
                "struct_name",
                new PVAny("anyValue", new PVAString("string2", "String2")),
                new PVAStructure(
                        "anyStruct",
                        "any_struct",
                        new PVAny("anyValue1", new PVAString("string3", "String3")),
                        new PVAny("anyValue2", new PVAString("stringblank", ""))));
        var fieldValuesCache = new FieldValuesCache(pvaStructure, false);
        var expectedMap = new HashMap<>();
        expectedMap.put("anyValue", pvaStructure.get("anyValue").format());
        expectedMap.put(
                "anyStruct.anyValue1",
                pvaStructure.locate("anyStruct.anyValue1").format());
        expectedMap.put(
                "anyStruct.anyValue2",
                pvaStructure.locate("anyStruct.anyValue2").format());
        Assertions.assertEquals(expectedMap, fieldValuesCache.getCurrentFieldValues());

        pvaStructure.get("anyValue").setValue(new PVAString("string4", "String4"));
        var bitSet = new BitSet();
        bitSet.set(1, true);
        fieldValuesCache.updateFieldValues(pvaStructure, bitSet);
        fieldValuesCache.getUpdatedFieldValues(false, new ArrayList<>());
        expectedMap.put("anyValue", pvaStructure.get("anyValue").format());
        Assertions.assertEquals(expectedMap, fieldValuesCache.getCurrentFieldValues());

        pvaStructure.locate("anyStruct.anyValue1").setValue(new PVAString("string5", "String5"));
        bitSet = new BitSet();
        bitSet.set(2, true);
        fieldValuesCache.updateFieldValues(pvaStructure, bitSet);
        fieldValuesCache.getUpdatedFieldValues(false, new ArrayList<>());
        expectedMap.put(
                "anyStruct.anyValue1",
                pvaStructure.locate("anyStruct.anyValue1").format());
        Assertions.assertEquals(expectedMap, fieldValuesCache.getCurrentFieldValues());
    }

    @Test
    public void testStructureArray() throws Exception {
        var pvaStructure = new PVAStructure(
                "pvaStructure",
                "struct_name",
                new PVAStructureArray(
                        "array",
                        new PVAStructure("struct", "structure", new PVAString("string0")),
                        new PVAStructure("struct", "structure", new PVAString("string0", "String0"))));
        var fieldValuesCache = new FieldValuesCache(pvaStructure, false);
        var expectedMap = new HashMap<>();
        expectedMap.put("array", "[" + ((PVAStructureArray) pvaStructure.get("array")).get()[0].format() + "]");
        Assertions.assertEquals(expectedMap, fieldValuesCache.getCurrentFieldValues());

        ((PVAStructureArray) pvaStructure.get("array"))
                .set(new PVAStructure[] {new PVAStructure("struct", "structure", new PVAString("string0", "String1"))});

        var bitSet = new BitSet();
        bitSet.set(1, true);
        fieldValuesCache.updateFieldValues(pvaStructure, bitSet);
        fieldValuesCache.getUpdatedFieldValues(false, new ArrayList<>());
        expectedMap.put("array", "[" + ((PVAStructureArray) pvaStructure.get("array")).get()[0].format() + "]");
        Assertions.assertEquals(expectedMap, fieldValuesCache.getCurrentFieldValues());
    }

    @Test
    public void testGetTimeStampBits() {
        var timeStamp = new PVAStructure(
                "timeStamp", "timeStamp_t", new PVAString("string", "String"), new PVAString("string2", "String2"));

        var pvaStructure = new PVAStructure("structureName", "struct_name", timeStamp);
        var fieldValuesCache = new FieldValuesCache(pvaStructure, false);
        BitSet expectedBitSet = new BitSet();
        expectedBitSet.set(1, true);
        expectedBitSet.set(2, true);
        expectedBitSet.set(3, true);

        Assertions.assertEquals(expectedBitSet, fieldValuesCache.getTimeStampBits());

        pvaStructure = new PVAStructure("structureName", "struct_name", new PVAString("string3", "String3"), timeStamp);
        expectedBitSet.clear();
        expectedBitSet.set(2, true);
        expectedBitSet.set(3, true);
        expectedBitSet.set(4, true);

        Assertions.assertEquals(expectedBitSet, new FieldValuesCache(pvaStructure, false).getTimeStampBits());

        pvaStructure = new PVAStructure("structureName", "struct_name");
        expectedBitSet.clear();

        Assertions.assertEquals(expectedBitSet, new FieldValuesCache(pvaStructure, false).getTimeStampBits());
    }

    @Test
    public void testEmptyGetUpdatedFieldValues() throws Exception {
        // Test Empty structure
        var pvaStructure = new PVAStructure("structureName", "struct_name");
        var actualFieldValues = new FieldValuesCache(pvaStructure, false);
        actualFieldValues.updateFieldValues(pvaStructure, new BitSet());
        var actualMap = actualFieldValues.getUpdatedFieldValues(false, new ArrayList<>());
        Assertions.assertEquals(new HashMap<>(), actualMap);
    }

    @Test
    public void testIgnoreDefaultGetUpdatedFieldValues() throws Exception {
        // Test ignore default stored values
        var timeStamp = new PVAStructure("timeStamp", "timeStamp_t", new PVAString("string", "String"));
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAString("alarmString", "alarmString"));
        var value = new PVAString("value", "String2");

        var pvaStructure = new PVAStructure("structureName", "struct_name", timeStamp, alarm, value);
        var allBits = new BitSet(22);
        allBits.set(1, true);
        allBits.set(2, true);
        allBits.set(3, true);
        allBits.set(4, true);
        allBits.set(5, true);

        var fieldValuesCache = new FieldValuesCache(pvaStructure, false);
        fieldValuesCache.updateFieldValues(pvaStructure, allBits);
        Assertions.assertEquals(new HashMap<>(), fieldValuesCache.getUpdatedFieldValues(false, new ArrayList<>()));
    }

    @Test
    public void testV4GetUpdatedFieldValues() throws Exception {
        // Test v4 changes
        var v4Value = new PVAString("v4string", "v4String");
        var allBits = new BitSet(22);
        allBits.set(1, true);
        allBits.set(2, true);
        allBits.set(3, true);
        allBits.set(4, true);
        allBits.set(5, true);
        allBits.set(6, true);
        var timeStamp = new PVAStructure("timeStamp", "timeStamp_t", new PVAString("string", "String"));
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAString("alarmString", "alarmString"));
        var value = new PVAString("value", "String2");
        var pvaStructure = new PVAStructure("structureName", "struct_name", timeStamp, alarm, value, v4Value);
        var fieldValuesCache = new FieldValuesCache(pvaStructure, false);
        var expectedMap = new HashMap<>();
        expectedMap.put("v4string", "v4String");
        fieldValuesCache.updateFieldValues(pvaStructure, allBits);
        Assertions.assertEquals(expectedMap, fieldValuesCache.getUpdatedFieldValues(false, new ArrayList<>()));

        // Test exclude v4 changes and get Everything
        expectedMap.put("timeStamp.string", "String");
        expectedMap.put("alarm.alarmString", "alarmString");
        fieldValuesCache = new FieldValuesCache(pvaStructure, true);
        fieldValuesCache.updateFieldValues(pvaStructure, allBits);
        Assertions.assertEquals(expectedMap, fieldValuesCache.getUpdatedFieldValues(true, new ArrayList<>()));

        // Test not exclude v4 changes and get everything
        fieldValuesCache = new FieldValuesCache(pvaStructure, false);
        fieldValuesCache.updateFieldValues(pvaStructure, allBits);
        Assertions.assertEquals(expectedMap, fieldValuesCache.getUpdatedFieldValues(true, new ArrayList<>()));

        // Test exclude v4 changes and not everything
        fieldValuesCache = new FieldValuesCache(pvaStructure, true);
        expectedMap = new HashMap<>();
        fieldValuesCache.updateFieldValues(pvaStructure, allBits);
        Assertions.assertEquals(expectedMap, fieldValuesCache.getUpdatedFieldValues(false, new ArrayList<>()));

        // Test exclude v4 changes and not everything with added meta field
        fieldValuesCache = new FieldValuesCache(pvaStructure, true);
        var metaFields = new ArrayList<String>();
        metaFields.add("v4string");
        expectedMap = new HashMap<>();
        expectedMap.put("v4string", "v4String");
        fieldValuesCache.updateFieldValues(pvaStructure, allBits);
        Assertions.assertEquals(expectedMap, fieldValuesCache.getUpdatedFieldValues(false, metaFields));

        // Test exclude v4 changes and not everything with added meta field that is not in structure
        fieldValuesCache = new FieldValuesCache(pvaStructure, true);
        metaFields = new ArrayList<String>();
        metaFields.add("v5string");
        expectedMap = new HashMap<>();
        fieldValuesCache.updateFieldValues(pvaStructure, allBits);
        Assertions.assertEquals(expectedMap, fieldValuesCache.getUpdatedFieldValues(false, metaFields));
    }

    @Test
    public void testV3V4GetUpdatedFieldValues() throws Exception {
        // Test v3 - v4 Mapping
        var limitLow = new PVADouble("limitLow", 1.0);
        var limitHigh = new PVADouble("limitHigh", 1.0);
        var description = new PVAString("description", "DESC");
        var units = new PVAString("units", "kHz");
        var display = new PVAStructure("display", "display_t", limitLow, limitHigh, description, units);
        var allBits = new BitSet(22);
        allBits.set(1, true);
        allBits.set(2, true);
        allBits.set(3, true);
        allBits.set(4, true);
        allBits.set(5, true);
        allBits.set(6, true);
        allBits.set(7, true);
        allBits.set(8, true);
        allBits.set(9, true);
        allBits.set(10, true);
        allBits.set(11, true);
        var expectedMap = new HashMap<>();
        expectedMap.put("LOPR", "1.0");
        expectedMap.put("HOPR", "1.0");
        expectedMap.put("DESC", "DESC");
        expectedMap.put("EGU", "kHz");

        var c_limitLow = new PVADouble("limitLow", 1.0);
        var c_limitHigh = new PVADouble("limitHigh", 1.0);
        var minStep = new PVADouble("minStep", 1.0);
        var control = new PVAStructure("control", "control_t", c_limitLow, c_limitHigh, minStep);
        allBits.set(12, true);
        allBits.set(13, true);
        allBits.set(14, true);
        allBits.set(15, true);
        expectedMap.put("DRVL", "1.0");
        expectedMap.put("DRVH", "1.0");
        expectedMap.put("PREC", "1.0");

        var lowAlarmLimit = new PVAInt("lowAlarmLimit", 1);
        var lowWarningLimit = new PVAInt("lowWarningLimit", 1);
        var highWarningLimit = new PVAInt("highWarningLimit", 1);
        var highAlarmLimit = new PVAInt("highAlarmLimit", 1);
        var hysteresis = new PVAInt("hysteresis", 1);
        expectedMap.put("LOLO", "1");
        expectedMap.put("LOW", "1");
        expectedMap.put("HIGH", "1");
        expectedMap.put("HIHI", "1");
        expectedMap.put("HYST", "1");

        var valueAlarm = new PVAStructure(
                "valueAlarm",
                "valueAlarm_t",
                lowAlarmLimit,
                lowWarningLimit,
                highWarningLimit,
                highAlarmLimit,
                hysteresis);
        allBits.set(16, true);
        allBits.set(17, true);
        allBits.set(18, true);
        allBits.set(19, true);
        allBits.set(20, true);
        allBits.set(21, true);

        var timeStamp = new PVAStructure("timeStamp", "timeStamp_t", new PVAString("string", "String"));
        var alarm = new PVAStructure("alarm", "alarm_t", new PVAString("alarmString", "alarmString"));
        var value = new PVAString("value", "String2");
        var v4Value = new PVAString("v4string", "v4String");
        var pvaStructure = new PVAStructure(
                "structureName", "struct_name", timeStamp, alarm, value, display, control, valueAlarm, v4Value);
        var fieldValuesCache = new FieldValuesCache(pvaStructure, true);
        fieldValuesCache.updateFieldValues(pvaStructure, allBits);
        Assertions.assertEquals(expectedMap, fieldValuesCache.getUpdatedFieldValues(false, new ArrayList<>()));

        expectedMap = new HashMap<>();
        expectedMap.put("v4string", "v4String");
        expectedMap.put("timeStamp.string", "String");
        expectedMap.put("alarm.alarmString", "alarmString");
        expectedMap.put("display.limitLow", "1.0");
        expectedMap.put("display.limitHigh", "1.0");
        expectedMap.put("display.description", "DESC");
        expectedMap.put("display.units", "kHz");
        expectedMap.put("control.limitLow", "1.0");
        expectedMap.put("control.limitHigh", "1.0");
        expectedMap.put("control.minStep", "1.0");
        expectedMap.put("valueAlarm.lowAlarmLimit", "1");
        expectedMap.put("valueAlarm.lowWarningLimit", "1");
        expectedMap.put("valueAlarm.highWarningLimit", "1");
        expectedMap.put("valueAlarm.highAlarmLimit", "1");
        expectedMap.put("valueAlarm.hysteresis", "1");
        fieldValuesCache.updateFieldValues(pvaStructure, allBits);
        Assertions.assertEquals(expectedMap, fieldValuesCache.getUpdatedFieldValues(true, new ArrayList<>()));
    }
}
