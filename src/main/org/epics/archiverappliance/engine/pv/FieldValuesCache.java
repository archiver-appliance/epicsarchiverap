/*
 * Copyright (C) 2023 European Spallation Source ERIC.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 */
package org.epics.archiverappliance.engine.pv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.pva.data.PVAAnyArray;
import org.epics.pva.data.PVABool;
import org.epics.pva.data.PVABoolArray;
import org.epics.pva.data.PVAByte;
import org.epics.pva.data.PVAByteArray;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVADouble;
import org.epics.pva.data.PVADoubleArray;
import org.epics.pva.data.PVAFloat;
import org.epics.pva.data.PVAFloatArray;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAIntArray;
import org.epics.pva.data.PVALong;
import org.epics.pva.data.PVALongArray;
import org.epics.pva.data.PVAShort;
import org.epics.pva.data.PVAShortArray;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.PVAStructureArray;
import org.epics.pva.data.PVAny;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Main purpose is to hold a cache of a pvaStructure as a flat map from string
 * to string.
 * <p>
 * Maps v4 field names to v3 field names in getters. Via:
 * <p>
 * display.limitLow, "LOPR"
 * display.limitHigh, "HOPR"
 * display.description, "DESC"
 * display.units, "EGU"
 * control.limitLow, "DRVL"
 * control.limitHigh, "DRVH"
 * control.minStep, "PREC"
 * valueAlarm.lowAlarmLimit, "LOLO"
 * valueAlarm.lowWarningLimit, "LOW"
 * valueAlarm.highWarningLimit, "HIGH"
 * valueAlarm.highAlarmLimit, "HIHI"
 * valueAlarm.hysteresis, "HYST"
 * <p>
 * Provides calculation of field values to update in a DBRTimeEvent.
 */
public class FieldValuesCache {
    private static final Logger logger = LogManager.getLogger(FieldValuesCache.class.getName());

    /**
     * Map the V4 names to the V3 names.
     */
    private static final HashMap<String, String> v4FieldNames2v3FieldNames = new HashMap<>();

    static {
        v4FieldNames2v3FieldNames.put("display.limitLow", "LOPR");
        v4FieldNames2v3FieldNames.put("display.limitHigh", "HOPR");
        v4FieldNames2v3FieldNames.put("display.description", "DESC");
        v4FieldNames2v3FieldNames.put("display.units", "EGU");
        v4FieldNames2v3FieldNames.put("control.limitLow", "DRVL");
        v4FieldNames2v3FieldNames.put("control.limitHigh", "DRVH");
        v4FieldNames2v3FieldNames.put("control.minStep", "PREC");
        v4FieldNames2v3FieldNames.put("valueAlarm.lowAlarmLimit", "LOLO");
        v4FieldNames2v3FieldNames.put("valueAlarm.lowWarningLimit", "LOW");
        v4FieldNames2v3FieldNames.put("valueAlarm.highWarningLimit", "HIGH");
        v4FieldNames2v3FieldNames.put("valueAlarm.highAlarmLimit", "HIHI");
        v4FieldNames2v3FieldNames.put("valueAlarm.hysteresis", "HYST");
    }

    /**
     * Cache the list of pvaScalar Classes
     */
    private static final Set<Class<? extends PVAData>> pvaScalarClasses = new HashSet<>();

    static {
        pvaScalarClasses.add(PVABool.class);
        pvaScalarClasses.add(PVAByte.class);
        pvaScalarClasses.add(PVADouble.class);
        pvaScalarClasses.add(PVAFloat.class);
        pvaScalarClasses.add(PVAInt.class);
        pvaScalarClasses.add(PVALong.class);
        pvaScalarClasses.add(PVAShort.class);
        pvaScalarClasses.add(PVAString.class);
        pvaScalarClasses.add(PVAny.class);
    }

    /**
     * Cache the list of pvaScalarArray Classes
     */
    private static final Set<Class<? extends PVAData>> pvaScalarArrayClasses = new HashSet<>();

    static {
        pvaScalarArrayClasses.add(PVABoolArray.class);
        pvaScalarArrayClasses.add(PVAByteArray.class);
        pvaScalarArrayClasses.add(PVADoubleArray.class);
        pvaScalarArrayClasses.add(PVAFloatArray.class);
        pvaScalarArrayClasses.add(PVAIntArray.class);
        pvaScalarArrayClasses.add(PVALongArray.class);
        pvaScalarArrayClasses.add(PVAShortArray.class);
        pvaScalarArrayClasses.add(PVAStringArray.class);
        pvaScalarArrayClasses.add(PVAAnyArray.class);
        pvaScalarArrayClasses.add(PVAStructureArray.class);
    }

    /**
     * Converts a list of parts to a dot separated string:
     * <p>
     * makeFullFieldName("abc", "def") = "abc.def"
     *
     * @param parts fieldNames
     * @return dot separated string of parts
     */
    private static String makeFullFieldName(final String... parts) {
        final StringWriter buf = new StringWriter();
        boolean firstDone = false;
        for (final String part : parts) {
            if (part == null || part.isEmpty()) {
                continue;
            }
            if (firstDone) {
                buf.write(".");
            } else {
                firstDone = true;
            }
            buf.write(part);
        }
        return buf.toString().trim();
    }

    /**
     * Creates a cache of indexes to dot separated fieldNames:
     * <p>
     * input:
     * pvStructure pv {
     * value 1,
     * timeStamp {
     * seconds 11,
     * nanos
     * }
     * }
     * <p>
     * returns: {"", "value", "timeStamp", "timeStamp.seconds", "timeStamp.nanos"}
     *
     * @param pvaData input data
     * @return Return list of full field names
     */
    private static List<String> get2BitFieldMapping(final PVAData pvaData) {
        final List<String> mapping = new ArrayList<>();
        add2BitFieldMapping("", pvaData, mapping);
        return mapping;
    }

    /**
     * Recursive version of the get2BitFieldMapping(PVAData)
     *
     * @param fldName current fieldName
     * @param pvaData input pvaData
     * @param mapping current Mapping in stage
     */
    private static void add2BitFieldMapping(final String fldName, final PVAData pvaData, final List<String> mapping) {
        final Class<? extends PVAData> type = pvaData.getClass();

        if (pvaScalarClasses.contains(type) || pvaScalarArrayClasses.contains(type)) {
            mapping.add(fldName);
        }

        if (type == PVAStructure.class) {

            mapping.add(fldName);
            for (final PVAData subPvaData : ((PVAStructure) pvaData).get()) {
                final String fieldName = subPvaData.getName();
                final String fulFldName = makeFullFieldName(fldName, fieldName);
                add2BitFieldMapping(fulFldName, subPvaData, mapping);
            }
        }
    }

    /**
     * Converts Scalar and ScalarArray {@link PVAData} to String
     *
     * @param pvaData scalar or scalar array
     * @return string of value or emptyString
     */
    private static String getScalarField(final PVAData pvaData) {
        final Class<? extends PVAData> type = pvaData.getClass();

        if (type == PVABool.class) {
            return Boolean.toString(((PVABool) pvaData).get());
        }
        if (type == PVAByte.class) {
            return Byte.toString(((PVAByte) pvaData).get());
        }
        if (type == PVADouble.class) {
            return Double.toString(((PVADouble) pvaData).get());
        }
        if (type == PVAFloat.class) {
            return Float.toString(((PVAFloat) pvaData).get());
        }
        if (type == PVAInt.class) {
            return Integer.toString(((PVAInt) pvaData).get());
        }
        if (type == PVALong.class) {
            return Long.toString(((PVALong) pvaData).get());
        }
        if (type == PVAShort.class) {
            return Short.toString(((PVAShort) pvaData).get());
        }
        if (type == PVAString.class) {
            return ((PVAString) pvaData).get();
        }
        if (type == PVABoolArray.class) {
            final boolean[] data = ((PVABoolArray) pvaData).get();
            return Arrays.toString(data);
        }
        if (type == PVAByteArray.class) {
            final byte[] data = ((PVAByteArray) pvaData).get();
            return Arrays.toString(data);
        }
        if (type == PVADoubleArray.class) {
            final double[] data = ((PVADoubleArray) pvaData).get();
            return Arrays.toString(data);
        }
        if (type == PVAFloatArray.class) {
            final float[] data = ((PVAFloatArray) pvaData).get();
            return Arrays.toString(data);
        }
        if (type == PVAIntArray.class) {
            final int[] data = ((PVAIntArray) pvaData).get();
            return Arrays.toString(data);
        }
        if (type == PVALongArray.class) {
            final long[] data = ((PVALongArray) pvaData).get();
            return Arrays.toString(data);
        }
        if (type == PVAShortArray.class) {
            final short[] data = ((PVAShortArray) pvaData).get();
            return Arrays.toString(data);
        }
        if (type == PVAStringArray.class) {
            final String[] data = ((PVAStringArray) pvaData).get();
            return Arrays.toString(data);
        }
        if (type == PVAAnyArray.class) {
            final PVAny[] data = ((PVAAnyArray) pvaData).get();
            return Arrays.toString(data);
        }
        if (type == PVAStructureArray.class) {
            final PVAStructure[] data = ((PVAStructureArray) pvaData).get();
            return Arrays.toString(data);
        }
        return pvaData.format();
    }

    /**
     * Converts non value elements of a {@link PVAStructure} to flat map of strings.
     * <p>
     * input:
     * pvStructure pv {
     * value 1,
     * timeStamp {
     * seconds 11,
     * nanos 12
     * },
     * otherValue 5
     * }
     * <p>
     * returns: {("timeStamp.seconds", "11"), ("timeStamp.nanos", "12"),
     * ("otherValue", "5")}
     *
     * @param pvStructure input structure for conversion
     * @return flatmap of input
     */
    private static Map<String, String> currentFieldValues(final PVAStructure pvStructure) {
        final Map<String, String> fieldValues = new HashMap<>();
        currentFieldValues(null, pvStructure, fieldValues);
        return fieldValues;
    }

    private static void currentFieldValues(
            final String rootName, final PVAStructure pvStructure, final Map<String, String> fieldValues) {
        for (final PVAData pvaData : pvStructure.get()) {
            final String fieldName = pvaData.getName();
            if (fieldName.equals("value")) continue;
            final Class<? extends PVAData> type = pvaData.getClass();

            if (type == PVAStructure.class) {
                currentFieldValues(makeFullFieldName(rootName, fieldName), ((PVAStructure) pvaData), fieldValues);
            } else {
                fieldValues.put(makeFullFieldName(rootName, fieldName), getScalarField(pvaData));
            }
        }
    }

    /**
     * Get the changed field values of the input pvaStructure given a bitset of
     * changes
     *
     * @param pvaStructure         input structure
     * @param changes              bitset of changes
     * @param bit2FieldNameMapping mapping of indexes in changes to fullFieldNames
     * @return flat map of string to string changes
     * @throws Exception Throws when structure is invalid.
     */
    private static Map<String, String> changedFieldValues(
            final PVAStructure pvaStructure, final BitSet changes, final List<String> bit2FieldNameMapping)
            throws Exception {

        final HashMap<String, String> fieldValues = new HashMap<>();

        for (int i = changes.nextSetBit(0); i >= 0; i = changes.nextSetBit(i + 1)) {
            String fName = "";
            if (bit2FieldNameMapping.size() > i) {
                fName = bit2FieldNameMapping.get(i);
            } else {
                logger.error("Structure changed " + pvaStructure + " changes " + changes + " bit2FieldNameMapping "
                        + bit2FieldNameMapping);
                continue;
            }
            if (!(fName.isEmpty()
                    || fName.startsWith("value.")
                    || fName.startsWith("timeStamp.")
                    || fName.startsWith("alarm.")
                    || fName.equals("value")
                    || fName.equals("timeStamp")
                    || fName.equals("alarm"))) {
                logger.debug("Field " + fName + " has changed");
                final var value = pvaStructure.locate(fName);
                if (value.getClass() == PVAStructure.class) {
                    currentFieldValues(fName, (PVAStructure) value, fieldValues);
                } else {
                    fieldValues.put(fName, getScalarField(value));
                }
            }
        }
        return fieldValues;
    }

    /**
     * Conversion of a flat map of field values to one with v3 names via an input
     * mapping.
     *
     * @param fieldValues input field values
     * @return converted hashmap
     */
    private static HashMap<String, String> v3NamedValues(final Map<String, String> fieldValues) {
        final HashMap<String, String> result = new HashMap<>();
        for (final Entry<String, String> e : fieldValues.entrySet()) {

            final var v3Name = (FieldValuesCache.v4FieldNames2v3FieldNames).get(e.getKey());
            if (v3Name != null) {
                result.put(v3Name, e.getValue());
            } else {
                result.put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    /**
     * Cache of indexes to fullFieldNames of a pvStructure
     */
    private final List<String> bit2FieldNameMapping;

    /**
     * Current Field values of a pvStructure as a flat map
     */
    private final Map<String, String> cachedFieldValues;

    /**
     * Last changed fields
     */
    private Set<String> lastChangedFields;

    /**
     * Determine if to include any changes to pvaAccess field values
     * when they change, or to ignore.
     */
    private final boolean excludeV4Changes;

    /**
     * Constructor of the FieldValuesCache of a PVAStructure
     *
     * @param pvaStructure     Structure to make cache from
     * @param excludeV4Changes If to include pvaAccess extra values
     */
    public FieldValuesCache(final PVAStructure pvaStructure, final boolean excludeV4Changes) {
        this.bit2FieldNameMapping = get2BitFieldMapping(pvaStructure);
        this.cachedFieldValues = currentFieldValues(pvaStructure);
        this.excludeV4Changes = excludeV4Changes;
    }

    /**
     * Get the updated field values of a pvaStructure as a flat map of strings.
     *
     * @param getEverything If to retrieve every field value regardless if maps to a
     *                      v3 type or not and keep not mapped
     * @param metaFieldNames Any fields that should always be archived if they change
     * @return A flat map of String values
     */
    public HashMap<String, String> getUpdatedFieldValues(
            final boolean getEverything, final List<String> metaFieldNames) {
        if (getEverything) {
            return (HashMap<String, String>) this.cachedFieldValues;
        }
        Map<String, String> changed = new HashMap<>();
        for (String key : this.lastChangedFields) {
            String values = this.cachedFieldValues.get(key);
            if (values != null) {
                changed.put(key, this.cachedFieldValues.get(key));
            }
        }
        if (this.excludeV4Changes) {
            changed = changed.entrySet().stream()
                    .filter(e -> FieldValuesCache.v4FieldNames2v3FieldNames.containsKey(e.getKey())
                            || metaFieldNames.contains(e.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }
        return v3NamedValues(changed);
    }

    /**
     * Updated the field values of a pvaStructure as a flat map of strings.
     *
     * @param pvaStructure  input structure, must have same structure as input
     *                      pvStructure
     * @param changes       The bitset containing the indexes of changes in the
     *                      PVAStructure
     * @throws Exception Throws when structure is invalid.
     */
    public void updateFieldValues(PVAStructure pvaStructure, BitSet changes) throws Exception {
        var changed = changes == null
                ? this.cachedFieldValues
                : changedFieldValues(pvaStructure, changes, this.bit2FieldNameMapping);
        this.lastChangedFields = changed.keySet();
        this.cachedFieldValues.putAll(changed);
    }

    /**
     * Return the currently cached flat map of string values
     *
     * @return flat map of string values
     */
    public HashMap<String, String> getCurrentFieldValues() {
        return v3NamedValues(cachedFieldValues);
    }

    /**
     * Get the bitset of the timeStamp bits of the input pvaStructure
     *
     * @return a bitset of timeStamp bits
     */
    public BitSet getTimeStampBits() {
        final BitSet bits = new BitSet();
        for (int i = 0; i < this.bit2FieldNameMapping.size(); i++) {
            final String fullFieldName = this.bit2FieldNameMapping.get(i);
            if (fullFieldName.startsWith("timeStamp")) {
                bits.set(i);
            }
        }
        return bits;
    }

    @Override
    public String toString() {
        return "FieldValuesCache{" + "bit2FieldNameMapping="
                + bit2FieldNameMapping + ", cachedFieldValues="
                + cachedFieldValues + ", excludeV4Changes="
                + excludeV4Changes + '}';
    }
}
