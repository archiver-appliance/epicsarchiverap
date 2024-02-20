/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.channelarchiver;

import com.google.protobuf.Message;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * We get a HashMap of NVPairs from the Channel Archiver - this class exposes these as an archiver Event
 * We typically get secs=1250696265, value=70.9337, sevr=0, nano=267115322, stat=0
 * @author mshankar
 *
 */
public class HashMapEvent implements DBRTimeEvent {
    public static final String SECS_FIELD_NAME = "secs";
    public static final String NANO_FIELD_NAME = "nano";
    public static final String VALUE_FIELD_NAME = "value";
    public static final String STAT_FIELD_NAME = "stat";
    public static final String SEVR_FIELD_NAME = "sevr";
    public static final String FIELD_VALUES_FIELD_NAME = "fields";
    public static final String FIELD_VALUES_ACTUAL_CHANGE = "fieldsAreActualChange";

    private HashMap<String, Object> values;
    private ArchDBRTypes type;

    public HashMapEvent(ArchDBRTypes type, HashMap<String, Object> values) {
        this.values = values;
        this.type = type;
    }

    public HashMapEvent(ArchDBRTypes type, DBRTimeEvent event) {
        this.type = type;
        values = new HashMap<String, Object>();
        values.put(HashMapEvent.SECS_FIELD_NAME, Long.toString(event.getEpochSeconds()));
        values.put(
                HashMapEvent.NANO_FIELD_NAME,
                Integer.toString(event.getEventTimeStamp().getNano()));
        values.put(HashMapEvent.STAT_FIELD_NAME, Integer.toString(event.getStatus()));
        values.put(HashMapEvent.SEVR_FIELD_NAME, Integer.toString(event.getSeverity()));
        if (event.hasFieldValues()) {
            values.put(FIELD_VALUES_FIELD_NAME, event.getFields());
        }
        if (event.isActualChange()) {
            values.put(FIELD_VALUES_ACTUAL_CHANGE, Boolean.TRUE.toString());
        }
        values.put(VALUE_FIELD_NAME, event.getSampleValue().toString());
    }

    public void setValue(String name, String newValue) {
        this.values.put(name, newValue);
    }

    @Override
    public Event makeClone() {
        return new HashMapEvent(this.type, new HashMap<String, Object>(this.values));
    }

    @Override
    public int getRepeatCount() {
        return 0;
    }

    @Override
    public void setRepeatCount(int repeatCount) {}

    @Override
    public int getStatus() {
        return Integer.parseInt((String) values.get(STAT_FIELD_NAME));
    }

    @Override
    public void setStatus(int status) {
        values.put(STAT_FIELD_NAME, Integer.valueOf(status).toString());
    }

    @Override
    public int getSeverity() {
        return Integer.parseInt((String) values.get(SEVR_FIELD_NAME));
    }

    @Override
    public void setSeverity(int severity) {
        values.put(SEVR_FIELD_NAME, Integer.valueOf(severity).toString());
    }

    @Override
    public long getEpochSeconds() {
        return Long.parseLong((String) values.get(SECS_FIELD_NAME));
    }

    /**
     * @return
     */
    @Override
    public Message getMessage() {
        return null;
    }

    /**
     * @return
     */
    @Override
    public Class<? extends Message> getMessageClass() {
        return null;
    }

    @Override
    public Instant getEventTimeStamp() {
        return Instant.ofEpochSecond(Long.parseLong((String) values.get(SECS_FIELD_NAME)), Integer.parseInt((String)
                values.get(NANO_FIELD_NAME)));
    }

    @Override
    public ByteArray getRawForm() {
        throw new UnsupportedOperationException("There should be no need to support a raw form here.");
    }

    @Override
    public SampleValue getSampleValue() {
        switch (type) {
            case DBR_SCALAR_FLOAT:
            case DBR_SCALAR_DOUBLE: {
                String strValue = (String) values.get(VALUE_FIELD_NAME);
                try {
                    return new ScalarValue<Double>(Double.parseDouble(strValue));
                } catch (NumberFormatException nex) {
                    if (strValue.equals("nan")) {
                        // logger.debug("Got a nan from the ChannelArchiver; returning Double.Nan instead");
                        return new ScalarValue<Double>(Double.NaN);
                    } else {
                        throw nex;
                    }
                }
            }
            case DBR_SCALAR_BYTE:
            case DBR_SCALAR_SHORT:
            case DBR_SCALAR_ENUM:
            case DBR_SCALAR_INT: {
                String strValue = (String) values.get(VALUE_FIELD_NAME);
                return new ScalarValue<Integer>(Integer.parseInt(strValue));
            }
            case DBR_SCALAR_STRING: {
                String strValue = (String) values.get(VALUE_FIELD_NAME);
                return new ScalarStringSampleValue(strValue);
            }
            case DBR_WAVEFORM_FLOAT:
            case DBR_WAVEFORM_DOUBLE: {
                // No choice but to add this SuppressWarnings here.
                @SuppressWarnings("unchecked")
                LinkedList<String> vals = (LinkedList<String>) values.get(VALUE_FIELD_NAME);
                LinkedList<Double> dvals = new LinkedList<Double>();
                for (String val : vals) dvals.add(Double.parseDouble(val));
                return new VectorValue<Double>(dvals);
            }
            case DBR_WAVEFORM_ENUM:
            case DBR_WAVEFORM_SHORT:
            case DBR_WAVEFORM_BYTE:
            case DBR_WAVEFORM_INT: {
                // No choice but to add this SuppressWarnings here.
                @SuppressWarnings("unchecked")
                LinkedList<String> vals = (LinkedList<String>) values.get(VALUE_FIELD_NAME);
                LinkedList<Integer> ivals = new LinkedList<Integer>();
                for (String val : vals) ivals.add(Integer.parseInt(val));
                return new VectorValue<Integer>(ivals);
            }
            case DBR_WAVEFORM_STRING: {
                // No choice but to add this SuppressWarnings here.
                @SuppressWarnings("unchecked")
                LinkedList<String> vals = (LinkedList<String>) values.get(VALUE_FIELD_NAME);
                return new VectorStringSampleValue(vals);
            }
            case DBR_V4_GENERIC_BYTES: {
                throw new UnsupportedOperationException("Channel Archiver does not support V4 yet.");
            }
            default:
                throw new UnsupportedOperationException("Unknown DBR type " + type);
        }
    }

    @Override
    public boolean hasFieldValues() {
        return this.values.containsKey(FIELD_VALUES_FIELD_NAME);
    }

    @Override
    public boolean isActualChange() {
        return this.values.containsKey(FIELD_VALUES_ACTUAL_CHANGE)
                && Boolean.valueOf((String) this.values.get(FIELD_VALUES_ACTUAL_CHANGE));
    }

    @SuppressWarnings("unchecked")
    @Override
    public HashMap<String, String> getFields() {
        return (HashMap<String, String>) this.values.get(FIELD_VALUES_FIELD_NAME);
    }

    @SuppressWarnings("unchecked")
    @Override
    public String getFieldValue(String fieldName) {
        return ((HashMap<String, String>) this.values.get(FIELD_VALUES_FIELD_NAME)).get(fieldName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void addFieldValue(String fieldName, String fieldValue) {
        ((HashMap<String, String>) this.values.get(FIELD_VALUES_FIELD_NAME)).put(fieldName, fieldValue);
    }

    @Override
    public void markAsActualChange() {
        this.values.put(FIELD_VALUES_ACTUAL_CHANGE, Boolean.TRUE.toString());
    }

    @Override
    public void setFieldValues(HashMap<String, String> fieldValues, boolean markAsActualChange) {
        this.values.put(FIELD_VALUES_FIELD_NAME, fieldValues);
        if (markAsActualChange) {
            this.values.put(FIELD_VALUES_ACTUAL_CHANGE, Boolean.TRUE.toString());
        }
    }

    @Override
    public ArchDBRTypes getDBRType() {
        return this.type;
    }
}
