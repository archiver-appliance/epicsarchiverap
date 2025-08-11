/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.FieldValue;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.ScalarEnum.Builder;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBR_TIME_Enum;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRAlarm;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAStructure;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * A DBRTimeEvent for a scalar enum.
 * @author mshankar
 *
 */
public class PBScalarEnum implements DBRTimeEvent {
    ByteArray bar = null;
    short year = 0;
    EPICSEvent.ScalarEnum dbevent = null;

    public PBScalarEnum(short year, ByteArray bar) {
        this.bar = bar;
        this.year = year;
    }

    public PBScalarEnum(short year, Message.Builder message) {
        this.dbevent = (EPICSEvent.ScalarEnum) message.build();
        this.bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        this.year = year;
    }

    public PBScalarEnum(DBRTimeEvent ev) {
        YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(ev.getEventTimeStamp());
        year = yst.getYear();
        Builder builder = EPICSEvent.ScalarEnum.newBuilder()
                .setSecondsintoyear(yst.getSecondsintoyear())
                .setNano(yst.getNano())
                .setVal(ev.getSampleValue().getValue().intValue());
        if (ev.getSeverity() != 0) builder.setSeverity(ev.getSeverity());
        if (ev.getStatus() != 0) builder.setStatus(ev.getStatus());
        if (ev.hasFieldValues()) {
            HashMap<String, String> fields = ev.getFields();
            for (String fieldName : fields.keySet()) {
                FieldValue fv = EPICSEvent.FieldValue.newBuilder()
                        .setName(fieldName)
                        .setVal(fields.get(fieldName))
                        .build();
                builder.addFieldvalues(fv);
            }
            builder.setFieldactualchange(ev.isActualChange());
        }
        dbevent = builder.build();
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
    }

    public PBScalarEnum(DBR dbr) {
        DBR_TIME_Enum realtype = (DBR_TIME_Enum) dbr;
        YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(realtype.getTimeStamp());
        year = yst.getYear();
        Builder builder = EPICSEvent.ScalarEnum.newBuilder()
                .setSecondsintoyear(yst.getSecondsintoyear())
                .setNano(yst.getNano())
                .setVal(realtype.getEnumValue()[0]);
        if (realtype.getSeverity().getValue() != 0)
            builder.setSeverity(realtype.getSeverity().getValue());
        if (realtype.getStatus().getValue() != 0)
            builder.setStatus(realtype.getStatus().getValue());
        dbevent = builder.build();
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
    }

    public PBScalarEnum(PVAStructure v4Data) {
        YearSecondTimestamp yst = TimeUtils.convertFromPVTimeStamp(v4Data.get("timeStamp"));
        DBRAlarm alarm = DBRAlarm.convertPVAlarm(v4Data.get("alarm"));

        int value = ((PVAInt) ((PVAStructure) v4Data.get("value")).get("index")).get();

        year = yst.getYear();
        Builder builder = EPICSEvent.ScalarEnum.newBuilder()
                .setSecondsintoyear(yst.getSecondsintoyear())
                .setNano(yst.getNano())
                .setVal(value);
        if (alarm.severity != 0) builder.setSeverity(alarm.severity);
        if (alarm.status != 0) builder.setStatus(alarm.status);
        dbevent = builder.build();
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
    }

    @Override
    public Event makeClone() {
        return new PBScalarEnum(this);
    }

    @Override
    public Instant getEventTimeStamp() {
        unmarshallEventIfNull();
        return TimeUtils.convertFromYearSecondTimestamp(
                new YearSecondTimestamp(year, dbevent.getSecondsintoyear(), dbevent.getNano()));
    }

    @Override
    public long getEpochSeconds() {
        unmarshallEventIfNull();
        return TimeUtils.getStartOfYearInSeconds(year) + dbevent.getSecondsintoyear();
    }

    @Override
    public ByteArray getRawForm() {
        return bar;
    }

    @Override
    public SampleValue getSampleValue() {
        unmarshallEventIfNull();
        return new ScalarValue<Short>((short) dbevent.getVal());
    }

    @Override
    public int getStatus() {
        unmarshallEventIfNull();
        return dbevent.getStatus();
    }

    @Override
    public int getSeverity() {
        unmarshallEventIfNull();
        return dbevent.getSeverity();
    }

    @Override
    public int getRepeatCount() {
        unmarshallEventIfNull();
        return dbevent.getRepeatcount();
    }

    @Override
    public void setRepeatCount(int repeatCount) {
        unmarshallEventIfNull();
        dbevent = EPICSEvent.ScalarEnum.newBuilder()
                .mergeFrom(dbevent)
                .setRepeatcount(repeatCount)
                .build();
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        return;
    }

    @Override
    public void setStatus(int status) {
        unmarshallEventIfNull();
        if (status != 0) {
            dbevent = EPICSEvent.ScalarEnum.newBuilder()
                    .mergeFrom(dbevent)
                    .setStatus(status)
                    .build();
        } else {
            dbevent = EPICSEvent.ScalarEnum.newBuilder().mergeFrom(dbevent).build();
        }
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        return;
    }

    @Override
    public void setSeverity(int severity) {
        unmarshallEventIfNull();
        if (severity != 0) {
            dbevent = EPICSEvent.ScalarEnum.newBuilder()
                    .mergeFrom(dbevent)
                    .setSeverity(severity)
                    .build();
        } else {
            dbevent = EPICSEvent.ScalarEnum.newBuilder().mergeFrom(dbevent).build();
        }
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        return;
    }

    private void unmarshallEventIfNull() {
        try {
            if (dbevent == null) {
                dbevent = EPICSEvent.ScalarEnum.newBuilder()
                        .mergeFrom(bar.inPlaceUnescape().unescapedData, bar.off, bar.unescapedLen)
                        .build();
            }
        } catch (Exception ex) {
            throw new PBParseException(bar.toBytes(), ex);
        }
    }

    @Override
    public boolean hasFieldValues() {
        unmarshallEventIfNull();
        return dbevent.getFieldvaluesCount() > 0;
    }

    @Override
    public boolean isActualChange() {
        unmarshallEventIfNull();
        return dbevent.getFieldactualchange();
    }

    @Override
    public HashMap<String, String> getFields() {
        unmarshallEventIfNull();
        HashMap<String, String> ret = new HashMap<String, String>();
        List<FieldValue> fieldValues = dbevent.getFieldvaluesList();
        if (fieldValues != null && !fieldValues.isEmpty()) {
            for (FieldValue fieldValue : fieldValues) {
                ret.put(fieldValue.getName(), fieldValue.getVal());
            }
        }
        return ret;
    }

    @Override
    public String getFieldValue(String fieldName) {
        unmarshallEventIfNull();
        List<FieldValue> fieldValues = dbevent.getFieldvaluesList();
        if (fieldValues != null && !fieldValues.isEmpty()) {
            for (FieldValue fieldValue : fieldValues) {
                if (fieldValue.getName().equals(fieldName)) {
                    return fieldValue.getVal();
                }
            }
        }
        return null;
    }

    @Override
    public void addFieldValue(String fieldName, String fieldValue) {
        unmarshallEventIfNull();
        FieldValue fv = EPICSEvent.FieldValue.newBuilder()
                .setName(fieldName)
                .setVal(fieldValue)
                .build();
        dbevent = EPICSEvent.ScalarEnum.newBuilder()
                .mergeFrom(dbevent)
                .addFieldvalues(fv)
                .build();
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        return;
    }

    @Override
    public void markAsActualChange() {
        unmarshallEventIfNull();
        dbevent = EPICSEvent.ScalarEnum.newBuilder()
                .mergeFrom(dbevent)
                .setFieldactualchange(true)
                .build();
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        return;
    }

    @Override
    public void setFieldValues(HashMap<String, String> fieldValues, boolean markAsActualChange) {
        unmarshallEventIfNull();
        LinkedList<FieldValue> fieldValuesList = new LinkedList<FieldValue>();
        for (String fieldName : fieldValues.keySet()) {
            fieldValuesList.add(EPICSEvent.FieldValue.newBuilder()
                    .setName(fieldName)
                    .setVal(fieldValues.get(fieldName))
                    .build());
        }
        dbevent = EPICSEvent.ScalarEnum.newBuilder()
                .mergeFrom(dbevent)
                .addAllFieldvalues(fieldValuesList)
                .setFieldactualchange(markAsActualChange)
                .build();
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        return;
    }

    @Override
    public ArchDBRTypes getDBRType() {
        return ArchDBRTypes.DBR_SCALAR_ENUM;
    }

    @Override
    public Message getProtobufMessage() {
        unmarshallEventIfNull();
        return dbevent;
    }

    @Override
    public Class<? extends Message> getProtobufMessageClass() {
        return EPICSEvent.ScalarEnum.class;
    }

    @Override
    public String toString() {
        return DBRTimeEvent.toString(this);
    }
}
