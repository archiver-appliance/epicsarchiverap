package edu.stanford.slac.archiverappliance.PB.data;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.ByteBufSampleValue;
import org.epics.archiverappliance.data.DBRAlarm;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAStructure;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * A DBRTimeEvent that wraps a V4 struct. We store the val as a bunch of bytes
 * @author mshankar
 *
 */
public class PBV4GenericBytes implements DBRTimeEvent, PartionedTime {
    private static final Logger logger = LogManager.getLogger(PBV4GenericBytes.class.getName());
    ByteArray bar = null;
    short year = 0;
    EPICSEvent.V4GenericBytes dbevent = null;

    public PBV4GenericBytes(short year, ByteArray bar) {
        this.bar = bar;
        this.year = year;
    }

    public PBV4GenericBytes(DBRTimeEvent ev) {
        YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(ev.getEventTimeStamp());
        year = yst.getYear();
        ByteString byteString = ByteString.copyFrom(ev.getSampleValue().getValueAsBytes());
        EPICSEvent.V4GenericBytes.Builder builder = EPICSEvent.V4GenericBytes.newBuilder()
                .setSecondsintoyear(yst.getSecondsintoyear())
                .setNano(yst.getNano())
                .setVal(byteString);
        if (ev.getSeverity() != 0) builder.setSeverity(ev.getSeverity());
        if (ev.getStatus() != 0) builder.setStatus(ev.getStatus());
        if (ev.hasFieldValues()) {
            HashMap<String, String> fields = ev.getFields();
            for (String fieldName : fields.keySet()) {
                EPICSEvent.FieldValue fv = EPICSEvent.FieldValue.newBuilder()
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

    public PBV4GenericBytes(PVAStructure v4Data) throws IOException {
        PVAStructure timeStampPVStructure = v4Data.get("timeStamp");
        YearSecondTimestamp yst = TimeUtils.convertFromPVTimeStamp(timeStampPVStructure);
        int userTag = ((PVAInt) timeStampPVStructure.get("userTag")).get();
        DBRAlarm alarm = DBRAlarm.convertPVAlarm(v4Data.get("alarm"));

        var buffer = ByteBuffer.allocate(4 * 1024 * 1024);
        try {
            v4Data.encodeType(buffer, new BitSet());
            v4Data.encode(buffer);
            buffer.flip();
        } catch (Exception e) {
            logger.error("Error serializing pvAccess Generic Bytes ", e);
        }

        ByteString byteString = ByteString.copyFrom(buffer, buffer.limit());

        year = yst.getYear();
        EPICSEvent.V4GenericBytes.Builder builder = EPICSEvent.V4GenericBytes.newBuilder()
                .setSecondsintoyear(yst.getSecondsintoyear())
                .setNano(yst.getNano())
                .setUserTag(userTag)
                .setVal(byteString);

        if (alarm.severity != 0) builder.setSeverity(alarm.severity);

        if (alarm.status != 0) builder.setStatus(alarm.status);

        dbevent = builder.build();
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
    }

    @Override
    public Event makeClone() {
        return new PBV4GenericBytes(this);
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
        return new ByteBufSampleValue(dbevent.getVal().asReadOnlyByteBuffer());
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
        dbevent = EPICSEvent.V4GenericBytes.newBuilder()
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
            dbevent = EPICSEvent.V4GenericBytes.newBuilder()
                    .mergeFrom(dbevent)
                    .setStatus(status)
                    .build();
        } else {
            dbevent = EPICSEvent.V4GenericBytes.newBuilder().mergeFrom(dbevent).build();
        }
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        return;
    }

    @Override
    public void setSeverity(int severity) {
        unmarshallEventIfNull();
        if (severity != 0) {
            dbevent = EPICSEvent.V4GenericBytes.newBuilder()
                    .mergeFrom(dbevent)
                    .setSeverity(severity)
                    .build();
        } else {
            dbevent = EPICSEvent.V4GenericBytes.newBuilder().mergeFrom(dbevent).build();
        }
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        return;
    }

    private void unmarshallEventIfNull() {
        try {
            if (dbevent == null) {
                dbevent = EPICSEvent.V4GenericBytes.newBuilder()
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
        List<EPICSEvent.FieldValue> fieldValues = dbevent.getFieldvaluesList();
        if (fieldValues != null && !fieldValues.isEmpty()) {
            for (EPICSEvent.FieldValue fieldValue : fieldValues) {
                ret.put(fieldValue.getName(), fieldValue.getVal());
            }
        }
        return ret;
    }

    @Override
    public String getFieldValue(String fieldName) {
        unmarshallEventIfNull();
        List<EPICSEvent.FieldValue> fieldValues = dbevent.getFieldvaluesList();
        if (fieldValues != null && !fieldValues.isEmpty()) {
            for (EPICSEvent.FieldValue fieldValue : fieldValues) {
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
        EPICSEvent.FieldValue fv = EPICSEvent.FieldValue.newBuilder()
                .setName(fieldName)
                .setVal(fieldValue)
                .build();
        dbevent = EPICSEvent.V4GenericBytes.newBuilder()
                .mergeFrom(dbevent)
                .addFieldvalues(fv)
                .build();
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        return;
    }

    @Override
    public void markAsActualChange() {
        unmarshallEventIfNull();
        dbevent = EPICSEvent.V4GenericBytes.newBuilder()
                .mergeFrom(dbevent)
                .setFieldactualchange(true)
                .build();
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        return;
    }

    @Override
    public void setFieldValues(HashMap<String, String> fieldValues, boolean markAsActualChange) {
        unmarshallEventIfNull();
        LinkedList<EPICSEvent.FieldValue> fieldValuesList = new LinkedList<EPICSEvent.FieldValue>();
        for (String fieldName : fieldValues.keySet()) {
            fieldValuesList.add(EPICSEvent.FieldValue.newBuilder()
                    .setName(fieldName)
                    .setVal(fieldValues.get(fieldName))
                    .build());
        }
        dbevent = EPICSEvent.V4GenericBytes.newBuilder()
                .mergeFrom(dbevent)
                .addAllFieldvalues(fieldValuesList)
                .setFieldactualchange(markAsActualChange)
                .build();
        bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
        return;
    }

    @Override
    public ArchDBRTypes getDBRType() {
        return ArchDBRTypes.DBR_V4_GENERIC_BYTES;
    }

    @Override
    public Message getProtobufMessage() {
        unmarshallEventIfNull();
        return dbevent;
    }

    @Override
    public Class<? extends Message> getProtobufMessageClass() {
        return EPICSEvent.V4GenericBytes.class;
    }
}
