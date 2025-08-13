/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.data;

import edu.stanford.slac.archiverappliance.PB.data.PartionedTime;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.json.simple.JSONAware;
import org.json.simple.JSONValue;

import java.util.HashMap;
import java.util.Map;

/**
 * A catch all interface that contains the attributes in a EPICS DBR_TIME_XXX from db_access.h.
 * @author mshankar
 */
public interface DBRTimeEvent extends Event, SamplingInfo, AlarmInfo, FieldValues, PartionedTime, JSONAware {

    @Override
    public default YearSecondTimestamp getYearSecondTimestamp() {
        return TimeUtils.convertToYearSecondTimestamp(this.getEventTimeStamp());
    }

    public static String toString(DBRTimeEvent dbrTimeEvent) {
        return "DBRTimeEvent [getEventTimeStamp()=" + dbrTimeEvent.getEventTimeStamp()
                + ", getValue()=" + dbrTimeEvent.getSampleValue()
                + ", getStatus()=" + dbrTimeEvent.getStatus()
                + ", getSeverity()" + dbrTimeEvent.getSeverity()
                + ", getFieldValues()" + dbrTimeEvent.getFields()
                + "]";
    }

    @Override
    public default String toJSONString() {

        HashMap<String, Object> evnt = new HashMap<>();
        evnt.put("secs", this.getEpochSeconds());
        evnt.put("val", JSONValue.parse(this.getSampleValue().toJSONString()));
        evnt.put("nanos", this.getEventTimeStamp().getNano());
        evnt.put("severity", this.getSeverity());
        evnt.put("status", this.getStatus());
        Map<String, Object> fieldValues = new HashMap<>();
        evnt.put("fields", fieldValues);
        return JSONValue.toJSONString(evnt);
    }
}
