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

/**
 * A catch all interface that contains the attributes in a EPICS DBR_TIME_XXX from db_access.h.
 * @author mshankar
 */
public interface DBRTimeEvent extends Event, SamplingInfo, AlarmInfo, FieldValues, PartionedTime {

    @Override
    public default YearSecondTimestamp getYearSecondTimestamp() {
        return TimeUtils.convertToYearSecondTimestamp(this.getEventTimeStamp());
    }

}
