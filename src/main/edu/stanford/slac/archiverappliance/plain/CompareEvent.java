/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.data.PartionedTime;
import edu.stanford.slac.archiverappliance.PB.search.CompareEventLine;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * A comparator for PB events that is used in searching.
 * @author mshankar
 *
 */
public class CompareEvent implements CompareEventLine {
    private final YearSecondTimestamp yearSecondTimestamp;
    private final ArchDBRTypes type;

    public CompareEvent(ArchDBRTypes type, YearSecondTimestamp yearSecondTimestamp) {
        this.type = type;
        this.yearSecondTimestamp = yearSecondTimestamp;
    }

    @Override
    public NextStep compare(byte[] line1, byte[] line2) throws IOException {
        // The year does not matter here as we are driving solely off secondsintoyear. So we set it to 0.
        Constructor<? extends DBRTimeEvent> constructor =
                DBR2PBTypeMapping.getPBClassFor(type).getUnmarshallingFromByteArrayConstructor();
        YearSecondTimestamp line1Timestamp;
        YearSecondTimestamp line2Timestamp = new YearSecondTimestamp(
                this.yearSecondTimestamp.getYear(),
                PartitionGranularity.PARTITION_YEAR.getApproxSecondsPerChunk() + 1,
                0);
        try {
            // The raw forms for all the DBR types implement the PartionedTime interface
            PartionedTime e = constructor.newInstance(this.yearSecondTimestamp.getYear(), new ByteArray(line1));
            line1Timestamp = e.getYearSecondTimestamp();
            if (line2 != null) {
                PartionedTime e2 = constructor.newInstance(this.yearSecondTimestamp.getYear(), new ByteArray(line2));
                line2Timestamp = e2.getYearSecondTimestamp();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        if (line1Timestamp.getSecondsintoyear() < 0) {
            throw new IOException("We cannot have a negative seconds into year " + line1Timestamp);
        }
        if (line1Timestamp.compareTo(this.yearSecondTimestamp) > 0) {
            return NextStep.GO_LEFT;
        } else if (line2Timestamp.compareTo(this.yearSecondTimestamp) <= 0) {
            return NextStep.GO_RIGHT;
        } else {
            // If we are here, line1 < SS <= line2
            if (line2 != null) {
                if (line1Timestamp.compareTo(this.yearSecondTimestamp) <= 0
                        && line2Timestamp.compareTo(this.yearSecondTimestamp) > 0) {
                    return NextStep.STAY_WHERE_YOU_ARE;
                } else {
                    return NextStep.GO_LEFT;
                }
            } else {
                return NextStep.STAY_WHERE_YOU_ARE;
            }
        }
    }
}
