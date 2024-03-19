/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

/**
 * A small enum for defining the granularity of the partition.
 * Used principally by the PB storage plugin.
 * The time intervals here have to fit within each other for certain optimizations to work.
 * That is, 5 minutes completely fits within all the granularities larger than it (15,30,60,day,month,year)
 * and 15 minutes completely fits within all the granularities larger than it and so on...
 * @author mshankar
 *
 */
public enum PartitionGranularity {
    PARTITION_5MIN(5 * 60),
    PARTITION_15MIN(15 * 60),
    PARTITION_30MIN(30 * 60),
    PARTITION_HOUR(60 * 60),
    PARTITION_DAY(24 * 60 * 60),
    PARTITION_MONTH(31 * 24 * 60 * 60),
    PARTITION_YEAR(366 * 24 * 60 * 60);

    final int approxSecondsPerChunk;
    final int approxMinutesPerChunk;

    PartitionGranularity(int approxSecondsPerChunk) {
        this.approxSecondsPerChunk = approxSecondsPerChunk;
        this.approxMinutesPerChunk = this.approxSecondsPerChunk / 60;
    }

    public int getApproxSecondsPerChunk() {
        return approxSecondsPerChunk;
    }

    public PartitionGranularity getNextLargerGranularity() {
        return switch (this) {
            case PARTITION_5MIN -> PARTITION_15MIN;
            case PARTITION_15MIN -> PARTITION_30MIN;
            case PARTITION_30MIN -> PARTITION_HOUR;
            case PARTITION_HOUR -> PARTITION_DAY;
            case PARTITION_DAY -> PARTITION_MONTH;
            case PARTITION_MONTH -> PARTITION_YEAR;
            case PARTITION_YEAR -> null;
        };
    }

    public int getApproxMinutesPerChunk() {
        return approxMinutesPerChunk;
    }
}