package org.epics.archiverappliance.etl;

public record ETLInfoListStatistics(
        long time4checkSizes, long time4prepareForNewPartition, long time4appendToETLAppendData, long totalSrcBytes) {}
