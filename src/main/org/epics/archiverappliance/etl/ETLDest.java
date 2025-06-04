/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;

/**
 * An ETL dest is data source that can act as a sink for ETL.
 * In addition to Writer, There are the methods that an ETL dest needs to implement
 * @author mshankar
 *
 */
public interface ETLDest extends Writer {

    /*
     * Same as the one in StoragePlugin
     */
    public String getName();

    /**
     * This informs the destination that we are switching to a new partition and this dest needs to execute its pre-processing for a new partition.
     * For example, in a PlainPBStorage plugin, this will close the previous fileoutputstreams if any, open a new stream to the file backing the new partition writing a header if needed.
     * @param pvName The name of PV.
     * @param ev This is used to determine the partition for the new partition
     * @param archDBRType ArchDBRTypes
     * @param context ETLContext
     * @return boolean True or False
     * @throws IOException  &emsp;
     */
    public boolean prepareForNewPartition(String pvName, Event ev, ArchDBRTypes archDBRType, ETLContext context)
            throws IOException;

    /**
     * This appends an EventStream to the ETL append data for a PV.
     * @param pvName The name of PV.
     * @param stream The EventStream to append to the append data for a PV.
     * @param context ETLContext
     * @return boolean True or False
     * @throws IOException  &emsp;
     */
    public boolean appendToETLAppendData(String pvName, EventStream stream, ETLContext context) throws IOException;

    /**
     * This concatenates the ETL append data for a PV with the PV's destination data.
     * @param pvName The name of PV.
     * @param context ETLContext
     * @return boolean True or False
     * @throws IOException  &emsp;
     */
    public boolean commitETLAppendData(String pvName, ETLContext context) throws IOException;

    /**
     * Run the post processors associated with this plugin if any for this pv.
     * The post processing is done after the commit and outside of the ETL transaction.
     * This process is expected to catch up on previously missed/incomplete computation of cached post processing files.
     * I can think of at least two usecases for this - one where we decide to go back and add a post processor for a pv and one where we change the algorithm for the post processor and want to recompute all the cached files again.
     * @param pvName The name of PV.
     * @param dbrtype ArchDBRTypes
     * @param context ETLContext
     * @return boolean True or False
     * @throws IOException  &emsp;
     */
    public boolean runPostProcessors(String pvName, ArchDBRTypes dbrtype, ETLContext context) throws IOException;

    public PartitionGranularity getPartitionGranularity();

    public String getDescription();
}
