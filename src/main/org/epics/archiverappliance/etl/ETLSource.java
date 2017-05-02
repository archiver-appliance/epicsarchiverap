/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import org.epics.archiverappliance.common.PartitionGranularity;

/**
 * An ETL source is data source that can act as a source for ETL.
 * There are the methods that an ETL source needs to implement
 * @author mshankar
 *
 */
public interface ETLSource {
	/**
	 * Given a pv and a time, this method returns all the streams that are ready for ETL.
	 * For example, if the partition granularity of a source is an hour, then this method returns all the streams that are in this source for the previous hours.
	 * Ideally, these streams must be closed for writing and should not change.
	 * The ETL process will consolidates these streams into the ETL destination, which is expected to be at a longer time granularity.   
	 * @param pv The name of PV.
	 * @param currentTime The time that is being used as the cutoff. If we pass in a timestamp way out into the future, we should return all the streams available.
	 * @param context ETLContext
	 * @return List ETLinfo
	 * @throws IOException &emsp;
	 */
	public List<ETLInfo> getETLStreams(String pv, Timestamp currentTime, ETLContext context) throws IOException;
	
	/**
	 * Delete the ETLStream identifier by info when you can as it has already been consumed by the ETL destination.
	 * You can delete it later or immediately. 
	 * @param info ETLInfo
	 * @param context ETLContext
	 */
	public void markForDeletion(ETLInfo info, ETLContext context);
	
	
	public PartitionGranularity getPartitionGranularity();
	
	public String getDescription();
	
	
	/**
	 * Should ETL move data from this source to the destination on shutdown. 
	 * For example, if you are using a ramdisk for the STS and you have a UPS, you can minimize any data loss but turning this bit on for data stores that are on the ramdisk.
	 * On shutdown, ETL will try to move the data out of this store into the next lifetime store.
	 * @return boolean True or False
	 */
	public boolean consolidateOnShutdown();

}
