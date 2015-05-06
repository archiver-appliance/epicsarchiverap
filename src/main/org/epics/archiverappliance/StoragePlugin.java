/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance;

import java.io.IOException;

import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.ConversionFunction;

/**
 * The main interface for a storage plugin; this is an object that implements the reader and the writer interfaces.
 * In addition, this has some methods to help in initialization.
 * @author mshankar
 *
 */
public interface StoragePlugin extends Reader, Writer {
	/**
	 * Multiple PVs will probably use the same storage area and we identify the area using the name.
	 * This is principally used in capacity planning/load balancing to identify the storage area for the PV.
	 * We should make sure that storage's with similar lifetimes have the same name in all the appliances.
	 * The name is also used to identify the storage in the storage report.
	 * For example, the PlainPBStoragePlugin takes a name parameter and we should use something like STS as the identity for the short term store in all the appliances.
	 * @return
	 */
	public String getName();

	/**
	 * Get a string description of this plugin; one that can potentially be used in log messages and provide context.
	 * @return
	 */
	public String getDescription();
	
	/**
	 * Each storage plugin is registered to a URI scheme; for example, the PlainStoragePBPlugin uses pb:// as the scheme.
	 * Configuration for a storage plugin typically comes in as a URL like URI.
	 * <ol> 
	 * <li>The config service identifies the storage plugin using the scheme (&quot;pb&quot; maps to PlainStoragePBPlugin)</li>
	 * <li>Creates an instance using the default constructor.</li>
	 * <li>Calls initialize with the complete URL.</li>
	 * </ol>
	 * The storage plugin is expected to use the parameters in the URL to initialize itself.
	 * @param configURL
	 * @param configService
	 * @see org.epics.archiverappliance.config.StoragePluginURLParser
	 */
	public void initialize(String configURL, ConfigService configService) throws IOException;
	
	/**
	 * Change the name of a PV. This happens occasionally in the EPICS world when people change the names of PVs but want to retain the data.
	 * This method is used to change the name of the PV in any of the datasets for PV <code>oldName</code>.
	 * For example, in PB files, the name of the PV is encoded in the file names and is also stored in the header. In this case, we expect the plugin to move the data to new files names and change the PV name in the file header.
	 * To avoid getting into issues about data changing when renaming files, the PV can be assumed to be in a paused state.
	 * @param context
	 * @param oldName
	 * @param newName
	 * @throws IOException
	 */
	public void renamePV(BasicContext context, String oldName, String newName) throws IOException;
	
	
	/**
	 * Sometimes, PVs change types, EGUs etc. 
	 * In these cases, we are left with the problem of what to do with the already archived data.
	 * We can rename the PV to a new but related name - this keeps the existing data as is.
	 * Or, we can attempt to convert to the new type, EGU etc.
	 * This method can be used to convert the existing data using the supplied conversion function.
	 * Conversions should be all or nothing; that is, first convert all the streams into temporary chunks and then do a bulk rename once all the conversions have succeeded.
	 * Note that we'll also be using the same conversion mechanism for imports and other functions that change data.
	 * So, when/if implementing the conversion function, make sure we respect the typical expectations within the archiver - monotonically increasing timestamps and so on.
	 * To avoid getting into issues about data changing when converting, the PV can be assumed to be in a paused state.
	 *  
	 * @param context
	 * @param pvName
	 * @param conversionFuntion
	 */
	public void convert(BasicContext context, String pvName, ConversionFunction conversionFuntion) throws IOException;
	
}
