/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.imprt;

import edu.stanford.slac.archiverappliance.plain.PlainPBStoragePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;

/**
 * Simple import of a CSV file into storage plugin.
 * CSV file format is the one used by Bob Hall for export from ChannelArchiver - EPICS epochseconds, nanos, value, status, severity.
 * Example: - 644223600,461147000,5.59054,0,0 
 * @author mshankar
 *
 */
public class ImportCSV {
	private static final Logger logger = LogManager.getLogger(ImportCSV.class);
	/**
	 * @param args  &emsp; 
	 * @throws IOException  &emsp; 
	 */
	public static void main(String[] args) throws IOException {
		if(args.length < 4) {
			// For now we only support the PB plugin.
			System.err.println("Usage: java org.epics.archiverappliance.utils.imprt.ImportCSV <CSVFileName> <PVName> <DBRType> <PBRootFolder>");
			return;
		}

		String fileName = args[0];
		String pvName = args[1];
		ArchDBRTypes type = ArchDBRTypes.valueOf(args[2]);
		if(type == null) {
			System.err.println("Unable to determine the DBR type. Supported types are as follows ");
			for(ArchDBRTypes supptype : ArchDBRTypes.values()) {
				System.err.println(supptype.ordinal() + "\t:" + supptype.toString());
			}
		}
		String rootFolder = args[3];

		PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
		pbplugin.setRootFolder(rootFolder);
		                         
		CSVEventStream strm = null;
		try(BasicContext context = new BasicContext()) {
			strm = new CSVEventStream(pvName, fileName, type);
            int eventsAppended = pbplugin.appendData(context, pvName, strm);
            if (eventsAppended == 0) {
				throw new IOException("Please check the logs to make sure the import succeeded.");
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			try { if(strm != null) { strm.close(); strm = null; } } catch(Exception ex) {} 
		}
		
	}

}
