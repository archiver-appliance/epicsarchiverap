/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB.utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.data.DBRTimeEvent;

import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;

/**
 * @author mshankar
 *	Print all the times in a PB file.
 */
public class PrintExtraFields {
	public static void main(String[] args) throws Exception {
		for(String fileName : args) {
			Path path = Paths.get(fileName);
			System.out.println("Printing extra fields for file " + path.toAbsolutePath().toString());
			PBFileInfo info = new PBFileInfo(path);
			try (FileBackedPBEventStream strm = new FileBackedPBEventStream(info.getPVName(), path, info.getType())) {
				for(Event ev : strm) {
					DBRTimeEvent tev = (DBRTimeEvent)ev;
					if(tev.hasFieldValues()) {
						System.out.println(TimeUtils.convertToISO8601String(((DBRTimeEvent)ev).getEventTimeStamp())
								+ "\t" + TimeUtils.convertToHumanReadableString((((DBRTimeEvent)ev).getEventTimeStamp()))
								+ "\t" + ev.getSampleValue().toString());
						HashMap<String, String> extraFields = tev.getFields();
						for(String name : extraFields.keySet()) {
							System.out.println("\t" + name
									+ "\t" + extraFields.get(name));
						}
					}
				}
			}
		}
	}
}
