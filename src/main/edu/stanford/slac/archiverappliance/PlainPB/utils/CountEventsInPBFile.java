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

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;

import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;

/**
 * Utility for counting the number of events in a PV file.
 * @author mshankar
 *
 */
public class CountEventsInPBFile {
	private static final Logger logger = Logger.getLogger(CountEventsInPBFile.class);
	/**
	 * @param args &emsp;
	 * @throws Exception  &emsp;
	 */
	public static void main(String[] args) throws Exception {
		for(String fileName : args) {
			Path path = Paths.get(fileName);
			PBFileInfo info = new PBFileInfo(path);
			int i = 0;
			long start = System.currentTimeMillis();
			long previousEpochSeconds = 0L;
			Event firstEvent = null;
			Event lastEvent = null;
			try (FileBackedPBEventStream strm = new FileBackedPBEventStream(info.getPVName(), path, info.getType()))  {
				for(Event e : strm) { 
					long currEpochSeconds = e.getEpochSeconds();
					if(currEpochSeconds >= previousEpochSeconds) {
						previousEpochSeconds = currEpochSeconds;
						if(firstEvent == null) firstEvent = e;
						lastEvent = e;
					} else {
						throw new Exception("Current epoch seconds " + TimeUtils.convertToISO8601String(currEpochSeconds) 
								+ " is less than previous epoch seconds " + TimeUtils.convertToISO8601String(previousEpochSeconds)
								+ " at about line " + i
								);
					}
					i++;
				}
				long end = System.currentTimeMillis();
				System.out.println("There are " + i + " events " 
						+ "starting from " + TimeUtils.convertToISO8601String(firstEvent.getEpochSeconds()) 
						+ " to " + TimeUtils.convertToISO8601String(lastEvent.getEpochSeconds()) 
						+ " in " + fileName 
						+ " for PV " + info.getPVName()
						+ " which is of type " + info.getType()
						+ " with data for the year " + info.getDataYear()
						+ " - determined this in " + (end - start) + "(ms)");
				strm.close();
			} catch(Exception ex) {
				System.out.println("Exception at about line " + i 
						+ " when processing file " + path.toAbsolutePath().toString()
						+ " containing data from " + info.getPVName() 
						+ " for year " + info.getDataYear() 
						+ " of type " + info.getType());
				logger.error(ex.getMessage(), ex);
			}
		}
	}
}
