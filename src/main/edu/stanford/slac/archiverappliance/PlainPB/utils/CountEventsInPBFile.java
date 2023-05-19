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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
	private static final Logger logger = LogManager.getLogger(CountEventsInPBFile.class);
	/**
	 * @param args &emsp;
	 * @throws Exception  &emsp;
	 */
	public static void main(String[] args) throws Exception {
	    int totalInvalid = 0;
		for(String fileName : args) {
			Path path = Paths.get(fileName);
			PBFileInfo info = new PBFileInfo(path);
			long start = System.currentTimeMillis();
			long previousEpochSeconds = 0L;
			Event firstEvent = null;
			Event lastEvent = null;
			int lineNumber = 0;
			int validCount = 0;
			int invalidCount = 0;
			try (FileBackedPBEventStream strm = new FileBackedPBEventStream(info.getPVName(), path, info.getType()))  {
				for(Event e : strm) {
					lineNumber += 1;
					try {
						long currEpochSeconds = e.getEpochSeconds();
						if (currEpochSeconds >= previousEpochSeconds) {
							previousEpochSeconds = currEpochSeconds;
							if (firstEvent == null) firstEvent = e;
							lastEvent = e;
						} else {
							throw new Exception("Current epoch seconds " + TimeUtils.convertToISO8601String(currEpochSeconds)
									+ " is less than previous epoch seconds " + TimeUtils.convertToISO8601String(previousEpochSeconds)
									+ " at about line " + lineNumber
							);
						}
						validCount++;
					} catch (Exception ex) {
					    System.out.println("Event at line " + lineNumber + " is invalid");
						invalidCount++;
					}
				}
				long end = System.currentTimeMillis();
				if (firstEvent != null) {
					System.out.println("There are " + validCount + " events "
							+ "starting from " + TimeUtils.convertToISO8601String(firstEvent.getEpochSeconds())
							+ " to " + TimeUtils.convertToISO8601String(lastEvent.getEpochSeconds())
							+ " in " + fileName
							+ " for PV " + info.getPVName()
							+ " which is of type " + info.getType()
							+ " with data for the year " + info.getDataYear()
							+ " - determined this in " + (end - start) + "(ms)");
					System.out.println("There are " + invalidCount + " invalid events");
				} else {
					System.out.println("There is a valid header but no events for PV " + info.getPVName()
							+ " in " +fileName
							+ " which is of type " + info.getType()
							+ " with data for the year " + info.getDataYear()
							+ " - determined this in " + (end - start) + "(ms)");
				}
				totalInvalid += invalidCount;
			} catch(Exception ex) {
				System.out.println("Exception at about line " + lineNumber
						+ " when processing file " + path.toAbsolutePath().toString()
						+ " containing data from " + info.getPVName() 
						+ " for year " + info.getDataYear() 
						+ " of type " + info.getType());
				logger.error(ex.getMessage(), ex);
			}
		}
		System.exit(totalInvalid);
	}
}
