/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB.utils;

import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashSet;

/**
 * @author mshankar
 *	Checks if all timestamps in the first file are present in the second file
 *  Can be used to validate if a merge/splice operation was successful.
 *  This only compares timestamps; not the values or the meta fields.
 */
public class IsSubset {
	private static Logger logger = LogManager.getLogger(MergePBFile.class.getName());

	private static void printHelpMsg() {
		System.out.println();
		System.out.println("Usage: java edu.stanford.slac.archiverappliance.PlainPB.utils.IsSubset <PBFile0> <PBFile1>");
		System.out.println("This utility checks if all the timestamps in the first are present in the second file.");
		System.out.println();
		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		if(args == null || args.length != 2) { 
			printHelpMsg();
			return;
		}
		
		String srcFileName0 = args[0];
		String srcFileName1 = args[1];
		
		Path srcPath0 = Paths.get(srcFileName0);
		if(!Files.exists(srcPath0) || !Files.isRegularFile(srcPath0)) { 
			logger.error("Source path " + srcPath0.toString() + " does not seem to exist or is not a regular file.");
			System.exit(-1);
		}
		Path srcPath1 = Paths.get(srcFileName1);
		if(!Files.exists(srcPath1) || !Files.isRegularFile(srcPath1)) { 
			logger.error("Source path " + srcPath1.toString() + " does not seem to exist or is not a regular file.");
			System.exit(-1);
		}
		
		PBFileInfo fileInfo0 = new PBFileInfo(srcPath0), fileInfo1 = new PBFileInfo(srcPath1);
		if (!fileInfo0.getPVName().equals(fileInfo1.getPVName())) {
			logger.error("The two sources files are not for the same PV");
			System.exit(-1);
		}
		if (fileInfo0.getDataYear() != fileInfo1.getDataYear()) {
			logger.error("The two sources files are not for the same data year");
			System.exit(-1);
		}
		if (!fileInfo0.getType().equals(fileInfo1.getType())) {
			logger.error("The two sources files are of different DBR types");
			System.exit(-1);
		}

        HashSet<Instant> ts0 = getTimes(srcPath0), ts1 = getTimes(srcPath1);
		if(!ts1.containsAll(ts0)) {
			ts0.removeAll(ts1);
            for (Instant ts : ts0) {
				System.out.println("Missing sample at " + TimeUtils.convertToHumanReadableString(ts));
			}
			System.exit(-1);
		}
	}

    private static HashSet<Instant> getTimes(Path path) throws Exception {
        HashSet<Instant> ret = new HashSet<Instant>();
		PBFileInfo info = new PBFileInfo(path);
		try (FileBackedPBEventStream strm = new FileBackedPBEventStream(info.getPVName(), path, info.getType())) {
			for(Event ev : strm) {
				ret.add(((DBRTimeEvent)ev).getEventTimeStamp());
			}
		}
		return ret;
	}
}
