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
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.data.DBRTimeEvent;

import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;

/**
 * @author mshankar
 *	Print the PBFileInfo for a file of folder.
 *  If a folder is specified, we walk the folder and print the PBFileInfo for every .pb file we encounter
 */
public class PrintFileInfo {
	private static Logger logger = LogManager.getLogger(PrintFileInfo.class.getName());

	private static void printFileInfo(Path path) {
		try {
			PBFileInfo info = new PBFileInfo(path);
			System.out.println(info.getPVName() + "\t" + info.getDataYear() + "\t" + info.getType() + "\t" + path.toAbsolutePath().toString());
		} catch(Exception ex) {
			logger.error("Exception reading info from file " + path.toAbsolutePath().toString(), ex);
		}
	}

	public static void main(String[] args) throws Exception {
		if(args == null || args.length < 1) { 
			System.err.println("Usage: java edu.stanford.slac.archiverappliance.PlainPB.utils.PrintTimes <PBFiles or folder>");
			return;
		}
		
		for(String fileName : args) {
			Path path = Paths.get(fileName);
			if(Files.isDirectory(path)) {
				Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult visitFile(Path cpath, BasicFileAttributes attr) {
						if(attr.isRegularFile()) {
							printFileInfo(cpath);
						}
						return FileVisitResult.CONTINUE;
					}
				});
			} else {
				printFileInfo(path);
			}
		}
	}
}
