/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB.utils;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.utils.nio.ArchPaths;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedList;

/**
 * Make a copy of the specified PB file only including the samples between the specified timestamps.
 *  
 * @author mshankar
 */
public class SlicePBFile {
	private static Logger logger = LogManager.getLogger(SlicePBFile.class.getName());
	private static boolean verboseMode = false;
	
	/**
	 * @param args &emsp;
	 * @throws  Exception  &emsp;
	 */
	public static void main(String[] args) throws Exception {
		if(args == null || args.length < 4) { 
			printHelpMsg();
			return;
		}

		LinkedList<String> argsAfterOptions = new LinkedList<String>();
		for(String arg : args) { 
			if(arg.equals("-v")) { 
				verboseMode = true;
			} else if(arg.equals("-h")) {
				printHelpMsg();
				return;
			} else { 
				argsAfterOptions.add(arg);
			}
		}
		
		if(argsAfterOptions.size() < 4) { 
			printHelpMsg();
			return;
		}
		
		String srcFileName = argsAfterOptions.get(0);
		String startTime = argsAfterOptions.get(1);
		String endTime = argsAfterOptions.get(2);
		String destFileName = argsAfterOptions.get(3);
		
		if(srcFileName.equals(destFileName)) {
			logger.error("Source and dest files are the same");
			printHelpMsg();
			return;
		}
		
		Path srcPath = Paths.get(srcFileName);
		if(!Files.exists(srcPath) || !Files.isRegularFile(srcPath)) { 
			logger.error("Source path " + srcPath.toString() + " does not seem to exist or is not a regular file.");
			return;
		}

        Instant startTs = TimeUtils.convertFromISO8601String(startTime);
        Instant endTs = TimeUtils.convertFromISO8601String(endTime);
        if (!startTs.isBefore(endTs)) {
			logger.error("Start time " + startTime + " is not before " + endTime);
			return;
		}
		
		Path destPath = Paths.get(destFileName);
		if(Files.exists(destPath)) { 
			logger.error("Dest path " + destPath.toString() + " already seems to exist");
			return;
		}
		
		slicePBFile(srcPath, startTs, endTs, destPath);
	}

	private static void printHelpMsg() {
		System.out.println();
		System.out.println("Usage: run.sh edu.stanford.slac.archiverappliance.PlainPB.utils.SlicePBFile <Absolute Path to Source PB File> <ISO 8601 start time> <ISO 8601 end time> <Absolute Path to Dest PB File>");
		System.out.println();
		System.out.println("This utility copies the source PV file into the dest PB file; only the samples >= start time and < end time are copied over.");
		System.out.println("For example: run.sh edu.stanford.slac.archiverappliance.PlainPB.utils.SlicePBFile /arch/lts/ABC/DEF/XYZ\\:2015.pb 2013-01-01T00:00:00.000Z 2013-02-01T00:00:00.000Z Only_January_2013.pb");
		System.out.println("\t-h Prints this help");
		System.out.println("\t-v Turns on verbose logging.");
		System.out.println();
		System.out.println();
		System.out.println();
	}

    public static void slicePBFile(Path srcPath, Instant startTs, Instant endTs, Path destPath) {
		logger.info("Slicing " + srcPath.toString() + " from " + TimeUtils.convertToISO8601String(startTs) + " to " + TimeUtils.convertToISO8601String(endTs) + " into " + destPath);
		try (ArchPaths contexts = new ArchPaths()) {
			try { 
				PBFileInfo info = new PBFileInfo(srcPath);
				try(FileBackedPBEventStream strm = new FileBackedPBEventStream(info.getPVName(), srcPath, info.getType()); 
						OutputStream os = new BufferedOutputStream(Files.newOutputStream(destPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)))  {
					byte[] headerBytes = LineEscaper.escapeNewLines(PayloadInfo.newBuilder()
							.setPvname(info.getPVName())
							.setType(strm.getDescription().getArchDBRType().getPBPayloadType())
							.setYear(info.getDataYear())
							.build().toByteArray());
					os.write(headerBytes);
					os.write(LineEscaper.NEWLINE_CHAR);
					int eventNumber = 0;
					for(Event ev : strm) {
						try {
                            Instant eventTs = ev.getEventTimeStamp();
                            if ((eventTs.equals(startTs) || eventTs.isAfter(startTs)) && eventTs.isBefore(endTs)) {
								ByteArray val = ev.getRawForm();
								os.write(val.data, val.off, val.len);
								os.write(LineEscaper.NEWLINE_CHAR);
							} else { 
								if(verboseMode) { 
									logger.debug("Skipping event at " + TimeUtils.convertToISO8601String(eventTs));
								}
							}
						} catch(Throwable t) { 
							logger.error("Exception processing event " + eventNumber, t);
						}
						eventNumber++;
					}	
				} catch(Throwable t) { 
					logger.error("Exception fixing PB file " + srcPath, t);
				}
			} catch(Exception ex) { 
				logger.error("Exception fixing PB file " + srcPath, ex);
			}
		} catch(Exception ex) { 
			logger.error("Exception fixing PB file " + srcPath, ex);
		}
	}
}
