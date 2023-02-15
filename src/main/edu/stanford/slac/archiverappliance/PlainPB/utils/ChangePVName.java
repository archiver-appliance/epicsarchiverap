/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.utils.nio.ArchPaths;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;

/**
 * Change the name of the PV in the PB file.
 * Note that this is completely different from renaming the PV; this applies only to the specified PB files and is meant to address bugs/typo's in PV names etc
 *  
 * @author mshankar
 *
 */
public class ChangePVName {
	private static Logger logger = Logger.getLogger(ChangePVName.class.getName());
	
	/**
	 * @param args  &emsp;
	 * @throws Exception  &emsp;
	 */
	public static void main(String[] args) throws Exception {
		if(args == null || args.length <= 2) { 
			System.out.println("Usage: run.sh edu.stanford.slac.archiverappliance.PlainPB.utils.ChangePVName <OldPVName> <NewPVName> <PBFiles>");
			return;
		}
		
		String oldPVName = args[0];
		String newPVName = args[1];
		String[] files = Arrays.copyOfRange(args, 2, args.length);
		
		for(String fileName : files) {
			Path path = Paths.get(fileName);
			PBFileInfo info = new PBFileInfo(path);
			if(info.getPVName().equals(newPVName)) {
				System.out.println(fileName + " already is associated with PV " + newPVName);
				continue;
			}
			if(!info.getPVName().equals(oldPVName)) {
				System.out.println(fileName + " is not associated with PV " + oldPVName + ". Skipping this just to be safe");
				continue;
			}
			fixPBFile(path, newPVName);
		}
	}

	
	public static void fixPBFile(Path path, String newPVName) {
		try (ArchPaths contexts = new ArchPaths()) {
			PBFileInfo info = new PBFileInfo(path);			
			String[] pathNames = path.toString().split(File.separator);
			String finalNameComponent = pathNames[pathNames.length-1];
			String tempFileName = finalNameComponent + ".bak";
			Path tempPath = path.resolveSibling(tempFileName);
			if(tempPath.equals(path)) { 
				throw new IOException("When computing the temp file name, the original file name " + path + " and the temp file name " + tempPath + " are the same ");
			}
			try { 
				try(FileBackedPBEventStream strm = new FileBackedPBEventStream(info.getPVName(), path, info.getType()); 
						OutputStream os = new BufferedOutputStream(Files.newOutputStream(tempPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)))  {
					byte[] headerBytes = LineEscaper.escapeNewLines(PayloadInfo.newBuilder()
							.setPvname(newPVName)
							.setType(strm.getDescription().getArchDBRType().getPBPayloadType())
							.setYear(info.getDataYear())
							.build().toByteArray());
					os.write(headerBytes);
					os.write(LineEscaper.NEWLINE_CHAR);
					for(Event ev : strm) {
						ByteArray val = ev.getRawForm();
						os.write(val.data, val.off, val.len);
						os.write(LineEscaper.NEWLINE_CHAR);
					}	
				} catch(Throwable t) { 
					throw new IOException("Exception processing file " + path.toString(), t);
				}
				Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING);
			} catch(Exception ex) { 
				logger.error("Exception processing file " + path, ex);
			}
		} catch(Exception ex) { 
			logger.error("Exception processing file " + path, ex);
		}
	}
}
