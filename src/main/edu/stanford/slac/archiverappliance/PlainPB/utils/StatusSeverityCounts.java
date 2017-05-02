/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.data.DBRTimeEvent;

import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;

/**
 * Walks a directory and looks in the PB files and determines the most common status and severity codes....
 * 
 * On LCLS production, we had this for status and severity
 *
 * Statuses:
 * 0:	49,494,483:	89%
 * 18:	2,579,033:	5%
 * 5:	1,614,346:	3%
 * 6:	894,622:	2%
 * 7:	407,167:	1%
 * 14:	335,742:	1%
 * 1:	189,892:	0%
 * 3:	156,028:	0%
 * 4:	106,253:	0%
 * 12:	318:	0%
 * Severities:
 * 3:	51,791,204:	93%
 * 1:	2,221,453:	4%
 * 0:	1,292,648:	2%
 * 2:	472,579:	1%
 * 
 * @author mshankar
 *
 */
public class StatusSeverityCounts {

	/**
	 * @param args  &emsp;
	 * @throws IOException  &emsp;
	 */
	public static void main(String[] args) throws IOException {
		String dirName = args[0];
		File dir = new File(dirName);
		Path dirPath = dir.toPath();
		class Count {
			Count(int val) {
				this.val = val;
			}
			int val;
			long count = 0;
		}
		final HashMap<Integer, Count> statuses = new HashMap<Integer, Count>(); 
		final HashMap<Integer, Count> severities = new HashMap<Integer, Count>(); 
		
		Files.walkFileTree(dirPath, new FileVisitor<Path>() {
			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
				
				PBFileInfo info = new PBFileInfo(path);
				try(FileBackedPBEventStream strm = new FileBackedPBEventStream(info.getPVName(), path, info.getType())) {
					for(Event e : strm) {
						{ 
							int status = ((DBRTimeEvent) e).getStatus();
							Count statcount = statuses.get(status);
							if(statcount == null) { statcount = new Count(status); statuses.put(status, statcount); }
							statcount.count++;
						}
						
						{
							int severity = ((DBRTimeEvent) e).getSeverity();
							Count sevrcount = severities.get(severity);
							if(sevrcount == null) { sevrcount = new Count(severity); severities.put(severity, sevrcount); }
							sevrcount.count++;
						}
					}
				}
				
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				System.err.println("Visiting file " + file.toAbsolutePath() + " failed.");
				return FileVisitResult.CONTINUE;
			}
		});

		{ 
			List<Count> sortedStatuses = new LinkedList<Count>(statuses.values());
			Collections.sort(sortedStatuses, new Comparator<Count>() {
				@Override
				public int compare(Count o1, Count o2) {
					// We want descending order.
					return (int) (o2.count - o1.count);
				}
			});

			long total = 0;
			for(Count c : sortedStatuses) {
				total += c.count;
			}

			System.out.println("Statuses:");
			for(Count c : sortedStatuses) {
				System.out.println(MessageFormat.format("{0, number, integer}:\t{1, number, integer}:\t{2, number, percent}", c.val, c.count, (((float)c.count)/total)));
			}
		}
		
		
		{
			List<Count> sorttedSeverities = new LinkedList<Count>(severities.values());
			Collections.sort(sorttedSeverities, new Comparator<Count>() {
				@Override
				public int compare(Count o1, Count o2) {
					// We want descending order.
					return (int) (o2.count - o1.count);
				}
			});

			long total = 0;
			for(Count c : sorttedSeverities) {
				total += c.count;
			}

			System.out.println("Severities:");
			for(Count c : sorttedSeverities) {
				System.out.println(MessageFormat.format("{0, number, integer}:\t{1, number, integer}:\t{2, number, percent}", c.val, c.count, (((float)c.count)/total)));
			}
		}
		
	}
}
