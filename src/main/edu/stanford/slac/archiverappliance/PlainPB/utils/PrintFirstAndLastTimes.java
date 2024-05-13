/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB.utils;

import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;

/**
 * Print the timestamp of the first and last sample in a PB files/PB files in a directory
 * @author mshankar
 *
 */
public class PrintFirstAndLastTimes {

    public static void printFirstAndLastTimes(Path path) throws IOException {
        PBFileInfo info = new PBFileInfo(path);
        System.out.println(
                "File " + path.getFileName().toString() + "for PV " + info.getPVName() + " has data ranging from "
                        + info.getFirstEvent().getEventTimeStamp()
                        + " to "
                        + info.getLastEvent().getEventTimeStamp());
    }

    /**
     * @param args main function arguments
     * @throws Exception &emsp;
     */
    public static void main(String[] args) throws Exception {

        if (args == null || args.length <= 0) {
            printHelpMsg();
            return;
        }

        boolean verboseMode = false;
        LinkedList<String> argsAfterOptions = new LinkedList<String>();
        for (String arg : args) {
            if (arg.equals("-v")) {
                verboseMode = true;
            } else if (arg.equals("-h")) {
                printHelpMsg();
                return;
            } else {
                argsAfterOptions.add(arg);
            }
        }

        for (String fileName : argsAfterOptions) {
            Path path = Paths.get(fileName);
            if (Files.isDirectory(path)) {
                Files.walkFileTree(
                        path,
                        new FileVisitor<Path>() {
                            private boolean verboseMode = false;

                            FileVisitor<Path> init(boolean verboseMode) {
                                this.verboseMode = verboseMode;
                                return this;
                            }

                            @Override
                            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                                    throws IOException {
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                printFirstAndLastTimes(file);
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                                return FileVisitResult.CONTINUE;
                            }
                        }.init(verboseMode));
            } else {
                printFirstAndLastTimes(path);
            }
        }
    }

    private static void printHelpMsg() {
        System.out.println();
        System.out.println(
                "Usage: java edu.stanford.slac.archiverappliance.PlainPB.utils.PrintFirstAndLastTimes <options> <Any number of file or folder names>");
        System.out.println();
        System.out.println("\t-h Prints this help");
        System.out.println("\t-v Turns on verbose logging.");
        System.out.println();
        System.out.println();
        System.out.println();
    }
}
