/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain.utils;

import edu.stanford.slac.archiverappliance.plain.FileInfo;
import edu.stanford.slac.archiverappliance.plain.PlainFileHandler;
import edu.stanford.slac.archiverappliance.plain.pb.PBPlainFileHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.LinkedList;

/**
 * Validate every line in a PB file by unmarshalling it and accessing the timestamp.
 * If there are any exceptions, then copy only the events that can be unmarshalled correctly and are monotonically sequenced correctly to a temporary file and then replace the original file with the temporary file.
 * If the -b option is specified; then the original file is moved to a new file with a <code>.bak.</code> extension.
 *
 * @author mshankar
 */
public class ValidateAndFixPBFile {

    private static final Logger logger = LogManager.getLogger(ValidateAndFixPBFile.class.getName());

    /**
     * @param args &emsp;
     * @throws Exception &emsp;
     */
    public static void main(String[] args) throws Exception {
        if (args == null || args.length <= 0) {
            printHelpMsg();
            return;
        }

        boolean verboseMode = false;
        boolean makeBackups = false;
        LinkedList<String> argsAfterOptions = new LinkedList<String>();
        for (String arg : args) {
            if (arg.equals("-v")) {
                verboseMode = true;
            } else if (arg.equals("-b")) {
                makeBackups = true;
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
                            private boolean makeBackups = false;

                            FileVisitor<Path> init(boolean verboseMode, boolean makeBackups) {
                                this.verboseMode = verboseMode;
                                this.makeBackups = makeBackups;
                                return this;
                            }

                            @Override
                            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                                    throws IOException {
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                PlainFileHandler handler = getHandler(file);
                                boolean isValid = ValidatePlainFile.validatePlainFile(file, verboseMode, handler);
                                if (!isValid) {
                                    logger.debug("Path " + file + " is not a valid PB file");
                                    fixPBFile(file, verboseMode, makeBackups, handler);
                                }
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
                        }.init(verboseMode, makeBackups));
            } else {
                PlainFileHandler handler = getHandler(path);
                boolean isValid = ValidatePlainFile.validatePlainFile(path, verboseMode, handler);
                if (!isValid) {
                    logger.debug("Path " + path + " is not a valid PB file");
                    fixPBFile(path, verboseMode, makeBackups, handler);
                }
            }
        }
    }

    private static PlainFileHandler getHandler(Path path) {
        String filename = path.getFileName().toString();
        for (edu.stanford.slac.archiverappliance.plain.PlainStorageType type :
                edu.stanford.slac.archiverappliance.plain.PlainStorageType.values()) {
            PlainFileHandler handler = type.plainFileHandler();
            if (filename.endsWith(handler.getExtensionString())) {
                return handler;
            }
        }
        return new PBPlainFileHandler();
    }

    private static void printHelpMsg() {
        System.out.println();
        System.out.println("Usage: validateAndFix.sh <options> <Any number of file or folder names>");
        System.out.println();
        System.out.println("\t-h Prints this help");
        System.out.println("\t-v Turns on verbose logging.");
        System.out.println("\t-b If a file is fixed, keep the original as a .bak file.");
        System.out.println();
        System.out.println();
        System.out.println();
    }

    public static void fixPBFile(Path path, boolean verboseMode, boolean makeBackups, PlainFileHandler handler) {
        System.out.println("Fixing " + path + " which is an invalid PB file " + (makeBackups ? " with backups " : ""));
        long skippedEvents = 0;
        try (ArchPaths contexts = new ArchPaths()) {
            String[] pathNames = path.toString().split(File.separator);
            String finalNameComponent = pathNames[pathNames.length - 1];
            String tempFileName = finalNameComponent + ".bak";
            Path tempPath = path.resolveSibling(tempFileName);
            if (tempPath.equals(path)) {
                throw new IOException("When computing the temp file name, the original file name " + path
                        + " and the temp file name "
                        + tempPath
                        + " are the same ");
            }
            if (verboseMode)
                logger.info("Temporary file is " + tempPath + " with final component of temp file " + tempFileName);
            try {
                FileInfo info = handler.fileInfo(path);
                long previousEpochSeconds = Long.MIN_VALUE;
                long eventnum = 0;
                try (EventStream strm = handler.getStream(info.getPVName(), path, info.getType());
                        edu.stanford.slac.archiverappliance.plain.EventFileWriter writer =
                                handler.createEventFileWriter(
                                        info.getPVName(), tempPath, info.getType(), info.getDataYear())) {
                    for (Event ev : strm) {
                        try {
                            long epochSeconds = ev.getEpochSeconds();
                            if (epochSeconds >= previousEpochSeconds) {
                                previousEpochSeconds = epochSeconds;
                                writer.append(ev);
                            } else {
                                if (verboseMode)
                                    logger.debug("Skipping non sequential event " + eventnum + " in file " + path);
                                skippedEvents++;
                            }
                        } catch (Throwable t) {
                            if (verboseMode) logger.debug("Skipping event " + eventnum + " in file " + path);
                            skippedEvents++;
                        }
                        eventnum++;
                    }
                } catch (Throwable t) {
                    logger.error("Exception fixing PB file " + path, t);
                }
                if (makeBackups) {
                    Path tPath = Files.createTempFile(
                            path.getParent(),
                            "Temp",
                            "tempbak",
                            PosixFilePermissions.asFileAttribute(
                                    Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS)));
                    Files.move(path, tPath, StandardCopyOption.REPLACE_EXISTING);
                    Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING);
                    Files.move(tPath, tempPath, StandardCopyOption.REPLACE_EXISTING);
                } else {
                    Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (Exception ex) {
                logger.error("Exception fixing PB file " + path, ex);
            }
        } catch (Exception ex) {
            logger.error("Exception fixing PB file " + path, ex);
        }
        if (verboseMode) logger.info("Skipped events " + skippedEvents + " when fixing " + path.toString());
    }
}
