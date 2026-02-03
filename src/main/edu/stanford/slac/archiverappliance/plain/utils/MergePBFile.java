/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain.utils;

import edu.stanford.slac.archiverappliance.plain.EventFileWriter;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import edu.stanford.slac.archiverappliance.plain.PlainFileHandler;
import edu.stanford.slac.archiverappliance.plain.PlainStorageType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.mergededup.MergeDedupEventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;

/**
 * Merge two Plain files for the same PV and year into the third file.
 *
 * @author mshankar
 */
public class MergePBFile {

    private static Logger logger = LogManager.getLogger(MergePBFile.class.getName());
    private static boolean verboseMode = false;

    /**
     * @param args &emsp;
     * @throws  Exception  &emsp;
     */
    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 3) {
            printHelpMsg();
            return;
        }

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

        if (argsAfterOptions.size() < 3) {
            printHelpMsg();
            return;
        }

        String srcFileName0 = argsAfterOptions.get(0);
        String srcFileName1 = argsAfterOptions.get(1);
        String destFileName = argsAfterOptions.get(2);

        if (srcFileName0.equals(destFileName) || srcFileName1.equals(destFileName)) {
            logger.error("Source and dest files are the same");
            printHelpMsg();
            System.exit(-1);
        }

        Path srcPath0 = Paths.get(srcFileName0);
        if (!Files.exists(srcPath0) || !Files.isRegularFile(srcPath0)) {
            logger.error("Source path " + srcPath0.toString() + " does not seem to exist or is not a regular file.");
            System.exit(-1);
        }
        Path srcPath1 = Paths.get(srcFileName1);
        if (!Files.exists(srcPath1) || !Files.isRegularFile(srcPath1)) {
            logger.error("Source path " + srcPath1.toString() + " does not seem to exist or is not a regular file.");
            System.exit(-1);
        }

        Path destPath = Paths.get(destFileName);
        if (Files.exists(destPath)) {
            logger.error("Dest path " + destPath.toString() + " already seems to exist");
            System.exit(-1);
        }

        PlainFileHandler handler0 = PlainStorageType.getHandler(srcPath0);
        PlainFileHandler handler1 = PlainStorageType.getHandler(srcPath1);
        FileInfo fileInfo0 = handler0.fileInfo(srcPath0), fileInfo1 = handler1.fileInfo(srcPath1);
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

        mergePBFile(srcPath0, srcPath1, destPath, handler0);
    }

    private static void printHelpMsg() {
        System.out.println();
        System.out.println(
                "Usage: run.sh edu.stanford.slac.archiverappliance.PlainPB.utils.MergePBFile <Absolute Path to Source PB File 1> <Absolute Path to Source PB File 2> <Absolute Path to Dest PB File>");
        System.out.println();
        System.out.println(
                "This utility merges the source PV files into the dest PB file eliminating any duplicate samples.");
        System.out.println(
                "For example: run.sh edu.stanford.slac.archiverappliance.PlainPB.utils.MergePBFile /src0/ABC/DEF/XYZ\\:2015.pb /src1/ABC/DEF/XYZ\\:2015.pb /dest/ABC/DEF/XYZ\\:2015.pb");
        System.out.println("\t-h Prints this help");
        System.out.println("\t-v Turns on verbose logging.");
        System.out.println();
        System.out.println();
        System.out.println();
    }

    public static void mergePBFile(Path srcPath0, Path srcPath1, Path destPath, PlainFileHandler handler)
            throws Exception {
        logger.info("Merging " + srcPath0.toString() + " and " + srcPath1.toString() + " into " + destPath);
        try (ArchPaths contexts = new ArchPaths()) {
            FileInfo info0 = handler.fileInfo(srcPath0);
            try (EventStream strm0 = handler.getStream(info0.getPVName(), srcPath0, info0.getType());
                    EventStream strm1 = handler.getStream(info0.getPVName(), srcPath1, info0.getType());
                    MergeDedupEventStream mergestream = new MergeDedupEventStream(strm0, strm1);
                    EventFileWriter writer = handler.createEventFileWriter(
                            info0.getPVName(), destPath, info0.getType(), info0.getDataYear())) {
                writer.writeStreamToFile(mergestream);
            }
        }
    }
}
