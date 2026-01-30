package edu.stanford.slac.archiverappliance.plain.utils;

import edu.stanford.slac.archiverappliance.plain.EventFileWriter;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import edu.stanford.slac.archiverappliance.plain.PlainFileHandler;
import edu.stanford.slac.archiverappliance.plain.PlainStorageType;
import org.epics.archiverappliance.EventStream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

/**
 * Utility class to convert a file from one PlainStorageType to another.
 * This tool is intended for ad-hoc conversions between different storage types (e.g., PB to Parquet).
 */
public class ConvertFile {
    private static final Logger logger = Logger.getLogger(ConvertFile.class.getName());

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: ConvertFile <source_file_path> <new_type> [destination_path]");
            System.err.println("Example: ConvertFile /data/pv.pb PARQUET");
            System.err.println(
                    "Note: If destination_path is not provided, it defaults to the same directory with the new extension.");
            System.exit(1);
        }

        String sourcePathStr = args[0];
        String newTypeStr = args[1];
        Path sourcePath = Paths.get(sourcePathStr);

        if (!Files.exists(sourcePath)) {
            System.err.println("Source file does not exist: " + sourcePathStr);
            System.exit(1);
        }

        PlainStorageType destType;
        try {
            destType = PlainStorageType.valueOf(newTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid storage type: " + newTypeStr);
            System.err.println("Valid types: " + java.util.Arrays.toString(PlainStorageType.values()));
            System.exit(1);
            return;
        }

        Path destPath;
        if (args.length >= 3) {
            destPath = Paths.get(args[2]);
        } else {
            String fileName = sourcePath.getFileName().toString();
            String rawName = fileName.substring(0, fileName.lastIndexOf('.'));
            String newExtension = destType.plainFileHandler().getExtensionString();
            destPath = sourcePath.resolveSibling(rawName + newExtension);
        }

        // Ensure destination path is different from source path if it's the same type (although this tool targets
        // different types)
        if (sourcePath.toAbsolutePath().equals(destPath.toAbsolutePath())) {
            System.err.println(
                    "Source and destination paths are identical. Please specify a different destination path or type.");
            System.exit(1);
        }

        try {
            convert(sourcePath, destPath, destType);
            System.out.println("Successfully converted " + sourcePath + " to " + destPath);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Converts a source file to a destination file with the specified storage type.
     *
     * @param sourcePath Path to the source file.
     * @param destPath   Path to the destination file.
     * @param destType   The target PlainStorageType.
     * @throws IOException If an I/O error occurs.
     */
    public static void convert(Path sourcePath, Path destPath, PlainStorageType destType) throws Exception {
        PlainFileHandler srcHandler = PlainStorageType.getHandler(sourcePath);
        PlainFileHandler destHandler = destType.plainFileHandler();

        // Check if source and destination handlers are of the same type
        if (srcHandler.getClass().equals(destHandler.getClass())) {
            throw new IllegalArgumentException(
                    "Source and destination types are the same. This utility is for converting between different types.");
        }

        FileInfo srcInfo = srcHandler.fileInfo(sourcePath);
        String pvName = srcInfo.getPVName();

        try (EventStream stream = srcHandler.getStream(pvName, sourcePath, srcInfo.getType());
                EventFileWriter writer =
                        destHandler.createEventFileWriter(pvName, destPath, srcInfo.getType(), srcInfo.getDataYear())) {
            writer.writeStreamToFile(stream);
        }
    }
}
