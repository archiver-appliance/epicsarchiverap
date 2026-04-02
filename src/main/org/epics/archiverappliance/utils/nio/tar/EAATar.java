package org.epics.archiverappliance.utils.nio.tar;

/*
 * Generate a DB with 500 million potential gztar file catalog entries and check performance of query.
 * Result(s):
 * Took an average of 6.31(ms) to get files from the catalog for one PV from a catalog with 1000000 PVs and 366000000 files
 * Took 2506125(ms) to load 2000000 pvs
 * Took an average of 6.15(ms) to get files from the catalog for one PV from a catalog with 2000000 PVs and 732000000 files
 */

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipEncoding;
import org.apache.commons.compress.archivers.zip.ZipEncodingHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class EAATar {
    private static final int TAR_RECORD_SIZE = 512;
    private static final int GZTAR_MAX_RADIX = 36;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 10;
    public static final String LOGICAL_DELETE_PREFIX = "._del_";
    private static final ZipEncoding UTF8_ENC = ZipEncodingHelper.getZipEncoding("UTF-8");
    private static final Logger logger = LogManager.getLogger(EAATar.class.getName());

    private static final Map<String, Integer> commands = Map.of(
            "Catalog", 0,
            "Append", 1,
            "Extract", 2,
            "Delete", 1,
            "Optimize", 0);

    private static String taeToString(TarArchiveEntry te) {
        StringBuilder buf = new StringBuilder();
        buf.append("Name: ")
                .append(te.getName())
                .append(" size ")
                .append(te.getSize())
                .append(" isBlockDevice ")
                .append(te.isBlockDevice())
                .append(" isCharacterDevice ")
                .append(te.isCharacterDevice())
                .append(" isDirectory ")
                .append(te.isDirectory())
                .append(" isExtended ")
                .append(te.isExtended())
                .append(" isFIFO ")
                .append(te.isFIFO())
                .append(" isFile ")
                .append(te.isFile())
                .append(" isGlobalPaxHeader ")
                .append(te.isGlobalPaxHeader())
                .append(" isGNULongLinkEntry ")
                .append(te.isGNULongLinkEntry())
                .append(" isGNULongNameEntry ")
                .append(te.isGNULongNameEntry())
                .append(" isGNUSparse ")
                .append(te.isGNUSparse())
                .append(" isLink ")
                .append(te.isLink())
                .append(" isOldGNUSparse ")
                .append(te.isOldGNUSparse())
                .append(" isPaxGNU1XSparse ")
                .append(te.isPaxGNU1XSparse())
                .append(" isPaxGNUSparse ")
                .append(te.isPaxGNUSparse())
                .append(" isPaxHeader ")
                .append(te.isPaxHeader())
                .append(" isSparse ")
                .append(te.isSparse())
                .append(" isStarSparse ")
                .append(te.isStarSparse())
                .append(" isStreamContiguous ")
                .append(te.isStreamContiguous())
                .append(" isSymbolicLink ")
                .append(te.isSymbolicLink());
        return buf.toString();
    }

    private final String tarFileName;

    public EAATar(String tarFileName) throws IOException {
        this.tarFileName = tarFileName;
    }

    public String getTarFileName() {
        return this.tarFileName;
    }

    private static void readDataFromChannel(FileChannel channel, long dataSize, WritableByteChannel destChannel)
            throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        long endPos = channel.position() + (long) (Math.ceil((dataSize * 1.0) / TAR_RECORD_SIZE) * TAR_RECORD_SIZE);
        long totalBytesWritten = 0;
        while (((channel.read(buffer)) > 0) && totalBytesWritten < dataSize) {
            buffer.flip();
            if (buffer.hasRemaining()) {
                if ((dataSize - totalBytesWritten) < DEFAULT_BUFFER_SIZE) {
                    int remainingBytes =
                            (int) (dataSize - totalBytesWritten); // No danger of data loss from casting long to int
                    if (remainingBytes < buffer.limit()) {
                        buffer.limit(remainingBytes);
                    }
                }
                if (buffer.hasRemaining()) {
                    int bytesWritten = destChannel.write(buffer);
                    totalBytesWritten = totalBytesWritten + bytesWritten;
                }
            }
            buffer.clear();
        }
        channel.position(endPos);
    }

    public void parseCatalog(Predicate<TarEntry> filter) throws IOException {
        if (!Files.exists(Paths.get(this.tarFileName)) || !Files.isRegularFile(Paths.get(this.tarFileName))) {
            logger.debug("Tar file {} does not exist", this.tarFileName);
            return;
        }
        try (RandomAccessFile rf = new RandomAccessFile(this.tarFileName, "r")) {
            FileChannel channel = rf.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(TAR_RECORD_SIZE);
            String longFileName = null;
            int bytesRead = 0;
            long headerOffset = 0;
            while ((bytesRead = channel.read(buffer)) > 0) {
                buffer.flip();
                if (buffer.hasRemaining()) {
                    TarArchiveEntry entry = new TarArchiveEntry(buffer.array(), UTF8_ENC, true);
                    String fileName = entry.getName();
                    if (longFileName != null) {
                        logger.debug("Carrying over long name {} for entry {}", longFileName, fileName);
                        fileName = longFileName;
                        longFileName = null;
                    }
                    long dataSize = entry.getSize();

                    if (entry.isFile()) {
                        if (entry.isGNULongNameEntry()) {
                            if (dataSize > 0) {
                                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                readDataFromChannel(channel, dataSize - 1, Channels.newChannel(bos));
                                longFileName = new String(bos.toByteArray(), Charset.forName("UTF-8"));
                                logger.debug("Long file name for next file block is {}", longFileName);
                            }
                        } else if (fileName != null && fileName.length() > 0) {
                            long dataOffset = channel.position();
                            TarEntry te = new TarEntry(fileName, headerOffset, dataOffset, dataSize);
                            if (!filter.test(te)) {
                                logger.debug("Rejected tar entry {}", te);
                                return;
                            }
                            if (dataSize > 0) {
                                channel.position(channel.position()
                                        + (long) (Math.ceil((dataSize * 1.0) / TAR_RECORD_SIZE) * TAR_RECORD_SIZE));
                            }
                        } else {
                            logger.debug(
                                    "Skipping zero block entry at {} - {}", channel.position(), taeToString(entry));
                        }
                    } else if (entry.isDirectory()) {
                        logger.warn("Skipping directory {}", entry.getName());
                    } else {
                        logger.warn("Skipping entry for {}  and size ", entry.getName(), entry.getSize());
                        logger.warn(taeToString(entry));
                    }
                }
                buffer.clear();
                headerOffset = channel.position();
            }
        }
        return;
    }

    public Map<String, TarEntry> loadCatalog() throws IOException {
        Map<String, TarEntry> ret = new LinkedHashMap<String, TarEntry>();
        this.parseCatalog(new Predicate<TarEntry>() {
            @Override
            public boolean test(TarEntry te) {
                if (te.isDeleted()) {
                    logger.debug("Skipping logically deleted entry {}", te.entryName());
                    return true;
                }
                ret.put(te.entryName(), te);
                logger.debug("Added entry {} to catalog at offset {} size {}", te.entryName(), te.dataoffset());
                return true;
            }
        });

        return ret;
    }

    public boolean hasDeletedEntries() throws IOException {
        final boolean[] hasDeleted = {false};
        this.parseCatalog(new Predicate<TarEntry>() {
            @Override
            public boolean test(TarEntry te) {
                if (te.isDeleted()) {
                    hasDeleted[0] = true;
                    return false;
                }
                return true;
            }
        });
        return hasDeleted[0];
    }

    public TarReadOnlyByteChannel getReadOnlyByteChannel(TarEntry entry) throws IOException {
        return new TarReadOnlyByteChannel(this.tarFileName, entry);
    }

    public void extract(TarEntry entry, WritableByteChannel destChannel) throws IOException {
        logger.debug("Extracting and decompressing {}", entry);
        try (RandomAccessFile rf = new RandomAccessFile(this.tarFileName, "r")) {
            FileChannel channel = rf.getChannel();
            channel.position(entry.dataoffset());
            Path tmpPath = Files.createTempFile("Eaa", ".gz");
            File tmpFile = tmpPath.toFile();
            try (FileOutputStream tmpos = new FileOutputStream(tmpFile)) {
                try (FileChannel tmpChannel = tmpos.getChannel()) {
                    readDataFromChannel(channel, entry.size(), tmpChannel);
                }
            }

            ByteBuffer databuf = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            try (FileInputStream tmpis = new FileInputStream(tmpFile)) {
                try (ReadableByteChannel gzipchannel = Channels.newChannel(tmpis)) {
                    databuf.clear();
                    int bytesRead = 0;
                    while ((bytesRead = gzipchannel.read(databuf)) > 0) {
                        databuf.flip();
                        if (databuf.hasRemaining()) {
                            destChannel.write(databuf);
                        }
                        databuf.clear();
                    }
                }
            }
            Files.delete(tmpPath);
        }
    }

    public void extractFile(TarEntry entry, WritableByteChannel destChannel) throws IOException {
        try (RandomAccessFile rf = new RandomAccessFile(this.tarFileName, "r")) {
            FileChannel channel = rf.getChannel();
            channel.position(entry.dataoffset());
            readDataFromChannel(channel, entry.size(), destChannel);
        }
    }

    public void appendFiles(List<TarEntry> inputs) throws IOException {
        this.appendFiles(inputs, null);
    }

    public void appendFiles(List<TarEntry> inputs, TarEntry lastEntryInTarFile) throws IOException {
        ByteBuffer databuf = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        try (FileOutputStream fos = new FileOutputStream(this.tarFileName, true)) {
            try (FileChannel destChannel = fos.getChannel()) {
                if (lastEntryInTarFile != null) {
                    logger.debug(
                            "Seeking to the header of the last entry in the tar file {}",
                            lastEntryInTarFile.entryName());
                    destChannel.position(lastEntryInTarFile.headeroffset());
                }
                for (TarEntry input : inputs) {
                    File file = input.srcContent();
                    logger.debug("Before compression, file size is {}", file.length());

                    Path tmpPath = Files.createTempFile("Eaa", ".gz");
                    File tmpFile = tmpPath.toFile();
                    try (FileOutputStream tmpos = new FileOutputStream(tmpFile)) {
                        try (WritableByteChannel gzipchannel = Channels.newChannel(tmpos)) {
                            try (FileInputStream fis = new FileInputStream(file)) {
                                try (FileChannel srcChannel = fis.getChannel()) {
                                    databuf.clear();
                                    int bytesRead = 0;
                                    while ((bytesRead = srcChannel.read(databuf)) > 0) {
                                        databuf.flip();
                                        if (databuf.hasRemaining()) {
                                            gzipchannel.write(databuf);
                                        }
                                        databuf.clear();
                                    }
                                }
                            }
                        }
                    }

                    logger.debug("After compression, file size is {}", tmpFile.length());

                    try {
                        byte[] headerbuf = new byte[TAR_RECORD_SIZE];
                        TarArchiveEntry entry = new TarArchiveEntry(tmpFile, input.entryName());
                        entry.clearExtraPaxHeaders();
                        entry.setGroupName(Long.toString(file.length(), GZTAR_MAX_RADIX));
                        entry.writeEntryHeader(headerbuf, UTF8_ENC, true);
                        destChannel.write(ByteBuffer.wrap(headerbuf));
                    } catch (Throwable t) {
                        throw new IOException(t);
                    }
                    try (FileInputStream fis = new FileInputStream(tmpFile)) {
                        try (FileChannel srcChannel = fis.getChannel()) {
                            databuf.clear();
                            int bytesRead = 0;
                            long totalBytesWritten = 0;
                            while ((bytesRead = srcChannel.read(databuf)) > 0) {
                                databuf.flip();
                                if (databuf.hasRemaining()) {
                                    int bytesWritten = destChannel.write(databuf);
                                    totalBytesWritten = totalBytesWritten + bytesWritten;
                                }
                                databuf.clear();
                            }
                            // Write the padding now
                            int paddingBytes = TAR_RECORD_SIZE - (int) (totalBytesWritten % TAR_RECORD_SIZE);
                            byte[] paddingBuf = new byte[paddingBytes];
                            Arrays.fill(paddingBuf, (byte) 0);
                            destChannel.write(ByteBuffer.wrap(paddingBuf));
                        }
                    }

                    Files.delete(tmpPath);
                }
            }
        }
    }

    public void markFileAsDeleted(TarEntry entry) throws IOException {
        logger.debug("Marking {} as deleted", entry.entryName());
        ByteBuffer buffer = ByteBuffer.allocate(TAR_RECORD_SIZE);
        TarArchiveEntry entryFromFile = null;
        try (RandomAccessFile randacess = new RandomAccessFile(this.tarFileName, "rw")) {
            try (FileChannel channel = randacess.getChannel()) {
                channel.position(entry.headeroffset());
                int bytesRead = channel.read(buffer);
                buffer.flip();
                assert (bytesRead == TAR_RECORD_SIZE);
                entryFromFile = new TarArchiveEntry(buffer.array(), UTF8_ENC, true);
                if (!entry.entryName().equals(entryFromFile.getName()) || !(entry.size() == entryFromFile.getSize())) {
                    throw new IOException("Entry and tarEntry do not match File names "
                            + entry.entryName() + "/" + entryFromFile.getName() + " and sizes "
                            + entry.size() + "/"
                            + entryFromFile.getSize()); // If we see this exception we mostly need to reloas the catalog
                }
                logger.debug("Marking {} of size {} as logically deleted", entry.entryName(), entryFromFile.getSize());
                entryFromFile.setName(LOGICAL_DELETE_PREFIX + entry.entryName());
                byte[] headerbuf = new byte[TAR_RECORD_SIZE];
                entryFromFile.writeEntryHeader(headerbuf, UTF8_ENC, true);
                channel.position(entry.headeroffset());
                channel.write(ByteBuffer.wrap(headerbuf));
            }
        }
    }

    public void renameEntry(TarEntry entry, String newEntryName) throws IOException {
        logger.debug("Renaming {} to {}", entry, newEntryName);
        ByteBuffer buffer = ByteBuffer.allocate(TAR_RECORD_SIZE);
        TarArchiveEntry entryFromFile = null;
        try (RandomAccessFile randacess = new RandomAccessFile(this.tarFileName, "rw")) {
            try (FileChannel channel = randacess.getChannel()) {
                channel.position(entry.headeroffset());
                int bytesRead = channel.read(buffer);
                buffer.flip();
                assert (bytesRead == TAR_RECORD_SIZE);
                entryFromFile = new TarArchiveEntry(buffer.array(), UTF8_ENC, true);
                if (!entry.entryName().equals(entryFromFile.getName()) || !(entry.size() == entryFromFile.getSize())) {
                    throw new IOException("Entry and tarEntry do not match File names "
                            + entry.entryName() + "/" + entryFromFile.getName() + " and sizes "
                            + entry.size() + "/"
                            + entryFromFile.getSize()); // If we see this exception we mostly need to reloas the catalog
                }
                entryFromFile.setName(newEntryName);
                byte[] headerbuf = new byte[TAR_RECORD_SIZE];
                entryFromFile.writeEntryHeader(headerbuf, UTF8_ENC, true);
                channel.position(entry.headeroffset());
                channel.write(ByteBuffer.wrap(headerbuf));
            }
        }
    }

    public void optimize() throws IOException {
        final boolean[] allFilesCopied = {true};
        File optTarFile = File.createTempFile("__eaaoptimize", ".tar", new File(this.tarFileName).getParentFile());
        try (FileOutputStream fos = new FileOutputStream(optTarFile)) {
            try (FileChannel destChannel = fos.getChannel()) {
                this.parseCatalog(new Predicate<TarEntry>() {
                    @Override
                    public boolean test(TarEntry te) {
                        if (te.isDeleted()) {
                            logger.debug("Skipping logically deleted entry {}", te);
                            return true;
                        }

                        logger.debug("Copying over entry {}", te);
                        try {
                            byte[] headerbuf = new byte[TAR_RECORD_SIZE];
                            TarArchiveEntry entry = new TarArchiveEntry(te.entryName());
                            entry.clearExtraPaxHeaders();
                            entry.setSize(te.size());
                            entry.writeEntryHeader(headerbuf, UTF8_ENC, true);
                            destChannel.write(ByteBuffer.wrap(headerbuf));
                        } catch (IOException ex) {
                            allFilesCopied[0] = false;
                            logger.error("Exception writing header for {}", te, ex);
                            return false;
                        }

                        // Write the raw content now
                        try (RandomAccessFile rf = new RandomAccessFile(tarFileName, "r")) {
                            try (FileChannel srcChannel = rf.getChannel()) {
                                srcChannel.position(te.dataoffset());
                                readDataFromChannel(srcChannel, te.size(), destChannel);

                                // Write the padding now
                                int paddingBytes = TAR_RECORD_SIZE - (int) (te.size() % TAR_RECORD_SIZE);
                                byte[] paddingBuf = new byte[paddingBytes];
                                Arrays.fill(paddingBuf, (byte) 0);
                                destChannel.write(ByteBuffer.wrap(paddingBuf));
                            }
                        } catch (IOException ex) {
                            allFilesCopied[0] = false;
                            logger.error("Exception writing data for {}", te, ex);
                            return false;
                        }
                        return true;
                    }
                });
            }
        }

        if (!allFilesCopied[0]) {
            logger.error(
                    "Optimization failed - not all files could be copied over. Leaving the original tar file intact at {}",
                    this.tarFileName);
            Files.delete(optTarFile.toPath());
            return;
        }
        Files.move(
                optTarFile.toPath(),
                Paths.get(this.tarFileName),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("EAATar <Command> <Archive> <Delete> <Other args>");
            return;
        }

        String command = args[0];
        String archiveFile = args[1];
        if (!commands.keySet().contains(command)) {
            System.out.println(
                    "Invalid command - " + command + ". Possible command are " + String.join(", ", commands.keySet()));
            return;
        }

        String[] remainingArgs = Arrays.copyOfRange(args, 2, args.length);
        logger.debug("Remaining args {}", String.join(", ", remainingArgs));

        if (command.equals("Catalog")) {
            EAATar tarFile = new EAATar(archiveFile);
            Map<String, TarEntry> entries = tarFile.loadCatalog();
            List<TarEntry> tarEntries = new LinkedList<TarEntry>(entries.values());
            Collections.sort(tarEntries, new Comparator<TarEntry>() {
                @Override
                public int compare(TarEntry o1, TarEntry o2) {
                    return o1.entryName().compareTo(o2.entryName());
                }
            });
            for (TarEntry entry : tarEntries) {
                System.out.println(entry.toString());
            }
        } else if (command.equals("Extract")) {
            if (remainingArgs.length < 2) {
                System.out.println(
                        "Please specify the name of the file in the tar file and the destination filename to extract to");
                return;
            }
            String fileNameInArchive = remainingArgs[0];
            String outputFileName = remainingArgs[1];
            EAATar tarFile = new EAATar(archiveFile);
            Map<String, TarEntry> entries = tarFile.loadCatalog();
            TarEntry entry = entries.get(fileNameInArchive);
            if (entry == null) {
                System.out.println("Could not find tar entry for " + fileNameInArchive);
            } else {
                logger.info("Found tar file entry for " + fileNameInArchive + " " + entry.toString());
                try (FileOutputStream fos = new FileOutputStream(new File(outputFileName))) {
                    try (FileChannel destChannel = fos.getChannel()) {
                        tarFile.extract(entry, destChannel);
                        System.out.println("Done extracting " + fileNameInArchive + " to " + outputFileName);
                    }
                }
            }
        } else if (command.equals("Append")) {
            if (remainingArgs.length < 1) {
                System.out.println("Please specify the absolute paths to the files that needed to be added to the tar");
                return;
            }

            EAATar tarFile = new EAATar(archiveFile);
            List<TarEntry> inputs = Arrays.asList(remainingArgs).stream()
                    .map(x -> new TarEntry(x, new File(x)))
                    .toList();
            tarFile.appendFiles(inputs);
            System.out.println("Done appending " + String.join(", ", remainingArgs) + " to " + archiveFile);
        } else if (command.equals("Delete")) {
            if (remainingArgs.length < 1) {
                System.out.println(
                        "Please specify the name of the file(s) that need to be marked as deleted. Note this is a logical delete; the first tar entry with a matching file name will be prefixed with a ._del_ to indicate that this file has been deleted.");
                return;
            }

            EAATar tarFile = new EAATar(archiveFile);
            Map<String, TarEntry> entries = tarFile.loadCatalog();
            for (String fileNameInArchive : remainingArgs) {
                TarEntry entry = entries.get(fileNameInArchive);
                if (entry == null) {
                    System.out.println("Could not find tar entry for " + fileNameInArchive);
                } else {
                    System.out.println("Found tar file entry for " + fileNameInArchive + " " + entry.toString());
                    tarFile.markFileAsDeleted(entry);
                    System.out.println("Marked " + fileNameInArchive + " as logically deleted in " + archiveFile);
                }
            }
        } else if (command.equals("Optimize")) {
            EAATar tarFile = new EAATar(archiveFile);
            if (!tarFile.hasDeletedEntries()) {
                System.out.println(
                        "The tar file " + archiveFile + " has no logically deleted entries. No need to optimize.");
                return;
            }
            long start = System.currentTimeMillis();
            tarFile.optimize();
            long end = System.currentTimeMillis();
            System.out.println("Done optimizing " + archiveFile + " in " + (end - start) + " (ms)");
        }
    }
}
