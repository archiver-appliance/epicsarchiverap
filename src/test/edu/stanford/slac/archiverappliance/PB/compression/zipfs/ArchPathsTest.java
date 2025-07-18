package edu.stanford.slac.archiverappliance.PB.compression.zipfs;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class ArchPathsTest {
    private final String rootFolderStr = ConfigServiceForTests.getDefaultPBTestFolder() + "/ArchPathsTest";
    private static final Logger logger = LogManager.getLogger(ArchPathsTest.class.getName());

    private static Thread getReaderThread(String zipFileName, Thread writerThread) {

        Runnable reader = () -> {
            int exceptionCount = 0;
            boolean checkedAtLeastOnce = false;
            while (writerThread.isAlive()) {
                if (new File(zipFileName).exists()) {
                    logger.info("Checking concurrent access");
                    try {
                        for (int filenum = 1; filenum < 100; filenum++) {
                            try (ArchPaths paths = new ArchPaths()) {
                                checkedAtLeastOnce = true;
                                Path destPath = paths.get(
                                        "jar:file://" + zipFileName + "!/result/SomeTextFile" + filenum + ".txt");
                                Files.exists(destPath);
                            } catch (Exception ex) {
                                exceptionCount++;
                                logger.error("Exception reading file", ex);
                            }
                        }
                        Thread.sleep(10);
                    } catch (Exception ex) {
                        logger.error(ex);
                    }
                } else {
                    logger.debug("Skipping checking concurrent access");
                }
            }
            Assertions.assertTrue(checkedAtLeastOnce, "We have not check the reader part even once ");
            Assertions.assertEquals(0, exceptionCount, "The read thread had " + exceptionCount + " exceptions");
        };

        Thread readerthread = new Thread(reader);
        readerthread.setName("Reader");
        return readerthread;
    }

    @BeforeEach
    public void setUp() throws Exception {
        File rootFolder = new File(rootFolderStr);
        FileUtils.deleteDirectory(rootFolder);
        assert rootFolder.mkdirs();
        // We create some sample files for testing.
        for (int filenum = 1; filenum < 100; filenum++) {
            try (PrintWriter out =
                    new PrintWriter(new FileOutputStream(rootFolderStr + File.separator + "text" + filenum + ".txt"))) {
                for (int i = 0; i < 1000; i++) {
                    out.println("Line " + i);
                }
            }
        }
    }

    @Test
    public void testPack() throws Exception {
        String zipFileName = rootFolderStr + "/pack.zip";
        String zipFilePath = "jar:file://" + zipFileName + "!/result/SomeTextFile.txt";
        try (ArchPaths arch = new ArchPaths()) {
            Path sourcePath = arch.get(rootFolderStr + "/text1.txt");
            Path destPath = arch.get(zipFilePath, true);
            Files.copy(sourcePath, destPath, REPLACE_EXISTING);
        }

        Assertions.assertTrue(new File(zipFileName).exists(), "Zip file does not exist " + zipFileName);

        try (ArchPaths validateArchPaths = new ArchPaths()) {
            Assertions.assertTrue(
                    Files.exists(validateArchPaths.get(zipFilePath)),
                    "Path does not exist after packing " + zipFilePath);
        }

        // Now we test adding more files.
        try (ArchPaths arch = new ArchPaths()) {
            for (int filenum = 2; filenum < 10; filenum++) {
                Path sourcePath = arch.get(rootFolderStr + "/text" + filenum + ".txt");
                Path destPath = arch.get("jar:file://" + zipFileName + "!/result/SomeTextFile" + filenum + ".txt");
                logger.debug("Packing " + sourcePath.toString() + " into " + destPath.toString());
                Files.copy(sourcePath, destPath, REPLACE_EXISTING);
            }
        }

        try (ArchPaths validateArchPaths = new ArchPaths()) {
            for (int filenum = 2; filenum < 10; filenum++) {
                Assertions.assertTrue(
                        Files.exists(validateArchPaths.get(
                                "jar:file:///" + zipFileName + "!/result/SomeTextFile" + filenum + ".txt")),
                        "Path does not exist after packing " + zipFilePath);
            }
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.cleanDirectory(new File(rootFolderStr));
        FileUtils.deleteDirectory(new File(rootFolderStr));
    }

    @Test
    @Tag("flaky")
    public void testConcurrentAccess() {
        // Test to see if we can access the zip file concurrently.
        // We launch two threads and see if one can add while the other can read

        final String zipFileName = rootFolderStr + "/concurrpack.zip";
        Runnable writer = () -> {
            int exceptionCount = 0;
            try {
                for (int filenum = 1; filenum < 100; filenum++) {
                    try (ArchPaths paths = new ArchPaths()) {
                        Path sourcePath = paths.get(rootFolderStr + "/text" + filenum + ".txt");
                        Path destPath = paths.get(
                                "jar:file://" + zipFileName + "!/result/SomeTextFile" + filenum + ".txt", true);
                        logger.info("Packing " + sourcePath.toString() + " into " + destPath.toString());
                        Files.copy(sourcePath, destPath, REPLACE_EXISTING);
                    } catch (Exception ex) {
                        exceptionCount++;
                        logger.error("Exception writing file", ex);
                    }
                    Thread.sleep(10);
                }
            } catch (Exception ex) {
                logger.error(ex);
            }
            Assertions.assertEquals(0, exceptionCount, "The write thread had " + exceptionCount + " exceptions");
        };

        final Thread writerThread = new Thread(writer);
        writerThread.setName("Writer");
        writerThread.start();

        Thread readerthread = getReaderThread(zipFileName, writerThread);
        readerthread.start();
    }
}
