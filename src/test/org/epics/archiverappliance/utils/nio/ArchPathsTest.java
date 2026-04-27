package org.epics.archiverappliance.utils.nio;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
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
import java.nio.file.Paths;
import java.time.Instant;

public class ArchPathsTest {
    private static final String zipFolderStr = ConfigServiceForTests.getDefaultPBTestFolder() + "/ArchPathsTest";
    private static final Logger logger = LogManager.getLogger(ArchPathsTest.class.getName());
    private static final Instant startOfLastYear = TimeUtils.getStartOfYear(TimeUtils.getCurrentYear() - 1);
    private static final String chunkKey = "ArchUnitTest/sine:";

    private static Thread getReaderThread(String zipFileName, Thread writerThread) {
        Runnable reader = () -> {
            int exceptionCount = 0;
            boolean checkedAtLeastOnce = false;
            while (writerThread.isAlive()) {
                if (new File(zipFileName).exists()) {
                    logger.info("Checking concurrent access");
                    try {
                        for (int daynum = 1; daynum < 300; daynum++) {
                            String timeComponent = TimeUtils.getPartitionName(
                                    startOfLastYear.plusSeconds(daynum * 24 * 60 * 60),
                                    PartitionGranularity.PARTITION_DAY);
                            try (ArchPaths arch = new ArchPaths()) {
                                checkedAtLeastOnce = true;
                                Path destPath = arch.get(
                                        new PVPath("jar:file:" + zipFolderStr, chunkKey, timeComponent, ".txt"));
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
        File rootFolder = new File(zipFolderStr);
        FileUtils.deleteDirectory(rootFolder);
        assert rootFolder.mkdirs();
        // We create some sample files for testing.
        for (int daynum = 1; daynum < 365; daynum++) {
            String timeComponent = TimeUtils.getPartitionName(
                    startOfLastYear.plusSeconds(daynum * 24 * 60 * 60), PartitionGranularity.PARTITION_DAY);
            Path destPath = Paths.get(rootFolder.getAbsolutePath(), timeComponent + ".txt");
            try (PrintWriter out = new PrintWriter(new FileOutputStream(destPath.toFile()))) {
                for (int i = 0; i < 1000; i++) {
                    out.println("Line " + i);
                }
            }
        }
    }

    @Test
    public void testPack() throws Exception {
        File rootFolder = new File(zipFolderStr);
        try (ArchPaths arch = new ArchPaths()) {
            for (int daynum = 1; daynum < 200; daynum++) {
                String timeComponent = TimeUtils.getPartitionName(
                        startOfLastYear.plusSeconds(daynum * 24 * 60 * 60), PartitionGranularity.PARTITION_DAY);

                Path sourcePath = Paths.get(rootFolder.getAbsolutePath(), timeComponent + ".txt");
                Path destPath =
                        arch.get(new PVPath("jar:file://" + zipFolderStr, chunkKey, timeComponent, ".txt"), true);
                Files.copy(sourcePath, destPath, REPLACE_EXISTING);
            }
        }

        PVPath zipFilePath = new PVPath("jar:file://" + zipFolderStr, chunkKey);
        Assertions.assertTrue(
                new File(zipFilePath.getContainerPath()).exists(),
                "Zip file does not exist " + zipFilePath.getContainerPath());

        try (ArchPaths validateArchPaths = new ArchPaths()) {
            Assertions.assertTrue(
                    Files.exists(validateArchPaths.get(zipFilePath)),
                    "Path does not exist after packing " + zipFilePath.getFullPath());
        }

        // Now we test adding more files.
        try (ArchPaths arch = new ArchPaths()) {
            for (int daynum = 200; daynum < 300; daynum++) {
                String timeComponent = TimeUtils.getPartitionName(
                        startOfLastYear.plusSeconds(daynum * 24 * 60 * 60), PartitionGranularity.PARTITION_DAY);

                Path sourcePath = Paths.get(rootFolder.getAbsolutePath(), timeComponent + ".txt");
                Path destPath =
                        arch.get(new PVPath("jar:file://" + zipFolderStr, chunkKey, timeComponent, ".txt"), true);
                Files.copy(sourcePath, destPath, REPLACE_EXISTING);
            }
        }

        try (ArchPaths arch = new ArchPaths()) {
            for (int daynum = 1; daynum < 300; daynum++) {
                String timeComponent = TimeUtils.getPartitionName(
                        startOfLastYear.plusSeconds(daynum * 24 * 60 * 60), PartitionGranularity.PARTITION_DAY);

                Path destPath = arch.get(new PVPath("jar:file://" + zipFolderStr, chunkKey, timeComponent, ".txt"));
                Assertions.assertTrue(
                        Files.exists(destPath),
                        "Path does not exist after packing " + destPath.toString() + " for daynum " + daynum);
            }
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.cleanDirectory(new File(zipFolderStr));
        FileUtils.deleteDirectory(new File(zipFolderStr));
    }

    @Test
    @Tag("flaky")
    public void testConcurrentAccess() {
        // Test to see if we can access the zip file concurrently.
        // We launch two threads and see if one can add while the other can read

        final String zipFileName = zipFolderStr + "/concurrpack.zip";
        Runnable writer = () -> {
            int exceptionCount = 0;
            File rootFolder = new File(zipFolderStr);
            try {
                for (int daynum = 1; daynum < 200; daynum++) {
                    try (ArchPaths arch = new ArchPaths()) {
                        String timeComponent = TimeUtils.getPartitionName(
                                startOfLastYear.plusSeconds(daynum * 24 * 60 * 60), PartitionGranularity.PARTITION_DAY);

                        Path sourcePath = Paths.get(rootFolder.getAbsolutePath(), timeComponent + ".txt");
                        Path destPath =
                                arch.get(new PVPath("jar:file:" + zipFolderStr, chunkKey, timeComponent, ".txt"), true);
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
