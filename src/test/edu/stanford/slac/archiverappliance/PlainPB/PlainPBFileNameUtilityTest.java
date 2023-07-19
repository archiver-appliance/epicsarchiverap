/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Test the mapping of PVs to file/key names
 * @author mshankar
 *
 */
public class PlainPBFileNameUtilityTest {
    private static final Logger logger = LogManager.getLogger(PlainPBFileNameUtilityTest.class);
    String fileName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "PlainPBFileNameUtility/";
    String rootFolderStr = fileName;
    private ConfigService configService;

    public static String getFormatString(PartitionGranularity partitionGranularity) {
        return switch (partitionGranularity) {
            case PARTITION_5MIN, PARTITION_30MIN, PARTITION_15MIN -> "mm";
            case PARTITION_HOUR -> "HH";
            case PARTITION_DAY -> "dd";
            case PARTITION_MONTH -> "MM";
            case PARTITION_YEAR -> "yyyy";
        };
    }

    static Stream<PartitionGranularity> providePartitionFileExtension() {
        return Arrays.stream(PartitionGranularity.values()).filter(g -> g.getNextLargerGranularity() != null);
    }

    private static void mkPath(Path nf) throws IOException {
        if (!Files.exists(nf)) {
            Files.createDirectories(nf.getParent());
            Files.createFile(nf);
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(new File("./bin"));
        File rootFolder = new File(rootFolderStr);
        if (rootFolder.exists()) {
            FileUtils.deleteDirectory(rootFolder);
        }

        assert rootFolder.mkdirs();
    }

    @AfterEach
    public void tearDown() throws Exception {
        File rootFolder = new File(rootFolderStr);
        FileUtils.deleteDirectory(rootFolder);
    }

    @ParameterizedTest
    @MethodSource("providePartitionFileExtension")
    public void testGetFilesWithData(PartitionGranularity granularity) throws Exception {
        // Lets create some files that cater to this partition.
        Instant startOfYear = TimeUtils.getStartOfYear(TimeUtils.getCurrentYear());
        String pvName = granularity.name() + "Part_1";
        String extension = PlainPBStoragePlugin.pbFileExtension;
        long nIntervals = 1 + granularity.getNextLargerGranularity().getApproxSecondsPerChunk()
                / granularity.getApproxSecondsPerChunk();
        Instant fileTime = null;
        for (long nGranularity = 0;
             nGranularity
                     < nIntervals;
             nGranularity++) {
            fileTime = startOfYear.plusSeconds(nGranularity * granularity.getApproxSecondsPerChunk());
            mkPath(PlainPBPathNameUtility.getPathNameForTime(
                    rootFolderStr,
                    pvName,
                    fileTime,
                    granularity,
                    new ArchPaths(),
                    CompressionMode.NONE,
                    configService.getPVNameToKeyConverter()));
        }

        Path[] matchingPaths = PlainPBPathNameUtility.getPathsWithData(
                new ArchPaths(),
                rootFolderStr,
                pvName,
                startOfYear,
                startOfYear.plusSeconds(nIntervals * granularity.getApproxSecondsPerChunk() - 1),
                extension,
                granularity,
                CompressionMode.NONE,
                configService.getPVNameToKeyConverter());
        Assertions.assertEquals(nIntervals, matchingPaths.length, "File count " + matchingPaths.length);

        Path[] etlPaths = PlainPBPathNameUtility.getPathsBeforeCurrentPartition(
                new ArchPaths(),
                rootFolderStr,
                pvName,
                startOfYear.plusSeconds(nIntervals * granularity.getApproxSecondsPerChunk()),
                extension,
                granularity,
                CompressionMode.NONE,
                configService.getPVNameToKeyConverter());
        Assertions.assertEquals(nIntervals, etlPaths.length, "File count " + etlPaths.length);

        File mostRecentFile = PlainPBPathNameUtility.getMostRecentPathBeforeTime(
                        new ArchPaths(),
                        rootFolderStr,
                        pvName,
                        startOfYear.plusSeconds(nIntervals * granularity.getApproxSecondsPerChunk()),
                        extension,
                        granularity,
                        CompressionMode.NONE,
                        configService.getPVNameToKeyConverter())
                .toFile();
        Assertions.assertNotNull(mostRecentFile, "Most recent file is null?");
        assert fileTime != null;
        String fileEnding = fileTime.atZone(ZoneId.of("Z")).format(DateTimeFormatter.ofPattern(getFormatString(granularity)));
        Assertions.assertTrue(
                mostRecentFile.getName().endsWith(fileEnding + extension),
                "Unxpected most recent file " + mostRecentFile.getAbsolutePath() + " expected ending " + fileEnding);
    }

    @Test
    public void testGetFilesWithDataOnAYearlyPartition() throws Exception {
        // Lets create some files that cater to this partition.
        ZonedDateTime startOfYear =
                TimeUtils.getStartOfYear(TimeUtils.getCurrentYear()).atZone(ZoneId.from(ZoneOffset.UTC));
        logger.info("Current time is " + startOfYear);
        ZonedDateTime curr = startOfYear;
        String pvName = "First:Second:Third:YearPart_1";
        PartitionGranularity partition = PartitionGranularity.PARTITION_YEAR;
        String extension = PlainPBStoragePlugin.pbFileExtension;
        ZonedDateTime endYear =
                null;
        for (int years = 0; years < 20; years++) {
            mkPath(PlainPBPathNameUtility.getPathNameForTime(
                    rootFolderStr,
                    pvName,
                    curr.toInstant(),
                    partition,
                    new ArchPaths(),
                    CompressionMode.NONE,
                    configService.getPVNameToKeyConverter()));
            curr = curr.plusYears(1);
            if (years == 7) endYear = curr;
        }

        Path[] matchingPaths = PlainPBPathNameUtility.getPathsWithData(
                new ArchPaths(),
                rootFolderStr,
                pvName,
                startOfYear.toInstant(),
                endYear.minusSeconds(1).toInstant(),
                extension,
                partition,
                CompressionMode.NONE,
                configService.getPVNameToKeyConverter());
        Assertions.assertEquals(8, matchingPaths.length, "File count " + matchingPaths.length);

        Path[] etlPaths = PlainPBPathNameUtility.getPathsBeforeCurrentPartition(
                new ArchPaths(),
                rootFolderStr,
                pvName,
                endYear.toInstant(),
                extension,
                partition,
                CompressionMode.NONE,
                configService.getPVNameToKeyConverter());
        Assertions.assertEquals(8, etlPaths.length, "File count " + etlPaths.length);

        // Ask for the next year here; the last file written out is for current year plus (20 - 1)
        File mostRecentFile = PlainPBPathNameUtility.getMostRecentPathBeforeTime(
                        new ArchPaths(),
                        rootFolderStr,
                        pvName,
                        curr.plusYears(1).toInstant(),
                        extension,
                        partition,
                        CompressionMode.NONE,
                        configService.getPVNameToKeyConverter())
                .toFile();
        Assertions.assertNotNull(mostRecentFile, "Most recent file is null?");
        Assertions.assertTrue(
                mostRecentFile.getName().endsWith(curr.minusYears(1).getYear() + extension),
                "Unxpected most recent file " + mostRecentFile.getAbsolutePath());

        File mostRecentFile2 = PlainPBPathNameUtility.getMostRecentPathBeforeTime(
                        new ArchPaths(),
                        rootFolderStr,
                        pvName,
                        endYear.toInstant(),
                        extension,
                        partition,
                        CompressionMode.NONE,
                        configService.getPVNameToKeyConverter())
                .toFile();
        Assertions.assertNotNull(mostRecentFile2, "Most recent file is null?");
        String expectedEnd2 = endYear.minusYears(1).getYear() + extension;
        Assertions.assertTrue(
                mostRecentFile2.getName().endsWith(expectedEnd2),
                "Unxpected most recent file " + mostRecentFile2.getAbsolutePath() + " expecting " + expectedEnd2);
    }

}
