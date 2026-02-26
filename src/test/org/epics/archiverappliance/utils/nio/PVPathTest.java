package org.epics.archiverappliance.utils.nio;

import org.apache.commons.io.FileUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class PVPathTest {
    private final File testFolder =
            new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "PVPathTest");
    private final String tPath = testFolder.getAbsolutePath();

    @BeforeEach
    public void setUp() throws Exception {
        if (testFolder.exists()) {
            FileUtils.deleteDirectory(testFolder);
        }
        testFolder.mkdirs();
        new File(tPath + File.separator + "ArchUnitTest").mkdirs();

        {
            URI uri = URI.create("jar:file://" + tPath + "/ArchUnitTest/sine.zip");
            try (FileSystem fs = FileSystems.newFileSystem(uri, Map.of("create", "true"))) {
                for (int i = 0; i < 10; i++) {
                    Path nf = fs.getPath(i + ".txt");
                    try (BufferedWriter writer =
                            Files.newBufferedWriter(nf, StandardCharsets.UTF_8, StandardOpenOption.CREATE)) {
                        writer.write("This is a test");
                    }
                }
            }
        }
        {
            URI uri = URI.create("jar:file://" + tPath + "/sine.zip");
            try (FileSystem fs = FileSystems.newFileSystem(uri, Map.of("create", "true"))) {
                for (int i = 0; i < 10; i++) {
                    Path nf = fs.getPath(i + ".txt");
                    try (BufferedWriter writer =
                            Files.newBufferedWriter(nf, StandardCharsets.UTF_8, StandardOpenOption.CREATE)) {
                        writer.write("This is a test");
                    }
                }
            }
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(testFolder);
    }

    @Test
    public void testFullPath() {
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest/sine:", null, null).getFullPath(),
                "/arch/lts/ArchUnitTest/sine:");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest/sine:", "2026_01_01", null).getFullPath(),
                "/arch/lts/ArchUnitTest/sine:2026_01_01");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".parquet").getFullPath(),
                "/arch/lts/ArchUnitTest/sine:2026_01_01.parquet");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest:", null, null).getFullPath(), "/arch/lts/ArchUnitTest:");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest:", "2026_01_01", null).getFullPath(),
                "/arch/lts/ArchUnitTest:2026_01_01");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest:", "2026_01_01", ".parquet").getFullPath(),
                "/arch/lts/ArchUnitTest:2026_01_01.parquet");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getFullPath(),
                "/arch/lts/ArchUnitTest/sine:2026_01_01.pb");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb.tmpabc").getFullPath(),
                "/arch/lts/ArchUnitTest/sine:2026_01_01.pb.tmpabc");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", null, null).getFullPath(),
                "jar:file:///arch/lts/ArchUnitTest/sine.zip");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", null).getFullPath(),
                "jar:file:///arch/lts/ArchUnitTest/sine.zip!/sine:2026_01_01");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getFullPath(),
                "jar:file:///arch/lts/ArchUnitTest/sine.zip!/sine:2026_01_01.pb");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb.tmpabc").getFullPath(),
                "jar:file:///arch/lts/ArchUnitTest/sine.zip!/sine:2026_01_01.pb.tmpabc");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest:", null, null).getFullPath(),
                "jar:file:///arch/lts/ArchUnitTest.zip");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest:", "2026_01_01", null).getFullPath(),
                "jar:file:///arch/lts/ArchUnitTest.zip!/ArchUnitTest:2026_01_01");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest:", "2026_01_01", ".pb").getFullPath(),
                "jar:file:///arch/lts/ArchUnitTest.zip!/ArchUnitTest:2026_01_01.pb");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", null, null).getFullPath(),
                "tar:///arch/lts/ArchUnitTest/sine.tar");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", null).getFullPath(),
                "tar:///arch/lts/ArchUnitTest/sine.tar!/sine:2026_01_01");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getFullPath(),
                "tar:///arch/lts/ArchUnitTest/sine.tar!/sine:2026_01_01.pb");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb.tmpabc").getFullPath(),
                "tar:///arch/lts/ArchUnitTest/sine.tar!/sine:2026_01_01.pb.tmpabc");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest:", null, null).getFullPath(),
                "tar:///arch/lts/ArchUnitTest.tar");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest:", "2026_01_01", null).getFullPath(),
                "tar:///arch/lts/ArchUnitTest.tar!/ArchUnitTest:2026_01_01");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest:", "2026_01_01", ".pb").getFullPath(),
                "tar:///arch/lts/ArchUnitTest.tar!/ArchUnitTest:2026_01_01.pb");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", null, null).getFullPath(),
                "s3:///arch/lts/ArchUnitTest/sine:");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", null).getFullPath(),
                "s3:///arch/lts/ArchUnitTest/sine:2026_01_01");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getFullPath(),
                "s3:///arch/lts/ArchUnitTest/sine:2026_01_01.pb");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb.tmpabc").getFullPath(),
                "s3:///arch/lts/ArchUnitTest/sine:2026_01_01.pb.tmpabc");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest:", null, null).getFullPath(),
                "s3:///arch/lts/ArchUnitTest:");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest:", "2026_01_01", null).getFullPath(),
                "s3:///arch/lts/ArchUnitTest:2026_01_01");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest:", "2026_01_01", ".pb").getFullPath(),
                "s3:///arch/lts/ArchUnitTest:2026_01_01.pb");
    }

    @Test
    public void testGetParentPathForCreation() {
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest/sine:", null, null).getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest/sine:", "2026_01_01", null).getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb.tmpabc").getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest:", null, null).getParentPathForCreation(), "/arch/lts");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest:", "2026_01_01", null).getParentPathForCreation(), "/arch/lts");
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest:", "2026_01_01", ".pb").getParentPathForCreation(), "/arch/lts");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", null, null).getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", null).getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb")
                        .getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb.tmpabc")
                        .getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest:", null, null).getParentPathForCreation(),
                "/arch/lts");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest:", "2026_01_01", null).getParentPathForCreation(),
                "/arch/lts");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest:", "2026_01_01", ".pb").getParentPathForCreation(),
                "/arch/lts");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", null, null).getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", null).getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb.tmpabc")
                        .getParentPathForCreation(),
                "/arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest:", null, null).getParentPathForCreation(), "/arch/lts");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest:", "2026_01_01", null).getParentPathForCreation(),
                "/arch/lts");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest:", "2026_01_01", ".pb").getParentPathForCreation(),
                "/arch/lts");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", null, null).getParentPathForCreation(),
                "s3:///arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", null).getParentPathForCreation(),
                "s3:///arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getParentPathForCreation(),
                "s3:///arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb.tmpabc")
                        .getParentPathForCreation(),
                "s3:///arch/lts/ArchUnitTest");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest:", null, null).getParentPathForCreation(), "s3:///arch/lts");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest:", "2026_01_01", null).getParentPathForCreation(),
                "s3:///arch/lts");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest:", "2026_01_01", ".pb").getParentPathForCreation(),
                "s3:///arch/lts");
    }

    @Test
    public void testFromPath() {
        Assertions.assertEquals(
                PVPath.fromPath("/arch/lts/ArchUnitTest/sine:2026_01_01.pb", "ArchUnitTest/sine:"),
                new PVPath("/arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb"));
        Assertions.assertEquals(
                PVPath.fromPath("/arch/lts/ArchUnitTest/sine:2026_01_01", "ArchUnitTest/sine:"),
                new PVPath("/arch/lts", "ArchUnitTest/sine:", "2026_01_01", null));
        Assertions.assertEquals(
                PVPath.fromPath("/arch/lts/ArchUnitTest/sine:", "ArchUnitTest/sine:"),
                new PVPath("/arch/lts", "ArchUnitTest/sine:", null, null));
        Assertions.assertEquals(
                PVPath.fromPath("jar:file:///arch/lts/ArchUnitTest/sine.zip!/2026_01_01.pb", "ArchUnitTest/sine:"),
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb"));
        Assertions.assertEquals(
                PVPath.fromPath("jar:file:///arch/lts/ArchUnitTest/sine.zip!/2026_01_01", "ArchUnitTest/sine:"),
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", null));
        Assertions.assertEquals(
                PVPath.fromPath("jar:file:///arch/lts/ArchUnitTest/sine.zip", "ArchUnitTest/sine:"),
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", null, null));
        Assertions.assertEquals(
                PVPath.fromPath("jar:file:///arch/lts/ArchUnitTest/sine.zip!/2026_01_01.pb", "ArchUnitTest/sine:"),
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb"));
        Assertions.assertEquals(
                PVPath.fromPath("jar:file:///arch/lts/ArchUnitTest/sine.zip!/2026_01_01", "ArchUnitTest/sine:"),
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", null));
        Assertions.assertEquals(
                PVPath.fromPath("jar:file:///arch/lts/ArchUnitTest/sine.zip", "ArchUnitTest/sine:"),
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", null, null));
        Assertions.assertEquals(
                PVPath.fromPath("s3:///arch/lts/ArchUnitTest/sine.zip!/2026_01_01.pb", "ArchUnitTest/sine:"),
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb"));
        Assertions.assertEquals(
                PVPath.fromPath("s3:///arch/lts/ArchUnitTest/sine.zip!/2026_01_01", "ArchUnitTest/sine:"),
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", null));
        Assertions.assertEquals(
                PVPath.fromPath("s3:///arch/lts/ArchUnitTest/sine.zip", "ArchUnitTest/sine:"),
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", null, null));
        Assertions.assertEquals(
                PVPath.fromPath("s3:///arch/lts/ArchUnitTest/sine.zip!/2026_01_01.pb", "ArchUnitTest/sine:"),
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb"));
        Assertions.assertEquals(
                PVPath.fromPath("s3:///arch/lts/ArchUnitTest/sine.zip!/2026_01_01", "ArchUnitTest/sine:"),
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", null));
        Assertions.assertEquals(
                PVPath.fromPath("s3:///arch/lts/ArchUnitTest/sine.zip", "ArchUnitTest/sine:"),
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", null, null));
    }

    @Test
    public void testWithNewExtension() {
        PVPath original = new PVPath("/arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb");
        PVPath modified = original.withNewExtension(".pb.tmpabc");
        Assertions.assertEquals("/arch/lts/ArchUnitTest/sine:2026_01_01.pb.tmpabc", modified.getFullPath());

        Assertions.assertEquals(
                PVPath.fromPath("jar:file:///arch/lts/ArchUnitTest/sine.zip!/2026_01_01.pb", "ArchUnitTest/sine:")
                        .withNewExtension(".pb.tmp"),
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb.tmp"));
        Assertions.assertEquals(
                PVPath.fromPath("tar:///arch/lts/ArchUnitTest/sine.zip!/2026_01_01.pb", "ArchUnitTest/sine:")
                        .withNewExtension(".pb.tmp"),
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb.tmp"));
        Assertions.assertEquals(
                PVPath.fromPath("s3:///arch/lts/ArchUnitTest/sine.zip!/2026_01_01.pb", "ArchUnitTest/sine:")
                        .withNewExtension(".pb.tmp"),
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb.tmp"));
    }

    @Test
    public void testContainerPath() {
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getContainerPath(),
                "/arch/lts/ArchUnitTest/sine.zip");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".parquet").getContainerPath(),
                "/arch/lts/ArchUnitTest/sine.zip");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", null, ".parquet").getContainerPath(),
                "/arch/lts/ArchUnitTest/sine.zip");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getContainerPath(),
                "/arch/lts/ArchUnitTest/sine.tar");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".parquet").getContainerPath(),
                "/arch/lts/ArchUnitTest/sine.tar");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", null, ".parquet").getContainerPath(),
                "/arch/lts/ArchUnitTest/sine.tar");
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getContainerPath());
    }

    @Test
    public void testContainedPath() {
        Assertions.assertEquals(
                new PVPath("/arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getContainedPath(),
                "/ArchUnitTest/sine:2026_01_01.pb");
        Assertions.assertEquals(
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getContainedPath(),
                "/sine:2026_01_01.pb");
        Assertions.assertEquals(
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getContainedPath(),
                "/sine:2026_01_01.pb");
        Assertions.assertEquals(
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", "2026_01_01", ".pb").getContainedPath(),
                "/ArchUnitTest/sine:2026_01_01.pb");
    }

    @Test
    public void testGlobSearchParams() throws IOException {
        try (ArchPaths paths = new ArchPaths()) {
            Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> paths.getGlobSearchParams(new PVPath("/arch/lts", null, "2026_01_01", ".pb"), ".parquet"));
            Assertions.assertEquals(
                    new ArchPaths.GlobSearchParams(Path.of("/arch/lts/ArchUnitTest"), "sine:*.parquet"),
                    paths.getGlobSearchParams(new PVPath("/arch/lts", "ArchUnitTest/sine:", null, null), ".parquet"));
            Assertions.assertEquals(
                    new ArchPaths.GlobSearchParams(Path.of("/arch/lts"), "sine:*.parquet"),
                    paths.getGlobSearchParams(new PVPath("/arch/lts", "sine:", null, null), ".parquet"));
            {
                ArchPaths.GlobSearchParams sparams = paths.getGlobSearchParams(
                        new PVPath("jar:file://" + tPath, "ArchUnitTest/sine:", null, null), ".parquet");
                Assertions.assertEquals("sine:*.parquet", sparams.globPattern());
                Assertions.assertTrue(
                        sparams.searchFolder()
                                .getFileSystem()
                                .getClass()
                                .getName()
                                .contains("Zip"),
                        "Expected a Zip file system for the jar file");
                Assertions.assertEquals(
                        "jar:file://" + tPath + "/ArchUnitTest/sine.zip!/",
                        sparams.searchFolder().toUri().toString());
                Assertions.assertEquals(
                        "/", sparams.searchFolder().toAbsolutePath().toString());
            }
            {
                ArchPaths.GlobSearchParams sparams =
                        paths.getGlobSearchParams(new PVPath("jar:file://" + tPath, "sine:", null, null), ".parquet");
                Assertions.assertEquals("sine:*.parquet", sparams.globPattern());
                Assertions.assertTrue(
                        sparams.searchFolder()
                                .getFileSystem()
                                .getClass()
                                .getName()
                                .contains("Zip"),
                        "Expected a Zip file system for the jar file");
                Assertions.assertEquals(
                        "jar:file://" + tPath + "/sine.zip!/",
                        sparams.searchFolder().toUri().toString());
                Assertions.assertEquals(
                        "/", sparams.searchFolder().toAbsolutePath().toString());
            }
            // Turn these on once we have tar and s3 plugins in the build.
            // Assertions.assertEquals(
            //         new ArchPaths.GlobSearchParams(Path.of("tar:///arch/lts/ArchUnitTest/sine.tar"), "*.parquet"),
            //         paths.getGlobSearchParams(new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", null, null),
            // ".parquet")
            // );
            // Assertions.assertEquals(
            //         new ArchPaths.GlobSearchParams(Path.of("tar:///arch/lts/sine.tar"), "*.parquet"),
            //         paths.getGlobSearchParams(new PVPath("tar:///arch/lts", "sine:", null, null), ".parquet")
            // );
            // Assertions.assertEquals(
            //         new ArchPaths.GlobSearchParams(Path.of("s3:///arch/lts/ArchUnitTest"), "sine:*.parquet"),
            //         paths.getGlobSearchParams(new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", null, null),
            // ".parquet")
            // );
            // Assertions.assertEquals(
            //         new ArchPaths.GlobSearchParams(Path.of("s3:///arch/lts"), "sine:*.parquet"),
            //         paths.getGlobSearchParams(new PVPath("s3:///arch/lts", "sine:", null, null), ".parquet")
            // );
        }
    }

    @Test
    public void testRootURI() {
        Assertions.assertEquals(
                URI.create("/arch/lts"), new PVPath("/arch/lts", "ArchUnitTest/sine:", null, null).toRootURI());
        Assertions.assertEquals(
                URI.create("jar:file:///arch/lts/ArchUnitTest/sine.zip"),
                new PVPath("jar:file:///arch/lts", "ArchUnitTest/sine:", null, null).toRootURI());
        Assertions.assertEquals(
                URI.create("tar:///arch/lts/ArchUnitTest/sine.tar"),
                new PVPath("tar:///arch/lts", "ArchUnitTest/sine:", null, null).toRootURI());
        Assertions.assertEquals(
                URI.create("tar:///arch/lts/sine.tar"), new PVPath("tar:///arch/lts", "sine:", null, null).toRootURI());
        Assertions.assertEquals(
                URI.create("s3:///arch/lts"),
                new PVPath("s3:///arch/lts", "ArchUnitTest/sine:", null, null).toRootURI());
    }
}
