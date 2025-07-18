package edu.stanford.slac.archiverappliance.PB.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.Random;

public class ReverseLineByteStreamTest {
    private static Logger logger = LogManager.getLogger(ReverseLineByteStreamTest.class.getName());

    /*
     * Test that we are reading all the bytes in the file.
     * The ReverseLineByteStream consists of multiple layers
     * The bottom most reads the file into a buffer.
     * This only tests that layer
     */
    @Test
    public void testWeAreReadingAllBytes() throws Exception {
        logger.info("testWeAreReadingAllBytes");
        String fileName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ReverseLineByteStreamTest.txt";
        for (int fileSize = 2 * ReverseLineByteStream.MAX_LINE_SIZE;
                fileSize < 4 * ReverseLineByteStream.MAX_LINE_SIZE;
                fileSize += 1) {
            File f = new File(fileName);
            if (f.exists()) {
                f.delete();
            }

            Random rand = new Random();
            try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(f, false))) {
                byte[] buffer = new byte[fileSize];
                rand.nextBytes(buffer); // Don't really care what we put into the buffer
                os.write(buffer);
            }

            logger.debug("Testing with file length {} ", f.length());
            try (ReverseLineByteStream lis = new ReverseLineByteStream(f.toPath())) {
                boolean bRead = lis.testFillBuffer();
                while (bRead) {
                    bRead = lis.testFillBuffer();
                }
                Assertions.assertEquals(f.length(), lis.getTotalBytesRead(), "Mismatch in bytes read and file length");

                f.delete();
            }
            logger.debug("Done testing with file length {} ", f.length());
        }
    }

    /*
     * The ReverseLineByteStream consists of multiple layers
     * The next layer up reads lines from the byte buffer into a line buffer.
     * This only tests the line buffer layer
     */
    @Test
    public void testWeAreReadingAllLines() throws Exception {
        logger.info("testWeAreReadingAllLines");
        String fileName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ReverseLineByteStreamTest.txt";
        for (int numLines = 2; numLines < 100; numLines++) {
            File f = new File(fileName);
            if (f.exists()) {
                f.delete();
            }

            int expectedLines = 10 * 1024 + numLines;

            try (PrintStream fos = new PrintStream(new BufferedOutputStream(new FileOutputStream(f, false)))) {
                for (int i = 0; i <= expectedLines; i++) {
                    fos.print("We print the same line over and over again" + LineEscaper.NEWLINE_CHAR_STR);
                }
            }

            logger.debug("Testing with numlines {} ", numLines);
            try (ReverseLineByteStream lis = new ReverseLineByteStream(f.toPath())) {
                int linesRead = 0;
                boolean bRead = lis.testFillLineBuffer();
                while (bRead) {
                    linesRead++;
                    bRead = lis.testFillLineBuffer();
                }
                // The -1 is because the ReverseLineByteStream constructors do a fillLineBuffer
                Assertions.assertEquals(expectedLines - 1, linesRead, "Mismatch in lines read");

                f.delete();
            }
            logger.debug("Done testing with numlines {} ", numLines);
        }
    }

    /*
     * The ReverseLineByteStream consists of multiple layers
     * This tests the final layer; the one where we actually get the line
     */
    @Test
    public void testTheActualLines() throws Exception {
        logger.info("testTheActualLines");
        String fileName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ReverseLineByteStreamTest.txt";
        for (int numLines = 2; numLines < 100; numLines++) {
            File f = new File(fileName);
            if (f.exists()) {
                f.delete();
            }

            int expectedLines = 10 * 1024 + numLines;
            int maxInt = -1;

            try (PrintStream fos = new PrintStream(new BufferedOutputStream(new FileOutputStream(f, false)))) {
                for (int i = 0; i < expectedLines; i++) {
                    fos.print("This is a line " + i + LineEscaper.NEWLINE_CHAR_STR);
                    maxInt = i;
                }
            }
            int expectedInt = maxInt;
            logger.debug("Testing with numlines {} ", numLines);
            try (ReverseLineByteStream lis = new ReverseLineByteStream(f.toPath())) {
                int linesRead = 0;
                ByteArray bar = new ByteArray(ReverseLineByteStream.MAX_LINE_SIZE);
                boolean bRead = lis.readLine(bar);
                String theline = new String(bar.toBytes());
                while (bRead) {
                    Assertions.assertEquals("This is a line " + expectedInt, theline, "Mismatch");
                    linesRead++;
                    expectedInt--;
                    bRead = lis.readLine(bar);
                    if (bRead) {
                        theline = new String(bar.toBytes());
                    }
                }
                Assertions.assertEquals("This is a line " + 0, theline, "Mismatch at line 0");
                Assertions.assertEquals(expectedLines, linesRead, "Mismatch in lines read");

                f.delete();
            }
            logger.debug("Done testing with numlines {} ", numLines);
        }
    }

    /*
     * Test seek to position...
     */
    @Test
    public void testSeekToPosition() throws Exception {
        logger.debug("testSeekToPosition");
        String fileName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ReverseLineByteStreamTest.txt";
        File f = new File(fileName);
        if (f.exists()) {
            f.delete();
        }

        DecimalFormat fmt = new DecimalFormat("0000");
        try (PrintStream fos = new PrintStream(new BufferedOutputStream(new FileOutputStream(f, false))); ) {
            for (int i = 0; i < 10000; i++) {
                fos.print(fmt.format(i) + LineEscaper.NEWLINE_CHAR_STR);
            }
        }
        // Each number is 4 chars and a new line = 5 chars
        seekandCheck(f, 99, "0018"); // x63
        seekandCheck(f, 100, "0018"); // x64
        seekandCheck(f, 101, "0018"); // x65
        seekandCheck(f, 102, "0019"); // x66
        seekandCheck(f, 103, "0019"); // x67
        seekandCheck(f, 104, "0019"); // x68
        seekandCheck(f, 105, "0019"); // x69
        seekandCheck(f, 106, "0019"); // x70
        seekandCheck(f, 107, "0020"); // x71
        seekandCheck(f, 108, "0020"); // x72

        // The seekToBeforePreviousLine discards a line; so if we seek to the end of the file and read, we should skip
        // one number
        seekandCheck(f, f.length(), "9998");

        // To read from the last line, use the plain constructor
        ByteArray bar = new ByteArray(LineByteStream.MAX_LINE_SIZE);
        try (ReverseLineByteStream lis = new ReverseLineByteStream(f.toPath())) {
            lis.readLine(bar);
            Assertions.assertEquals(
                    "9999", new String(bar.toBytes()), "Mismatch when reading from the end of the file");
        }

        f.delete();
    }

    private static void seekandCheck(File f, long position, String expectedNumberStr) throws IOException {
        logger.info("Seeking to " + position + " and checking for " + expectedNumberStr);
        ByteArray bar = new ByteArray(LineByteStream.MAX_LINE_SIZE);
        try (ReverseLineByteStream lis = new ReverseLineByteStream(f.toPath())) {
            lis.seekToBeforePreviousLine(position);
            lis.readLine(bar);
            Assertions.assertEquals(expectedNumberStr, new String(bar.toBytes()), "Mismatch at position " + position);
        }
    }
}
