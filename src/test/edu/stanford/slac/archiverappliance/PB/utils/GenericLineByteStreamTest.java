package edu.stanford.slac.archiverappliance.PB.utils;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.text.DecimalFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GenericLineByteStreamTest {
    private static Logger logger = LogManager.getLogger(GenericLineByteStreamTest.class.getName());
    private File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder()
            + File.separator
            + GenericLineByteStreamTest.class.getName());
    private String fileName = testFolder.getAbsolutePath() + "/" + "LineByteStream.txt";

    @Before
    public void setUp() throws Exception {
        Files.createDirectories(testFolder.toPath());
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(testFolder);
    }

    private static class LisRet {
        byte[] data;
        int offset;
        int length;

        public LisRet(byte[] data, int offset, int length) {
            this.data = data;
            this.offset = offset;
            this.length = length;
        }
    }

    private static class Lis implements AutoCloseable {
        private static final int DEFAULT_MAX_LINE_LENGTH = 10 * 1024 * 1024;

        FileInputStream fis = null;
        private byte[] dataBytes = null;
        private byte[] returnBytes = null;
        private int bytesRead;
        private int currentPositionInDataBytes = 0;

        public Lis(File f) throws IOException {
            this(f, DEFAULT_MAX_LINE_LENGTH);
        }

        public Lis(File f, int maxLineLength) throws IOException {
            fis = new FileInputStream(f);
            dataBytes = new byte[maxLineLength];
            returnBytes = new byte[maxLineLength];
            bytesRead = 0;
            currentPositionInDataBytes = 0;
        }

        @Override
        public void close() throws Exception {
            try {
                fis.close();
            } catch (Throwable t) {
            }
            fis = null;
        }

        public long position() throws IOException {
            return fis.getChannel().position() - bytesRead + currentPositionInDataBytes;
        }

        public void position(long newPosition) throws IOException {
            fis.getChannel().position(newPosition);
        }

        private LisRet readLine() throws IOException {
            int retIndex = 0;
            while (true) {
                if (readNextChunk()) return null;
                byte b = dataBytes[currentPositionInDataBytes++];

                if (b == LineEscaper.NEWLINE_CHAR) {
                    // Found the new line...
                    return new LisRet(returnBytes, 0, retIndex);
                }
                if (b == LineEscaper.ESCAPE_CHAR) {

                    if (readNextChunk()) return null;
                    b = dataBytes[currentPositionInDataBytes++];

                    // Add byte to return value
                    switch (b) {
                        case LineEscaper.ESCAPE_ESCAPE_CHAR:
                            returnBytes[retIndex++] = LineEscaper.ESCAPE_CHAR;
                            break;
                        case LineEscaper.NEWLINE_ESCAPE_CHAR:
                            returnBytes[retIndex++] = LineEscaper.NEWLINE_CHAR;
                            break;
                        case LineEscaper.CARRIAGERETURN_ESCAPE_CHAR:
                            returnBytes[retIndex++] = LineEscaper.CARRIAGERETURN_CHAR;
                            break;
                        default:
                            returnBytes[retIndex++] = b;
                            break;
                    }
                } else {
                    returnBytes[retIndex++] = b;
                }
            }
        }

        private boolean readNextChunk() throws IOException {
            if (currentPositionInDataBytes >= bytesRead) {
                bytesRead = fis.read(dataBytes);
                if (bytesRead <= 0) {
                    // End of file reached...
                    return true;
                }
                currentPositionInDataBytes = 0;
            }
            return false;
        }
    }

    /**
     * Test loading the entire file from start to finish - each line is the same length
     * @throws Exception
     */
    @Test
    public void testForwardMovesFixedFormat() throws Exception {
        DecimalFormat format = new DecimalFormat("000,000,000");
        File f = new File(fileName);
        if (f.exists()) {
            f.delete();
        }
        try (PrintStream fos = new PrintStream(new BufferedOutputStream(new FileOutputStream(f, false)))) {
            for (int i = 0; i < 1000000; i++) {
                fos.print(format.format(i) + LineEscaper.NEWLINE_CHAR_STR);
            }
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        }

        try (Lis lis = new Lis(f)) {
            int expectedNum = 0;
            long start = System.currentTimeMillis();
            LisRet ret = lis.readLine();
            while (ret != null) {
                String line = new String(ret.data, ret.offset, ret.length);
                String expectedNumStr = format.format(expectedNum);
                assertEquals(expectedNumStr, line);
                ret = lis.readLine();
                expectedNum++;
            }
            long end = System.currentTimeMillis();
            logger.info("Reading " + expectedNum + " lines took " + (end - start) + "(ms)");
        }
    }

    /**
     * Test loading the entire file from start to finish - each line is of different length
     * @throws Exception
     */
    @Test
    public void testForwardMovesVariableFormat() throws Exception {
        DecimalFormat format = new DecimalFormat("###,###,##0");
        File f = new File(fileName);
        if (f.exists()) {
            f.delete();
        }
        try (PrintStream fos = new PrintStream(new BufferedOutputStream(new FileOutputStream(f, false)))) {
            for (int i = 0; i < 1000000; i++) {
                fos.print(format.format(i) + LineEscaper.NEWLINE_CHAR_STR);
            }
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        }

        try (Lis lis = new Lis(f)) {
            int expectedNum = 0;
            long start = System.currentTimeMillis();
            LisRet ret = lis.readLine();
            while (ret != null) {
                String line = new String(ret.data, ret.offset, ret.length);
                String expectedNumStr = format.format(expectedNum);
                assertEquals(expectedNumStr, line);
                ret = lis.readLine();
                expectedNum++;
            }
            long end = System.currentTimeMillis();
            logger.info("Reading " + expectedNum + " lines took " + (end - start) + "(ms)");
        }
    }

    /**
     * The file starts with blank lines
     * @throws Exception
     */
    @Test
    public void testFileStartsWithEmptyLines() throws Exception {
        DecimalFormat format = new DecimalFormat("###,###,##0");
        File f = new File(fileName);
        if (f.exists()) {
            f.delete();
        }
        try (PrintStream fos = new PrintStream(new BufferedOutputStream(new FileOutputStream(f, false)))) {
            fos.print(LineEscaper.NEWLINE_CHAR_STR);
            for (int i = 0; i < 1000; i++) {
                fos.print(format.format(i) + LineEscaper.NEWLINE_CHAR_STR);
            }
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        }

        try (Lis lis = new Lis(f)) {
            int expectedNum = 0;
            LisRet ret = lis.readLine();
            {
                String line = new String(ret.data, ret.offset, ret.length);
                String expectedNumStr = "";
                assertEquals(expectedNumStr, line);
                ret = lis.readLine();
            }
            while (ret != null) {
                String line = new String(ret.data, ret.offset, ret.length);
                String expectedNumStr = format.format(expectedNum);
                assertEquals(expectedNumStr, line);
                ret = lis.readLine();
                expectedNum++;
            }
        }
    }

    /**
     * The file ends with blank lines
     * @throws Exception
     */
    @Test
    public void testFileEndsWithEmptyLines() throws Exception {
        DecimalFormat format = new DecimalFormat("###,###,##0");
        File f = new File(fileName);
        if (f.exists()) {
            f.delete();
        }
        int count = 1000;
        try (PrintStream fos = new PrintStream(new BufferedOutputStream(new FileOutputStream(f, false)))) {
            for (int i = 0; i < count; i++) {
                fos.print(format.format(i) + LineEscaper.NEWLINE_CHAR_STR);
            }
            fos.print(LineEscaper.NEWLINE_CHAR_STR);
            fos.print(LineEscaper.NEWLINE_CHAR_STR);
            fos.print(LineEscaper.NEWLINE_CHAR_STR);
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        }

        try (Lis lis = new Lis(f)) {
            int expectedNum = 0;
            LisRet ret = lis.readLine();
            while (ret != null) {
                if (expectedNum < count) {
                    String line = new String(ret.data, ret.offset, ret.length);
                    String expectedNumStr = format.format(expectedNum);
                    assertEquals(expectedNumStr, line);
                    ret = lis.readLine();
                } else {
                    String line = new String(ret.data, ret.offset, ret.length);
                    String expectedNumStr = "";
                    assertEquals(expectedNumStr, line);
                    ret = lis.readLine();
                }
                expectedNum++;
            }
            assertTrue("Expected " + (count + 3) + " numbers got " + expectedNum, (count + 3) == expectedNum);
        }
    }

    /**
     * The last line in the file does not end with a new line.
     * @throws Exception
     */
    @Test
    public void testFileEndsWithoutNewLines() throws Exception {
        DecimalFormat format = new DecimalFormat("###,###,##0");
        File f = new File(fileName);
        if (f.exists()) {
            f.delete();
        }
        int count = 1000;
        try (PrintStream fos = new PrintStream(new BufferedOutputStream(new FileOutputStream(f, false)))) {
            for (int i = 0; i < count; i++) {
                fos.print(format.format(i) + LineEscaper.NEWLINE_CHAR_STR);
            }
            fos.print(format.format(count));
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        }

        try (Lis lis = new Lis(f)) {
            int expectedNum = 0;
            LisRet ret = lis.readLine();
            while (ret != null) {
                String line = new String(ret.data, ret.offset, ret.length);
                String expectedNumStr = format.format(expectedNum);
                assertEquals(expectedNumStr, line);
                ret = lis.readLine();
                expectedNum++;
            }
            assertTrue("Expected " + count + " numbers got " + expectedNum, count == expectedNum);
        }
    }

    /**
     * Test seeking to a arbitrary position and then moving forward
     * @throws Exception
     */
    @Test
    public void testSeekAndForwardMovesVariableFormat() throws Exception {
        DecimalFormat format = new DecimalFormat("###,###,##0");
        File f = new File(fileName);
        if (f.exists()) {
            f.delete();
        }
        try (PrintStream fos = new PrintStream(new BufferedOutputStream(new FileOutputStream(f, false)))) {
            for (int i = 0; i < 1000000; i++) {
                fos.print(format.format(i) + LineEscaper.NEWLINE_CHAR_STR);
            }
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        }

        try (Lis lis = new Lis(f)) {
            lis.position(31);
            lis.readLine();
            assertTrue("Expected position to be " + 32 + " Got " + lis.position() + " instead ", lis.position() == 32);
            int expectedNum = 14;
            long start = System.currentTimeMillis();
            LisRet ret = lis.readLine();
            while (ret != null) {
                String line = new String(ret.data, ret.offset, ret.length);
                String expectedNumStr = format.format(expectedNum);
                assertEquals(expectedNumStr, line);
                ret = lis.readLine();
                expectedNum++;
            }
            long end = System.currentTimeMillis();
            logger.info("Reading " + expectedNum + " lines took " + (end - start) + "(ms)");
        }
    }
}
