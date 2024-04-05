package org.epics.archiverappliance.zipfs;

import com.google.common.base.Ascii;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

/**
 * Test ways to optimize parallel fetch
 * @author mshankar
 *
 */
public class ZipFetchTest {
    private static Logger logger = LogManager.getLogger(ZipFetchTest.class.getName());
    private static final int NUM_DAYS = 365;
    private static final int DATA_PER_DAY = 1024 * 1024;
    private static final int SAMPLE_SIZE = NUM_DAYS * DATA_PER_DAY;
    private static final int LINE_SIZE = 32;
    private byte[] data = new byte[SAMPLE_SIZE];

    @BeforeEach
    public void setUp() throws Exception {
        byte[] line = new byte[LINE_SIZE];
        for (int i = 0; i < LINE_SIZE - 1; i++) {
            line[i] = Ascii.SPACE;
        }
        line[LINE_SIZE - 1] = LineEscaper.NEWLINE_CHAR;
        for (int l = 0; l < SAMPLE_SIZE / LINE_SIZE; l++) {
            System.arraycopy(line, 0, data, l * LINE_SIZE, line.length);
        }
    }

    @AfterEach
    public void tearDown() throws Exception {}

    private static int countLinesInDay(byte[] data, int day) throws IOException {
        int dayStartsAt = day * DATA_PER_DAY;
        int lineCount = 0;
        byte[] input = new byte[LINE_SIZE];
        byte[] output = new byte[LINE_SIZE];
        for (int l = 0; l < DATA_PER_DAY / LINE_SIZE; l++) {
            int linei = 0;
            System.arraycopy(data, dayStartsAt + l * LINE_SIZE, input, 0, input.length);
            for (int i = 0; i < input.length; i++) {
                byte b = input[i];
                if (b == LineEscaper.ESCAPE_CHAR) {
                    i++;
                    if (i >= input.length) {
                        throw new RuntimeException(
                                "Index " + i + " is greater then input array length " + input.length);
                    }
                    b = input[i];
                    switch (b) {
                        case LineEscaper.ESCAPE_ESCAPE_CHAR:
                            output[linei++] = LineEscaper.ESCAPE_CHAR;
                            break;
                        case LineEscaper.NEWLINE_ESCAPE_CHAR:
                            output[linei++] = LineEscaper.NEWLINE_CHAR;
                            break;
                        case LineEscaper.CARRIAGERETURN_ESCAPE_CHAR:
                            output[linei++] = LineEscaper.CARRIAGERETURN_CHAR;
                            break;
                        default:
                            output[linei++] = b;
                            break;
                    }
                } else {
                    output[linei++] = b;
                }
            }
            lineCount++;
        }
        return lineCount;
    }

    @Test
    public void compareSpeedup() throws Exception {
        long serial = testSerialFetch();
        long parallel = testParallelFetch();
        logger.info("Speedup as a % " + (parallel * 100.0 / serial));
    }

    private long testSerialFetch() throws Exception {
        long st0 = System.currentTimeMillis();
        int totallinecount = 0;
        for (int day = 0; day < NUM_DAYS; day++) {
            totallinecount += countLinesInDay(data, day);
        }
        long st1 = System.currentTimeMillis();
        logger.info("Time taken for serial decoding " + (st1 - st0) + "(ms) yielding " + totallinecount + " lines");
        return st1 - st0;
    }

    private static class ParallelFetch implements Callable<Integer> {
        private byte[] data;
        private int day;

        ParallelFetch(byte[] data, int day) {
            this.data = data;
            this.day = day;
        }

        @Override
        public Integer call() {
            try {
                return countLinesInDay(data, day);
            } catch (IOException ex) {
                logger.error(ex);
            }
            return 0;
        }
    }

    private long testParallelFetch() throws Exception {
        ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        logger.info("The parallelism in the pool is " + forkJoinPool.getParallelism());
        long st0 = System.currentTimeMillis();
        List<Future<Integer>> futures = new LinkedList<Future<Integer>>();
        for (int day = 0; day < NUM_DAYS; day++) {
            futures.add(forkJoinPool.submit(new ParallelFetch(data, day)));
        }
        long firstTaskFinish = 0;
        int totallinecount = 0;
        for (Future<Integer> future : futures) {
            int numLines = future.get();
            if (firstTaskFinish == 0) {
                firstTaskFinish = System.currentTimeMillis();
            }
            Assertions.assertTrue(numLines > 1, "Day had no lines ");
            totallinecount += numLines;
        }
        long st1 = System.currentTimeMillis();
        logger.info("Time taken for parallel decoding " + (st1 - st0) + "(ms) yielding " + totallinecount
                + " lines. The first task took " + (firstTaskFinish - st0));
        forkJoinPool.shutdown();
        return st1 - st0;
    }
}
