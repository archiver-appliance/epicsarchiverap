package org.epics.archiverappliance;

import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

public class ByteArrayTest {

    @Test
    public void testByteArray() {
        Random random = new Random();
        int total = 10000;
        long exectime = 0;
        for (int i = 0; i < total; i++) {
            try {
                byte[] startBytes = new byte[total];
                random.nextBytes(startBytes);
                long start = System.currentTimeMillis();
                byte[] escapedData = LineEscaper.escapeNewLines(startBytes);
                ByteArray bar = new ByteArray(escapedData);
                bar.doubleBufferSize();
                if (!Arrays.equals(escapedData, Arrays.copyOfRange(bar.data, bar.off, bar.len))) {
                    Assertions.fail("Test failed");
                }
                bar.inPlaceUnescape();
                long end = System.currentTimeMillis();
                exectime += (end - start);
                if (!Arrays.equals(startBytes, Arrays.copyOfRange(bar.unescapedData, bar.off, bar.unescapedLen))) {
                    Assertions.fail("Test failed");
                }

            } catch (Exception ex) {
                Assertions.fail("Test failed with exception " + ex.getMessage());
            }
        }
        System.out.println("Time to escape/unescape " + total + " byte sequences is " + (exectime) / 1000
                + "(s) yielding " + (((float) total) / ((exectime) / 1000)) + " sequences per second");
    }
}
