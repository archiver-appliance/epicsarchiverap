/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.utils;

import org.epics.archiverappliance.ByteArray;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

/**
 * Test the LineEscaper.
 * @author mshankar
 *
 */
public class LineEscaperTest {

    @Test
    public void testEscapeNewLines() {
        Random random = new Random();
        int total = 1000000;
        long exectime = 0;
        for (int i = 0; i < total; i++) {
            try {
                byte[] randombytes = new byte[64];
                random.nextBytes(randombytes);
                long start = System.currentTimeMillis();
                byte[] escapedbytes = LineEscaper.escapeNewLines(randombytes);
                byte[] unescapedbytes = LineEscaper.unescapeNewLines(escapedbytes);
                long end = System.currentTimeMillis();
                exectime += (end - start);
                if (!Arrays.equals(randombytes, unescapedbytes)) {
                    Assertions.fail("Test failed");
                }
            } catch (Exception ex) {
                Assertions.fail("Test failed with exception " + ex.getMessage());
            }
        }
        System.out.println("Time to escape/unescape " + total + " byte sequences is " + (exectime) / 1000
                + "(s) yielding " + (((float) total) / ((exectime) / 1000)) + " sequences per second");
    }

    @Test
    public void testEscapeNewLinesByteArray() {
        Random random = new Random();
        int total = 1000000;
        long exectime = 0;
        for (int i = 0; i < total; i++) {
            try {
                byte[] randombytes = new byte[64];
                random.nextBytes(randombytes);
                long start = System.currentTimeMillis();
                byte[] escapedbytes = LineEscaper.escapeNewLines(randombytes);
                ByteArray bar = new ByteArray(escapedbytes);
                bar.inPlaceUnescape();
                byte[] unescapedbytes = bar.unescapedBytes();
                long end = System.currentTimeMillis();
                exectime += (end - start);
                if (!Arrays.equals(randombytes, unescapedbytes)) {
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
