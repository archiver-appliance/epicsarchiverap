/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.search;

import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import edu.stanford.slac.archiverappliance.plain.CompareEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This is a variant of binary search that searches for an event in a event stream file.
 * We assume a time-sorted PB file separated by unix newlines as outlined in the archiver appliance design doc.
 * We pass in file channel and a function that compares an event line (most likely a PB) to the desired time t1.
 * The fileChannels position is moved to a spot where this constraint is satisfied s1 &le; t1 &lt; s2
 * That is
 * <ol>
 * <li>Search the file for a time t1</li>
 * <li>Do a LineByteStream.seekToFirstNewLine</li>
 * <li>LineByteStream.readLine's after this should give events that satisfy getData's requirements.</li>
 * <li>Remember to terminate appropriately</li>
 * </ol>
 *
 * @author mshankar
 *
 */
public class FileEventStreamSearch {
    private static final Logger logger = LogManager.getLogger(FileEventStreamSearch.class);
    /**
     * Constant use to bound length of searches before giving up.
     */
    private static final int MAXITERATIONS = 1000;
    /**
     * Internally used to indicate the left/lower window for the search
     */
    private long min = 0;
    /**
     * Internally used to indicate the right/upper window for the search
     */
    private long max = 0;
    /**
     * Internally used to the current search point
     */
    private long mid = 0;
    /**
     * Remember the last left/lower bound; this is the requirement for getData when the time t1 is not present exactly in the file.
     */
    private long lastgoright = 0;

    private Path path;

    /**
     * Position where we found the value
     */
    private long foundPosition = 0;

    /**
     * PB files often have a header; so we'd want to start after the header.
     */
    private long startPosition = 0;

    /**
     * @param path      Path
     * @param startPosn a starting position of search PB files
     */
    public FileEventStreamSearch(Path path, long startPosn) {
        this.path = path;
        this.startPosition = startPosn;
    }

    public long getFoundPosition() {
        return foundPosition;
    }

    /**
     * Set the fileChannels position to a point that best satisfies the requirements for getData(t1,...).
     * If found (return value is true), the file's position is set such that
     * <code>
     * 		LineByteStream lis = new LineByteStream(fchannel);
     * 		lis.seekToFirstNewLine();
     * 		byte[] line = lis.readLine();
     * </code>
     * starts returning events that satisfy getData's requirements
     * @return  <code>true</code> or <code>false</code>
     * @param dbrtype ArchDBRType the enumeration type
     * @param yearSecondTimestamp Search seconds into year
     * @throws IOException  &emsp;
     * @see CompareEvent
     */
    public boolean seekToTime(ArchDBRTypes dbrtype, YearSecondTimestamp yearSecondTimestamp) throws IOException {
        CompareEvent comparefunction = new CompareEvent(dbrtype, yearSecondTimestamp);
        return seekToTime(comparefunction);
    }

    /**
     * This should only be used by the unit tests.
     *
     * @param comparefunction CompareEventLine
     * @return <code>true</code> or <code>false</code>
     * @throws IOException when parsing the absolute path
     */
    public boolean seekToTime(CompareEventLine comparefunction) throws IOException {
        boolean found = binarysearch(comparefunction);
        if (found) {
            // We found an exact match.
            return true;
        } else {
            // We did not find an exact match.
            // However, check to see if the location satisfies s1 <= t1 < s2
            if (lastgoright == 0 && startPosition != 0) {
                // Skip the header if any specified.
                lastgoright = startPosition;
            }
            try (LineByteStream lis = new LineByteStream(path, lastgoright)) {
                long currPosn = lis.getCurrentPosition();
                try {
                    lis.seekToFirstNewLine();
                    byte[] line1 = lis.readLine();
                    byte[] line2 = lis.readLine();
                    if (line1 == null || line2 == null || line1.length == 0 || line2.length == 0) {
                        // Nope, we did not find anything.
                        return false;
                    } else {
                        CompareEventLine.NextStep test = comparefunction.compare(line1, line2);
                        // For s1 < t1, the compare function should tell us to go right.
                        // For s1 == t1, it should tell us to stay where we are.
                        // for t1 < s2, the compare function should tell us to go left.
                        if (test == CompareEventLine.NextStep.STAY_WHERE_YOU_ARE) {
                            // We found a location that satisfies s1 <= t1 < s2
                            foundPosition = lastgoright;
                            return true;
                        } else {
                            // In this case, we really did not find the event as it is out of range.
                            return false;
                        }
                    }
                } catch (IOException ex) {
                    logger.error("Exception when parsing " + lis.getAbsolutePath() + " near posn " + currPosn, ex);
                    throw ex;
                }
            }
        }
    }

    private boolean binarysearch(CompareEventLine comparefunction) throws IOException {
        // We bound the binary search to avoid infinite loops.
        int maxIterations = MAXITERATIONS;
        try {
            // Set up binary search.
            min = this.startPosition;
            max = Files.size(path) - 1;
            do {
                mid = min + ((max - min) / 2);
                // System.out.println("Min: " + min + " Mid: " + mid + " Max: " + max);
                try (LineByteStream lis = new LineByteStream(path, mid)) {
                    lis.seekToFirstNewLine();
                    byte[] line1 = lis.readLine();
                    if (line1 == null || line1.length <= 0) {
                        // Empty line in the PB file  - Returning false from search.
                        return false;
                    }
                    byte[] line2 = lis.readLine();

                    CompareEventLine.NextStep nextStep = comparefunction.compare(line1, line2);
                    switch (nextStep) {
                        case GO_LEFT -> max = mid - 1;
                        case GO_RIGHT -> {
                            lastgoright = mid;
                            min = mid + 1;
                        }
                        case STAY_WHERE_YOU_ARE -> {
                            foundPosition = mid;
                            return true;
                        }
                        default -> logger.error("Compare function returned something unexpeected " + nextStep);
                    }

                    maxIterations--;
                }
            } while ((max > min) && maxIterations > 0);
        } catch (Exception ex) {
            throw new IOException(
                    "Exception searching in input stream; min: " + min + " mid: " + mid + " max:" + max, ex);
        }

        if (maxIterations <= 0) {
            throw new IOException("Max iterations reached");
        }

        return false;
    }
}
