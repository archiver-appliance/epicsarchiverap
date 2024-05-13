/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.search;

import java.io.IOException;

/**
 * This is a special kind of comparison function meant for a combo binary/linear search function.
 * This takes in a byte sequence (perhaps from a PB file) and compares it to the point (timestamp) being sought.
 * It then returns one of the enums to direct the direction of search.
 * We assume that time is from the left (past) to the right (future).
 * @author mshankar
 */
public interface CompareEventLine {
    enum NextStep {
        GO_LEFT,
        GO_RIGHT,
        STAY_WHERE_YOU_ARE
    }

    /**
     * This is the actual comparison function.
     * @param line1 - A full PB line
     * @param line2 - A full PB line
     * @return NextStep one of the eumuration of NextStep
     * @throws IOException  &emsp;
     */
    public NextStep compare(byte[] line1, byte[] line2) throws IOException;
}
