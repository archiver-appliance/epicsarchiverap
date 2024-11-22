/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.utils;

import java.io.IOException;

/**
 * An exception thrown by the LineByteStream when we encounter lines that are too long.
 * The LineByteStream can handle lines of any size; however to prevent infinite loops and such we have a safety factor
 * If a lines is longer than this safety factor, this exception is thrown.
 * @author mshankar
 *
 */
public class LineTooLongException extends IOException {
    private static final long serialVersionUID = 2786892485512623424L;

    public LineTooLongException(String message) {
        super(message);
    }
}
