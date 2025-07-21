/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * We convert PB exceptions into a runtime exception to minimize the number of IOExceptions in the method declarations.
 * We do want to avoid unmarshalling on construction; however, this forces us to unmarshal ( if needed )  on the gets.
 * @author mshankar
 *
 */
public class PBParseException extends RuntimeException {
    private static final long serialVersionUID = 5552973945298428020L;
    private static Logger logger = LogManager.getLogger(PBParseException.class.getName());

    public PBParseException(Exception ex) {
        super(ex);
    }

    public PBParseException(byte[] databytes, Exception ex) {
        super("Length of byte array = " + ((databytes != null) ? databytes.length : "null"), ex);
        if (logger.isDebugEnabled() && databytes != null) {
            logger.debug(Hex.encodeHexString(databytes));
        }
    }
}
