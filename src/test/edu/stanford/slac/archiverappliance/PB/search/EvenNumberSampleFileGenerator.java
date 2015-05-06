/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.search;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.log4j.Logger;

import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;

/**
 * @author mshankar
 * Generates a standard sample file (bunch of even numbers)
 * Sample file is a sequence of string representations of the first million even integers.
 *
 */
public class EvenNumberSampleFileGenerator {
	private static final Logger logger = Logger.getLogger(EvenNumberSampleFileGenerator.class);
	public static final int MAXSAMPLEINT = 10000000;

	public static void generateSampleFile(String fileName) {
		File f = new File(fileName);
		PrintStream fos = null;
		try {
			fos = new PrintStream(new BufferedOutputStream(new FileOutputStream(f, false)));
			for(int i = 0; i <= MAXSAMPLEINT; i=i+2) {
				fos.print("" + i + LineEscaper.NEWLINE_CHAR_STR);
			}
		} catch (IOException ex){
			logger.error(ex.getMessage(), ex);
		} finally {
			if(fos != null) { try { fos.close(); fos = null; } catch (Throwable t) {} } 
		}
	}
	
}
