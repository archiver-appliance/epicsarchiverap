/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.utils;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

/**
 * Simple class to escape/unescape newlines in binary data.
 * Eventually, this should be recoded as somthing that can participate in NIO.
 * @author mshankar
 *
 */
public class LineEscaper {
	public static final byte ESCAPE_CHAR = 0x1B;
	public static final byte ESCAPE_ESCAPE_CHAR = 0x01;
	public static final byte NEWLINE_CHAR = 0x0A;
	public static final byte NEWLINE_ESCAPE_CHAR = 0x02;
	public static final String NEWLINE_CHAR_STR = "\n";
	public static final byte CARRIAGERETURN_CHAR = 0x0D;
	public static final byte CARRIAGERETURN_ESCAPE_CHAR = 0x03;
	private static Logger logger = Logger.getLogger(LineEscaper.class.getName());
	
	public static byte[] escapeNewLines(byte[] input) {
		if(input == null) return null;
		
		int output_len = input.length;
		
		for (byte b : input) {
				switch(b) {
				case ESCAPE_CHAR:
				case NEWLINE_CHAR:
				case CARRIAGERETURN_CHAR:
						output_len++;
						break;
				}
		}
		
		byte[] output = new byte[output_len];
		int o = 0;
		
		for (byte b : input) {
				switch(b) {
				case ESCAPE_CHAR:
						output[o++] = ESCAPE_CHAR;
						output[o++] = ESCAPE_ESCAPE_CHAR;
						break;
				case NEWLINE_CHAR:
						output[o++] = ESCAPE_CHAR;
						output[o++] = NEWLINE_ESCAPE_CHAR;
						break;
				case CARRIAGERETURN_CHAR:
						output[o++] = ESCAPE_CHAR;
						output[o++] = CARRIAGERETURN_ESCAPE_CHAR;
						break;
				default:
						output[o++] = b;
						break;
				}
		}
		
		return output;
	}

	public static byte[] unescapeNewLines(byte[] input) {
		if(input == null) return null;
		try {
			// TODO if we eliminate raw byte arrays here, we can shave off about 1 sec out of 17 secs or so for 1Hz 1 year data.
			ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);
			for(int i = 0; i < input.length; i++) {
				byte b = input[i];
				if(b == ESCAPE_CHAR) {
					i++;
					if(i >= input.length) { throw new RuntimeException("Index " + i + " is greater then input array length " + input.length); }
					b = input[i];
					switch(b) {
					case ESCAPE_ESCAPE_CHAR: bos.write(ESCAPE_CHAR);break;
					case NEWLINE_ESCAPE_CHAR: bos.write(NEWLINE_CHAR);break;
					case CARRIAGERETURN_ESCAPE_CHAR: bos.write(CARRIAGERETURN_CHAR);break;
					default: bos.write(b);break;
					}
				} else {
					bos.write(b);
				}
			}
			bos.close();
			return bos.toByteArray();
		} catch(IOException ex) {
			logger.error("Exception unescaping new lines ", ex);
		}
		return null;
	}
}
