/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB.utils;

import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.FieldValue;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;

/**
 * @author mshankar
 * Low level utility for printing PB/HTTP response dumps. 
 */
public class PrintPBResponse {
	private static Logger logger = LogManager.getLogger(PrintPBResponse.class.getName());
	public static void main(String[] args) throws Exception {
		if(args == null || args.length < 1) { 
			System.err.println("Usage: java edu.stanford.slac.archiverappliance.PlainPB.utils.PrintTimes <PB/HTTP response dump>");
			return;
		}
		
		for(String fileName : args) {
			Path path = Paths.get(fileName);
			System.out.println("Printing times for file " + path.toAbsolutePath().toString());
			Constructor<? extends DBRTimeEvent> unmarshallingConstructor = null;
			short year = 1970;
			int lineNum = 1;
			try(LineByteStream lis = new LineByteStream(path)) {
				byte[] nextLine = lis.readLine();
				while(nextLine != null) { 
					if(unmarshallingConstructor == null) { 
						PayloadInfo info = PayloadInfo.parseFrom(LineEscaper.unescapeNewLines(nextLine));
						ArchDBRTypes dbrType = ArchDBRTypes.valueOf(info.getType());
						year = (short) info.getYear();
						System.out.println("Parsing payload info type is " + dbrType + " and data is for year " + year + " for the PV " + info.getPvname() + " Elementcount is " + info.getElementCount());
						for(FieldValue fieldValue : info.getHeadersList()) { 
							System.out.println("\tHeader " + fieldValue.getName() + " ==> " + fieldValue.getVal());
						}
						unmarshallingConstructor = DBR2PBTypeMapping.getPBClassFor(dbrType).getUnmarshallingFromByteArrayConstructor();
					} else { 
						if(nextLine.length <= 0) {
							System.out.println("Resetting the unmarshallingConstructor");
							unmarshallingConstructor = null;
						} else {
							try { 
								DBRTimeEvent ev = (DBRTimeEvent) unmarshallingConstructor.newInstance(year, new ByteArray(nextLine));
								System.out.println(TimeUtils.convertToISO8601String(((DBRTimeEvent)ev).getEventTimeStamp())
										+ "\t" + TimeUtils.convertToHumanReadableString((((DBRTimeEvent)ev).getEventTimeStamp()))
										+ "\t" + ev.getSampleValue().toString()
										+ "\t" + (((DBRTimeEvent)ev).getSeverity())
										+ "\t" + (((DBRTimeEvent)ev).getStatus())
										);
							} catch(Exception ex) { 
								logger.error("Exception unmarshalling line: " + lineNum, ex);
							}
						}
					}
					nextLine = lis.readLine();
					lineNum++;
				}
			}			
		}
	}
}
