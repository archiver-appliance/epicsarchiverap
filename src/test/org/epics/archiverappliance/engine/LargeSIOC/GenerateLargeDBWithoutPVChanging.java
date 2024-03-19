/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.LargeSIOC;

/**
 * Generates a large DB file full of ai's to stdout
 * @author mshankar
 *
 */
public class GenerateLargeDBWithoutPVChanging {
	
	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.err.println("Usage: java org.epics.archiverappliance.engine.LargeSIOC.GenerateLargeDBWithoutPVChanging <StartingAt> <PVCount>");
			return;
		}
		/*record(ai, "test:enable0")
{
        field(SCAN, "1 second")
        field(DESC, "Analog input No")
        field(INP, "test:enable0.VAL NPP NMS")

}
		 * 
		 * */
		int startingAt = Integer.parseInt(args[0]);
		int pvcount = Integer.parseInt(args[1]);
		for(int i = startingAt; i <(startingAt+pvcount); i++) {
			int m=i/200;
			System.out.println("record(ai, \"luofeng"+m+":step" + (i) + "\") {\n" +
					  "field(INP, \"luofeng"+m+":step"+ (i)+".VAL NPP NMS\")\n" + 
					  "field(HIHI, \"0.9\")\n" + 
					  "field(HHSV, \"MAJOR\")\n" + 
					  "field(HIGH, \"0.6\")\n" + 
					  "field(HSV, \"MINOR\")\n" + 
					  "field(LOW, \"-0.3\")\n" + 
					  "field(LSV, \"MINOR\")\n" + 
					  "field(LOLO, \"-0.5\")\n" + 
					  "field(LLSV, \"MAJOR\")\n" + 
					  "field(HOPR, \"0.8\")\n" + 
					  "field(ADEL, \"0.5\")\n" + 
					  "field(MDEL, \"0\")\n" + 
					  "}\n\n"
					  );
			
			
			 //field(INPA, "--ArchUnitTest:sine:calc.VAL NPP")
			
		}
		
	
	}
}
