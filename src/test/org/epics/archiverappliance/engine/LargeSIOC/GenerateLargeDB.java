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
public class GenerateLargeDB {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                    "Usage: java org.epics.archiverappliance.engine.LargeSIOC.GenerateLargeDB <StartingAt> <PVCount>");
            return;
        }

        int startingAt = Integer.parseInt(args[0]);
        int pvcount = Integer.parseInt(args[1]);
        for (int i = startingAt; i < (startingAt + pvcount); i++) {
            int m = i / 200;
            System.out.println("record(calc, \"luofeng" + m + ":step" + (i) + "\") {\n" + "field(SCAN, \"1 second\")\n"
                    + "field(INPA, \"luofeng"
                    + m + ":step" + (i) + ".VAL\")\n" + "field(CALC, \"(A<1)?A+0.0001:-1\")\n"
                    + "field(HIHI, \"0.9\")\n"
                    + "field(HHSV, \"MAJOR\")\n"
                    + "field(HIGH, \"0.6\")\n"
                    + "field(HSV, \"MINOR\")\n"
                    + "field(LOW, \"-0.3\")\n"
                    + "field(LSV, \"MINOR\")\n"
                    + "field(LOLO, \"-0.5\")\n"
                    + "field(LLSV, \"MAJOR\")\n"
                    + "field(HOPR, \"0.8\")\n"
                    + "field(ADEL, \"0.5\")\n"
                    + "field(MDEL, \"0\")\n"
                    + "}\n\n");

            // field(INPA, "--ArchUnitTest:sine:calc.VAL NPP")

        }
    }
}
