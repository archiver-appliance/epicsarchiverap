/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.utils;

import edu.stanford.slac.archiverappliance.PBOverHTTP.PBOverHTTPStoragePlugin;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.time.Instant;

/**
 * @author mshankar
 * Dumps data for a PV into something that can be used to plot into gnuplot.
 *
 */
public class ConvertGnuplotData {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        PBOverHTTPStoragePlugin storagePlugin = new PBOverHTTPStoragePlugin();
        ConfigService configService = new ConfigServiceForTests(-1);
        storagePlugin.initialize("http://archiver:15646/retrieval/data/getData.raw", configService);

        // Ask for a days worth of data
        Instant start = TimeUtils.convertFromISO8601String("2011-02-01T08:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String("2011-02-02T08:00:00.000Z");

        String pvName = "Sine1";
        if (args.length > 0) {
            pvName = args[0];
        }

        String dataFileName = "/tmp/archappdat";
        if (args.length > 1) {
            dataFileName = args[1];
        }

        File f = new File(dataFileName);
        FileOutputStream fos = new FileOutputStream(f);
        PrintWriter out = new PrintWriter(new BufferedOutputStream(fos));
        long s = System.currentTimeMillis();
        try (BasicContext context = new BasicContext();
                EventStream st = new CurrentThreadWorkerEventStream(
                        pvName,
                        storagePlugin.getDataForPV(context, pvName, start, end, new DefaultRawPostProcessor()))) {
            int totalEvents = 0;
            try {
                for (Event e : st) {
                    out.println(e.getEpochSeconds() + "\t" + e.getSampleValue().toString());
                    totalEvents++;
                }
            } finally {
                st.close();
            }
            long e = System.currentTimeMillis();
            System.out.println("Found a total of " + totalEvents + " in " + (e - s) + "(ms)");
        }
        out.flush();
        out.close();
    }
}
