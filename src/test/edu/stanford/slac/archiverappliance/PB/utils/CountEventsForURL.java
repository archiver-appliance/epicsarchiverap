/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.utils;

import java.io.File;
import java.sql.Timestamp;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;

import edu.stanford.slac.archiverappliance.PBOverHTTP.PBOverHTTPStoragePlugin;

/**
 * Count the number of events in a PB stream given a URL.
 * @author mshankar
 *
 */
public class CountEventsForURL {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		PBOverHTTPStoragePlugin storagePlugin = new PBOverHTTPStoragePlugin();
		ConfigService configService = new ConfigServiceForTests(new File("./bin"));
		storagePlugin.initialize("http://archiver:15646/retrieval/data/getData.raw", configService);

		// Ask for a days worth of data
		Timestamp start = TimeUtils.convertFromISO8601String("2011-02-01T08:00:00.000Z");
		Timestamp end = TimeUtils.convertFromISO8601String("2011-02-02T08:00:00.000Z");
		
		String pvName = "Test";
		if(args.length > 0) {
			pvName = args[0];
		}
		
		long s = System.currentTimeMillis();
		try(BasicContext context = new BasicContext(); EventStream st = new CurrentThreadWorkerEventStream(pvName, storagePlugin.getDataForPV(context, pvName, start, end, new DefaultRawPostProcessor()))) {
			int totalEvents = 0;
			try {
				for(@SuppressWarnings("unused") Event e : st) {
					// Here's where we do something to events.
					totalEvents++;
				}
			} finally {
				try { st.close(); } catch(Exception ex) { }					
			}
			long e = System.currentTimeMillis();
			System.out.println("Found a total of " + totalEvents + " in " + (e-s) + "(ms)");
		}
	}
}
