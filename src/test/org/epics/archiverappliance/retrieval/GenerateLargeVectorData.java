/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.VectorValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mshankar
 *
 */
public class GenerateLargeVectorData {
	private static Logger logger = LogManager.getLogger(GenerateLargeVectorData.class.getName());

	/**
	 * Generate a year's worth of data in the specified folder for the specified PV. 
	 * PV is assumed to be a DBR_DOUBLE waveform with 80000 elements. Assume daily partitions. 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.err.println("Usage: java org.epics.archiverappliance.retrieval.GenerateLargeVectorData <pvName> <folder>");
			return;
		}
		
		String pvName = args[0];
		String folder = args[1];
		
		
		ConfigService configService = new ConfigServiceForTests(new File("./bin"), 1);
		String pluginURL = "pb://localhost?name=LTS&rootFolder=" + folder + "&partitionGranularity=PARTITION_DAY";
		StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(pluginURL, configService);

        Instant end = TimeUtils.minusDays(TimeUtils.now(), 1);
        Instant start = TimeUtils.minusDays(end, 30);
		long startEpochSeconds = TimeUtils.convertToEpochSeconds(start);
		long endEpochSeconds = TimeUtils.convertToEpochSeconds(end);
		
		logger.info("Generating data for pv " + pvName + " using plugin " + plugin.getDescription() + " between " + TimeUtils.convertToHumanReadableString(start) + " and " + TimeUtils.convertToHumanReadableString(end));

		long currentSeconds = startEpochSeconds;
		while(currentSeconds < endEpochSeconds) {
			int eventsPerShot = 60;
			ArrayListEventStream instream = new ArrayListEventStream(eventsPerShot, new RemotableEventStreamDesc(ArchDBRTypes.DBR_WAVEFORM_DOUBLE, pvName, TimeUtils.computeYearForEpochSeconds(currentSeconds)));
			for(int i = 0; i < eventsPerShot; i++) {
				logger.info("Generating data for " + TimeUtils.convertToHumanReadableString(currentSeconds));
				YearSecondTimestamp yts = TimeUtils.convertToYearSecondTimestamp(currentSeconds);
				List<Double> vals = new ArrayList<Double>(80000);
				for(int k = 0; k < 80000; k++) { 
					vals.add(Double.valueOf(k));
				}
				instream.add(new SimulationEvent(yts.getSecondsintoyear(), yts.getYear(), ArchDBRTypes.DBR_WAVEFORM_DOUBLE, new VectorValue<Double>(vals)));
				currentSeconds += 60;
			}
			try(BasicContext context = new BasicContext()) {
				plugin.appendData(context, pvName, instream);
			}
		}
		logger.info("Done generating data for pv " + pvName + " using plugin " + plugin.getDescription() + " between " + TimeUtils.convertToHumanReadableString(start) + " and " + TimeUtils.convertToHumanReadableString(end));
		configService.shutdownNow();
	}
}
