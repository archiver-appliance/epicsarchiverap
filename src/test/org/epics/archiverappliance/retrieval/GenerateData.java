/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

/**
 * Used to generate data for unit tests.
 * @author mshankar
 *
 */
public class GenerateData {
	private static Logger logger = Logger.getLogger(GenerateData.class.getName());

	/**
	 * We generate a sine wave for the data if it does not already exist.
	 * @param filenum
	 * @throws IOException 
	 */
	public static void generateSineForPV(String pvName, int phasediffindegrees, ArchDBRTypes type) throws Exception {
		ConfigService configService = new ConfigServiceForTests(new File("./bin"), 1);
		PlainPBStoragePlugin storagePlugin = new PlainPBStoragePlugin();
		PBCommonSetup setup = new PBCommonSetup();
		setup.setUpRootFolder(storagePlugin);
		try(BasicContext context = new BasicContext()) {
			if(!Files.exists(PlainPBPathNameUtility.getPathNameForTime(storagePlugin, pvName, TimeUtils.getStartOfCurrentYearInSeconds(), context.getPaths(), configService.getPVNameToKeyConverter()))) {
				SimulationEventStream simstream = new SimulationEventStream(type, new SineGenerator(phasediffindegrees));
				storagePlugin.appendData(context, pvName, simstream);
			}
		}
		configService.shutdownNow();
	}
	
	
	/**
	 * Given a plugin URL, the main method generates a couple of years worth of sine data into the plugin until 'yesterday' 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.err.println("Usage: java org.epics.archiverappliance.retrieval.GenerateData <pvName> <pluginURL>");
			return;
		}
		
		String pvName = args[0];
		String pluginURL = args[1];
		
		
		ConfigService configService = new ConfigServiceForTests(new File("./bin"), 1);
		StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(pluginURL, configService);
		
		Timestamp end = TimeUtils.minusDays(TimeUtils.now(), 1);
		Timestamp start = TimeUtils.minusDays(end, 365*2);
		long startEpochSeconds = TimeUtils.convertToEpochSeconds(start);
		long endEpochSeconds = TimeUtils.convertToEpochSeconds(end);
		
		logger.info("Generating data for pv " + pvName + " using plugin " + plugin.getDescription() + " between " + TimeUtils.convertToHumanReadableString(start) + " and " + TimeUtils.convertToHumanReadableString(end));

		long currentSeconds = startEpochSeconds;
		while(currentSeconds < endEpochSeconds) {
			int eventsPerShot = 60*60*24;
			ArrayListEventStream instream = new ArrayListEventStream(eventsPerShot, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.computeYearForEpochSeconds(currentSeconds)));
			for(int i = 0; i < eventsPerShot; i++) {
				YearSecondTimestamp yts = TimeUtils.convertToYearSecondTimestamp(currentSeconds);
				instream.add(new SimulationEvent(yts.getSecondsintoyear(), yts.getYear(), ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(Math.sin((double)yts.getSecondsintoyear()))));
				currentSeconds++;
			}
			try(BasicContext context = new BasicContext()) {
				plugin.appendData(context, pvName, instream);
			}
		}
		logger.info("Done generating data for pv " + pvName + " using plugin " + plugin.getDescription() + " between " + TimeUtils.convertToHumanReadableString(start) + " and " + TimeUtils.convertToHumanReadableString(end));
	}
}
