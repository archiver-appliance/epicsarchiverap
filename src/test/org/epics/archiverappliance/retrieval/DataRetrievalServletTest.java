/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PBOverHTTP.PBOverHTTPStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;

/**
 * Test the simple data retrieval case.  
 * @author mshankar
 *
 */
@Tag("integration")
public class DataRetrievalServletTest {
	
	private static Logger logger = LogManager.getLogger(DataRetrievalServletTest.class.getName());
	private ConfigService configService;
	PBCommonSetup pbSetup = new PBCommonSetup();
	PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
	TomcatSetup tomcatSetup = new TomcatSetup();

	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "_dataretrieval";
	short year = (short) 2011;
	
	@BeforeEach
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(-1);
		pbSetup.setUpRootFolder(pbplugin);
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
	}

	@AfterEach
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();

		Files.deleteIfExists(PlainPBPathNameUtility.getPathNameForTime(pbplugin, pvName, TimeUtils.getStartOfYear(year), new ArchPaths(), configService.getPVNameToKeyConverter()));
	}
	
	
	/**
	 * Test that makes sure that the merge dedup gives data whose timestamps are ascending.
	 */
	@Test
	public void testTimesAreSequential() throws Exception {
		PBOverHTTPStoragePlugin storagePlugin = new PBOverHTTPStoragePlugin();
		ConfigService configService = new ConfigServiceForTests(-1);
		storagePlugin.initialize("pbraw://localhost?rawURL=" + URLEncoder.encode("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw", StandardCharsets.UTF_8), configService);

		Files.deleteIfExists(PlainPBPathNameUtility.getPathNameForTime(pbplugin, pvName, TimeUtils.getStartOfYear(year), new ArchPaths(), configService.getPVNameToKeyConverter()));
		SimulationEventStream simstream = new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0), TimeUtils.getStartOfYear(year), TimeUtils.getEndOfYear(year), 1);
		try(BasicContext context = new BasicContext()) {
			pbplugin.appendData(context, pvName, simstream);
		}

		// Ask for a days worth of data
		Instant start = TimeUtils.convertFromISO8601String("2011-02-01T08:00:00.000Z");
		Instant end = TimeUtils.convertFromISO8601String("2011-02-02T08:00:00.000Z");
		
		long starttimeinseconds = TimeUtils.convertToEpochSeconds(start);
		
		long s = System.currentTimeMillis();
		long next = -1;
		try(BasicContext context = new BasicContext(); EventStream stream = new CurrentThreadWorkerEventStream(pvName, storagePlugin.getDataForPV(context, pvName, start, end, new DefaultRawPostProcessor()))) {
			int totalEvents = 0;
			// Goes through the stream
			for(Event e : stream) {
				System.out.println(e.getRawForm());
				long actualSeconds = e.getEpochSeconds();
				long desired = starttimeinseconds + next++;
				Assertions.assertTrue(actualSeconds >= desired, "Expecting "
				+ TimeUtils.convertToISO8601String(desired)
				+ " got "
				+ TimeUtils.convertToISO8601String(actualSeconds)
				+ " at eventcount " +
				totalEvents);
				totalEvents++;
			}
			long e = System.currentTimeMillis();
			logger.info("Found a total of " + totalEvents + " in " + (e-s) + "(ms)");
		}
	}
}
