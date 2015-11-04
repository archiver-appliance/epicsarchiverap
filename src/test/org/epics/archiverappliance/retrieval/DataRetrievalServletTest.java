/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.sql.Timestamp;

import org.apache.log4j.Logger;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PBOverHTTP.PBOverHTTPStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

/**
 * Test the simple data retrieval case.  
 * @author mshankar
 *
 */
public class DataRetrievalServletTest {
	private static Logger logger = Logger.getLogger(DataRetrievalServletTest.class.getName());
	private ConfigService configService;
	PBCommonSetup pbSetup = new PBCommonSetup();
	PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
	TomcatSetup tomcatSetup = new TomcatSetup();
	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		pbSetup.setUpRootFolder(pbplugin);
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
	}

	@After
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();

		Files.deleteIfExists(PlainPBPathNameUtility.getPathNameForTime(pbplugin, pvName, TimeUtils.getStartOfYearInSeconds(year), new ArchPaths(), configService.getPVNameToKeyConverter()));
	}

	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "_dataretrieval";
	short year = (short) 2011;
	
	
	/**
	 * Test that makes sure that the merge dedup gives data whose timestamps are ascending.
	 */
	@Test
	public void testTimesAreSequential() throws Exception {
		PBOverHTTPStoragePlugin storagePlugin = new PBOverHTTPStoragePlugin();
		ConfigService configService = new ConfigServiceForTests(new File("./bin"));
		storagePlugin.initialize("pbraw://localhost?rawURL=" + URLEncoder.encode("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw", "UTF-8"), configService);
		
		Files.deleteIfExists(PlainPBPathNameUtility.getPathNameForTime(pbplugin, pvName, TimeUtils.getStartOfYearInSeconds(year), new ArchPaths(), configService.getPVNameToKeyConverter()));
		SimulationEventStream simstream = new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0), year);
		try(BasicContext context = new BasicContext()) {
			pbplugin.appendData(context, pvName, simstream);
		}

		// Ask for a days worth of data
		Timestamp start = TimeUtils.convertFromISO8601String("2011-02-01T08:00:00.000Z");
		Timestamp end = TimeUtils.convertFromISO8601String("2011-02-02T08:00:00.000Z");
		
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
				assertTrue("Expecting " 
				+ TimeUtils.convertToISO8601String(desired) 
				+ " got " 
				+ TimeUtils.convertToISO8601String(actualSeconds) 
				+ " at eventcount " + 
				totalEvents,
				actualSeconds >= desired);
				totalEvents++;
			}
			long e = System.currentTimeMillis();
			logger.info("Found a total of " + totalEvents + " in " + (e-s) + "(ms)");
		}
	}
}
