/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;


import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

/**
 * Test retrieval across year spans.
 * @author mshankar
 *
 */
@Category(IntegrationTests.class)
public class YearSpanRetrievalTest {
	private static final Logger logger = Logger.getLogger(YearSpanRetrievalTest.class.getName());
	private ConfigService configService;
	PBCommonSetup pbSetup = new PBCommonSetup();
	PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
	TomcatSetup tomcatSetup = new TomcatSetup();

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		pbSetup.setUpRootFolder(pbplugin);
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		generateDataForYears(ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "yspan", (short) 2010, (short) 2013);
	}
	
	private void generateDataForYears(String pvName, short startyear, short endyear) throws IOException {
		// We skip generation of the file only if all the files exist.
		boolean deletefilesandgeneratedata = false;
		for(short currentyear = startyear; currentyear <= endyear; currentyear++) {
			if(!PlainPBPathNameUtility.getPathNameForTime(pbplugin, pvName, TimeUtils.getStartOfYearInSeconds(currentyear), new ArchPaths(), configService.getPVNameToKeyConverter()).toFile().exists()) {
				logger.info("File for year " + currentyear + " does not exist. Generating data for all the years.");
				deletefilesandgeneratedata = true;
				break;
			}
		}
		// Delete all the files for the specified span
		if(deletefilesandgeneratedata) {
			for(short currentyear = startyear; currentyear <= endyear; currentyear++) {
				Files.deleteIfExists(PlainPBPathNameUtility.getPathNameForTime(pbplugin, pvName, TimeUtils.getStartOfYearInSeconds(currentyear), new ArchPaths(), configService.getPVNameToKeyConverter())); 
			}

			SimulationEventStream simstream = new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0), startyear, endyear);
			// The pbplugin should handle all the rotation etc.
			try(BasicContext context = new BasicContext()) {
				pbplugin.appendData(context, pvName, simstream);
			}
		}
	}

	@After
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
	}

	static class YearCount {
		int count = 0;
	}

	static long previousEpochSeconds = 0; 
	@Test
	public void testYearSpan() {
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		Timestamp start = TimeUtils.convertFromISO8601String("2011-12-31T08:00:00.000Z");
		Timestamp end = TimeUtils.convertFromISO8601String("2012-01-01T08:00:00.000Z");
		EventStream stream = null;
		try {
			stream = rawDataRetrieval.getDataForPVS(new String[] { ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "yspan" }, start, end, new RetrievalEventProcessor() {
				@Override
				public void newPVOnStream(EventStreamDesc desc) {
					logger.info("On the client side, switching to processing PV " + desc.getPvName());
					previousEpochSeconds = 0;
				}
			});

			
			// We are making sure that the stream we get back has times in sequential order...
			
			
			HashMap<Short, YearCount> counts = new HashMap<Short, YearCount>(); 
			
			for(Event e : stream) {
				long actualSeconds = e.getEpochSeconds();
				assertTrue(actualSeconds >= previousEpochSeconds);
				previousEpochSeconds = actualSeconds;
				
				YearSecondTimestamp actualYST = TimeUtils.convertToYearSecondTimestamp(actualSeconds);
				YearCount ycount = counts.get(Short.valueOf(actualYST.getYear()));
				if(ycount == null) {
					ycount = new YearCount();
					counts.put(Short.valueOf(actualYST.getYear()), ycount);
				}
				ycount.count++;
			}
			
			assertTrue(counts.get(Short.valueOf((short)2011)).count > 20000);
			assertTrue(counts.get(Short.valueOf((short)2012)).count > 20000);
		} finally {
			if(stream != null) try { stream.close(); stream = null; } catch(Throwable t) { }
		}
	}	
}
