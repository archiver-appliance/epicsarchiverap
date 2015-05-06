/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import static org.junit.Assert.assertTrue;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;

/**
 * Tests the various methods needed for ETLDest as implemented by the PlainPBStorage plugin
 * @author mshankar
 *
 */
public class PlainPBETLDestTest {
	private static final Logger logger = Logger.getLogger(PlainPBETLDestTest.class);
	PlainPBStoragePlugin storagePlugin = new PlainPBStoragePlugin();
	PBCommonSetup setup = new PBCommonSetup();

	@Before
	public void setUp() throws Exception {
		setup.setUpRootFolder(storagePlugin, "PVETLDestTests");
	}

	@After
	public void tearDown() throws Exception {
		setup.deleteTestFolder();
	}

	private static int getSecondsBetweenEvents(PartitionGranularity partitionGranularity) throws Exception {
		switch(partitionGranularity) {
		case PARTITION_5MIN:
			return 10;
		case PARTITION_15MIN:
			return 10;
		case PARTITION_30MIN:
			return 10;
		case PARTITION_HOUR:
			return 10;
		case PARTITION_DAY:
			return 10*60;
		case PARTITION_MONTH:
			return 10*60*24;
		case PARTITION_YEAR:
			return 10*60*24*30;
		default:
			throw new Exception("If we add new partition granularitites, we need to cater to this here.");
		}
	}
	
	@Test
	public void testGetLastKnownEvent() throws Exception {
		long epochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
		ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
		for(PartitionGranularity partitionGranularity : PartitionGranularity.values()) {
			logger.debug("Testing last known event for " + partitionGranularity);
			setup.setUpRootFolder(storagePlugin, "PVETLDestTests", partitionGranularity);
			int secondsBetweenEvents = getSecondsBetweenEvents(partitionGranularity);
			String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "PVETLDestTest" + partitionGranularity;
			// getLastKnownEvent for a PV with no data should return null
			try(BasicContext context = new BasicContext()) {
				assertTrue(storagePlugin.getLastKnownEvent(context, pvName) == null);
			}
			
			for(int i = 0; i < 10000; i++) {
				// We append one event at a time and make sure that the last event is what we expect.
				YearSecondTimestamp yts = TimeUtils.convertToYearSecondTimestamp(epochSeconds);
				ArrayListEventStream testData = new ArrayListEventStream(1, new RemotableEventStreamDesc(type, pvName, yts.getYear()));
				Event lastEvent = new SimulationEvent(yts.getSecondsintoyear(), yts.getYear(), type, new ScalarValue<Double>((double) yts.getSecondsintoyear()));
				epochSeconds += secondsBetweenEvents;
				testData.add(lastEvent);
				try(BasicContext context = new BasicContext()) {
					storagePlugin.appendData(context, pvName, testData);
				}
				try(BasicContext context = new BasicContext()) {
					assertTrue(storagePlugin.getLastKnownEvent(context, pvName) != null);
					assertTrue(storagePlugin.getLastKnownEvent(context, pvName).getEpochSeconds() == lastEvent.getEpochSeconds());
				}
			}
		}
	}
}
