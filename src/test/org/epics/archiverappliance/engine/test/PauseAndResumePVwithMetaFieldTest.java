package org.epics.archiverappliance.engine.test;

import java.io.File;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class PauseAndResumePVwithMetaFieldTest extends TestCase {
	private static Logger logger = Logger.getLogger(PauseAndResumePVwithMetaFieldTest.class.getName());
	private SIOCSetup ioc = null;
	private ConfigServiceForTests testConfigService;
	private WriterTest writer = new WriterTest();

	@Before
	public void setUp() throws Exception {
		ioc = new SIOCSetup();
		ioc.startSIOCWithDefaultDB();
		testConfigService = new ConfigServiceForTests(new File("./bin"));
		testConfigService.getEngineContext().setDisconnectCheckTimeoutInMinutesForTestingPurposesOnly(1);
		Thread.sleep(3000);
	}

	@After
	public void tearDown() throws Exception {

		testConfigService.shutdownNow();
		ioc.stopSIOC();

	}

	@Test
	public void testAll() {
		pauseAndResume();
	}

	/**
	 * test of starting or stopping archiving one pv
	 */
	private void pauseAndResume() {

		String pvName = "test_2";
		try {
			String[] metaFields = { "HIHI", "LOLO" };

			PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			typeInfo.setDataStores(new String[] {"blackhole://localhost"});
			typeInfo.setArchiveFields(metaFields);
			typeInfo.setSamplingMethod(SamplingMethod.SCAN);
			typeInfo.setSamplingPeriod(1);
			testConfigService.updateTypeInfoForPV(pvName, typeInfo);
			ArchiveEngine.archivePV(pvName, 1, SamplingMethod.SCAN, 60, writer,
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null,
					metaFields, false, false);
			// We wait for many minutes as the engine creates meta field channels after a delay
			logger.info("After call to archiving PV");
			Thread.sleep((8)*60*1000);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
			assertTrue("the channel for " + pvName
					+ " should be created but it is not",
					archiveChannel != null);
			// Clear the sample buffers and then wait for part of the engine write thread period.
			archiveChannel.getSampleBuffer().getCurrentSamples().clear();
			Thread.sleep(5*1000);
			
			boolean hasData = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size() > 0;
			assertTrue("the channel for " + pvName
					+ " should have data but it don't", hasData);
			// check the archive field archived
			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				assertTrue("the channel for " + pvNameTemp
						+ " should be connected but it is not",
						archiveChannel.isMetaPVConnected(metaFieldTemp));

			}
			
			assertTrue("We should have some CAJ channels for this PV", !testConfigService.getEngineContext().getAllChannelsForPV(pvName).isEmpty());

			
			logger.info("Before call to pausePV");
			ArchiveEngine.pauseArchivingPV(pvName, testConfigService);
			archiveChannel.getSampleBuffer().getCurrentSamples().clear();
			Thread.sleep(5000);
			// Make sure that we have closed all the channels associated with this PV
			
			assertTrue("All CAJ channels for this PV should be closed but it is not", testConfigService.getEngineContext().getAllChannelsForPV(pvName).isEmpty());
			
			
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			assertTrue("the channel for " + pvName
					+ " should be stopped but it is not",
					tempPVMetrics == null || !tempPVMetrics.isConnected());
			boolean hasData2 = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size() > 0;
			assertTrue("the channel for " + pvName
					+ " should not have data but it has", !hasData2);
			// check meta field
			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				assertTrue("the channel for " + pvNameTemp
						+ " should be not connected but it is ",
						!archiveChannel.isMetaPVConnected(metaFieldTemp));

			}

			logger.info("Before call to resumePV");
			ArchiveEngine.resumeArchivingPV(pvName, testConfigService);
			// We wait for many minutes as the engine creates meta field channels after a delay
			Thread.sleep((8)*60*1000);
			PVMetrics tempPVMetrics3 = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			assertTrue("the channel for " + pvName
					+ " should be restarted but it is not",
					tempPVMetrics3.isConnected());
			archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
			boolean hasData3 = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size() > 0;
			assertTrue("the channel for " + pvName
					+ " should have data but it don't", hasData3);

			// check meta field
			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				assertTrue("the channel for " + pvNameTemp
						+ " should be reconnected but it is not",
						archiveChannel.isMetaPVConnected(metaFieldTemp));

			}

			assertTrue("We should have some CAJ channels for this PV", !testConfigService.getEngineContext().getAllChannelsForPV(pvName).isEmpty());

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}
}