package org.epics.archiverappliance.engine.test;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import org.junit.jupiter.api.Test;

@Tag("localEpics")
public class PauseAndResumePVwithMetaFieldTest {
	private static Logger logger = LogManager.getLogger(PauseAndResumePVwithMetaFieldTest.class.getName());
	private SIOCSetup ioc = null;
	private ConfigServiceForTests testConfigService;
	private FakeWriter writer = new FakeWriter();

	@BeforeEach
	public void setUp() throws Exception {
		ioc = new SIOCSetup();
		ioc.startSIOCWithDefaultDB();
		testConfigService = new ConfigServiceForTests(new File("./bin"));
		testConfigService.getEngineContext().setDisconnectCheckTimeoutInMinutesForTestingPurposesOnly(1);
		Thread.sleep(3000);
	}

	@AfterEach
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
			Assertions.assertTrue(archiveChannel != null, "the channel for " + pvName
					+ " should be created but it is not");
			// Clear the sample buffers and then wait for part of the engine write thread period.
			archiveChannel.getSampleBuffer().getCurrentSamples().clear();
			Thread.sleep(5*1000);
			
			boolean hasData = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size() > 0;
			Assertions.assertTrue(hasData, "the channel for " + pvName
					+ " should have data but it don't");
			// check the archive field archived
			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				Assertions.assertTrue(archiveChannel.isMetaPVConnected(metaFieldTemp), "the channel for " + pvNameTemp
						+ " should be connected but it is not");

			}
			
			Assertions.assertTrue(!testConfigService.getEngineContext().getAllChannelsForPV(pvName).isEmpty(), "We should have some CAJ channels for this PV");

			
			logger.info("Before call to pausePV");
			ArchiveEngine.pauseArchivingPV(pvName, testConfigService);
			archiveChannel.getSampleBuffer().getCurrentSamples().clear();
			Thread.sleep(5000);
			// Make sure that we have closed all the channels associated with this PV
			
			Assertions.assertTrue(testConfigService.getEngineContext().getAllChannelsForPV(pvName).isEmpty(), "All CAJ channels for this PV should be closed but it is not");
			
			
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			Assertions.assertTrue(tempPVMetrics == null || !tempPVMetrics.isConnected(), "the channel for " + pvName
					+ " should be stopped but it is not");
			boolean hasData2 = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size() > 0;
			Assertions.assertTrue(!hasData2, "the channel for " + pvName
					+ " should not have data but it has");
			// check meta field
			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				Assertions.assertTrue(!archiveChannel.isMetaPVConnected(metaFieldTemp), "the channel for " + pvNameTemp
						+ " should be not connected but it is ");

			}

			logger.info("Before call to resumePV");
			ArchiveEngine.resumeArchivingPV(pvName, testConfigService);
			// We wait for many minutes as the engine creates meta field channels after a delay
			Thread.sleep((8)*60*1000);
			PVMetrics tempPVMetrics3 = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			Assertions.assertTrue(tempPVMetrics3.isConnected(), "the channel for " + pvName
					+ " should be restarted but it is not");
			archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);

			// check meta field
			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				Assertions.assertTrue(archiveChannel.isMetaPVConnected(metaFieldTemp), "the channel for " + pvNameTemp
						+ " should be reconnected but it is not");

			}

			Assertions.assertTrue(!testConfigService.getEngineContext().getAllChannelsForPV(pvName).isEmpty(), "We should have some CAJ channels for this PV");

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}
}