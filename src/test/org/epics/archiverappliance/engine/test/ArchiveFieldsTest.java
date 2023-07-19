/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

/**
 * test for meta data archiving
 * 
 * @author Luofeng Li
 * 
 */
@Tag("localEpics")
public class ArchiveFieldsTest  {
	private static Logger logger = LogManager.getLogger(ArchiveFieldsTest.class.getName());
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
		OneChannelWithMetaField();
		OneChannelWithMetaFieldWithControlPv();
	}

	/**
	 * test one pv with meta field. We must make sure the meta fields should be
	 * archived too
	 */
	private void OneChannelWithMetaField() {

		try {
			String pvName = "test_NOADEL";
			MemBufWriter myWriter = new MemBufWriter(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE);
			PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			typeInfo.addArchiveField("HIHI");
			typeInfo.addArchiveField("LOLO");
			testConfigService.updateTypeInfoForPV(pvName, typeInfo);
			testConfigService.registerPVToAppliance(pvName, testConfigService.getMyApplianceInfo());

			ArchiveEngine.archivePV(pvName, 1, SamplingMethod.MONITOR, 60, myWriter, testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null, typeInfo.getArchiveFields(), false, false);
			Thread.sleep(15*1000);
			testConfigService.getEngineContext().getChannelList().get(pvName).startUpMetaChannels();
			Thread.sleep(15*1000);			
			Assertions.assertTrue(!testConfigService.getEngineContext().getChannelList().get(pvName).metaChannelsNeedStartingUp(), "Not emough delay - metafields still need starting up");
			logger.info("Changing fields");
			SIOCSetup.caput(pvName + ".HIHI", 80);
			SIOCSetup.caput(pvName + ".LOLO", 5);
			Thread.sleep(1000);
			SIOCSetup.caput(pvName + ".HIHI", 85);
			SIOCSetup.caput(pvName + ".LOLO", 6);
			logger.info("Done changing fields");
			Thread.sleep(30000);

			int hihiNum = 0;
			int loloNUm = 0;
			int totalEvents = 0;
			for (Event e : myWriter.getCollectedSamples()) {
				DBRTimeEvent tempDBRTimeEvent = (DBRTimeEvent) e;
				String hihiVluue = tempDBRTimeEvent.getFieldValue("HIHI");
				if (hihiVluue != null) {
					hihiNum++;
				}

				String loloVluue = tempDBRTimeEvent.getFieldValue("LOLO");
				if (loloVluue != null) {
					loloNUm++;
				}
				totalEvents++;
			}
			Assertions.assertTrue(totalEvents >= 2, "We should have some events in the current samples " + totalEvents);
			Assertions.assertTrue(hihiNum >= 2, "the number of value for test_0.HIHI num is " + hihiNum
					+ " and <3 and" + "it should be >=3");
			Assertions.assertTrue(loloNUm >= 2, "the number of value for test_0.LOLO num is " + loloNUm
					+ " and <3 and" + "it should be >=3");
			Thread.sleep(3000);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}

	/**
	 * test one pv with meta field.this pv and the meta fields are controlled by
	 * another pv to start or stop archiving. We must make sure when the pv is
	 * stopped or started archiving ,all the meta field should be stopped or
	 * stated at the same time
	 */
	private void OneChannelWithMetaFieldWithControlPv() {

		try {
			String controlPVName = "test:enable0";
			SIOCSetup.caput(controlPVName, 1);
			Thread.sleep(3000);
			String[] metaFields = { "HIHI", "LOLO" };
			String pvName = "test_1";
			PVTypeInfo typeInfo = new PVTypeInfo(pvName,ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			typeInfo.setSamplingMethod(SamplingMethod.SCAN);
			typeInfo.setSamplingPeriod(60);
			typeInfo.setDataStores(new String[] {"blackhole://localhost"});
			typeInfo.setArchiveFields(metaFields);
			typeInfo.setControllingPV(controlPVName);
			testConfigService.updateTypeInfoForPV(pvName, typeInfo);
			ArchiveEngine.archivePV(pvName, 2, SamplingMethod.SCAN, 60, writer,
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null,
					controlPVName, metaFields, null, false, false);
			Thread.sleep(15*1000);
			testConfigService.getEngineContext().getChannelList().get(pvName).startUpMetaChannels();
			Thread.sleep(15*1000);			
			Assertions.assertTrue(!testConfigService.getEngineContext().getChannelList().get(pvName).metaChannelsNeedStartingUp(), "Not emough delay - metafields still need starting up");
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);

			boolean result = archiveChannel.isConnected()
					&& archiveChannel.getSampleBuffer().getCurrentSamples()
							.size() > 0;
			Assertions.assertTrue(result, pvName + "is not started successfully and it shoule be started successfully");

			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				Assertions.assertTrue(archiveChannel.isMetaPVConnected(metaFieldTemp), "the channel for " + pvNameTemp + " should be created and connected but it is not");

			}
			Thread.sleep(10000);
			SIOCSetup.caput(controlPVName, 0);
			testConfigService.getEngineContext().getWriteThead().flushBuffer();
			Thread.sleep(10000);
			archiveChannel = testConfigService.getEngineContext().getChannelList().get(pvName);
			Assertions.assertTrue(archiveChannel == null || !archiveChannel.isConnected(), pvName + "is not stopped successfully and it should be stopped successfully");
			Assertions.assertTrue(archiveChannel == null || archiveChannel.getSampleBuffer().getCurrentSamples().size() == 0, pvName + "should not have any data");

			if(archiveChannel != null) { 
				for (String metaFieldTemp : metaFields) {
					String pvNameTemp = pvName + "." + metaFieldTemp;
					Assertions.assertTrue(!archiveChannel.isMetaPVConnected(metaFieldTemp), "the channel for " + pvNameTemp
							+ " should be not connected but it is ");
				}
			}

			Thread.sleep(10000);
			SIOCSetup.caput(controlPVName, 1);
			Thread.sleep(30000);
			archiveChannel = testConfigService.getEngineContext().getChannelList().get(pvName);
			Assertions.assertTrue(archiveChannel != null, "After resuming the control channel, the archive channel for pv " + pvName + " is still null");
			boolean result3 = archiveChannel.isConnected();
			Assertions.assertTrue(result3, pvName
					+ "is not started successfully and it should be started successfully");
			
			Thread.sleep(15*1000);
			testConfigService.getEngineContext().getChannelList().get(pvName).startUpMetaChannels();
			Thread.sleep(15*1000);			
			Assertions.assertTrue(!testConfigService.getEngineContext().getChannelList().get(pvName).metaChannelsNeedStartingUp(), "Not emough delay - metafields still need starting up");

			// check meta field is not connected
			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				Assertions.assertTrue(archiveChannel.isMetaPVConnected(metaFieldTemp), "the channel for " + pvNameTemp
						+ " should be reconnected but it is  not");

			}

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}

}