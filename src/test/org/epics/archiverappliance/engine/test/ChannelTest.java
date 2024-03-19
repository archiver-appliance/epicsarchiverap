

/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * test of creating channels
 * @author Luofeng Li
 *
 */
@Tag("localEpics")
public class ChannelTest {
    private static final Logger logger = LogManager.getLogger(ChannelTest.class.getName());
    private static final String pvPrefix = ChannelTest.class.getSimpleName();
    private static SIOCSetup ioc = null;
    private static ConfigServiceForTests testConfigService;

    @BeforeAll
    public static void setUp() {
        ioc = new SIOCSetup(pvPrefix);
        try {
            ioc.startSIOCWithDefaultDB();
	        testConfigService = new ConfigServiceForTests(-1);
            Thread.sleep(3000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {

        testConfigService.shutdownNow();
        ioc.stopSIOC();
    }

    /**
     * test of creating the channel for the pv in scan mode
     */
    @Test
    public void singleScanChannel() {

        String pvName = pvPrefix + "test_0";
        MemBufWriter writer = new MemBufWriter(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE);

        try {
            ArchiveEngine.archivePV(
                    pvName,
                    1,
                    SamplingMethod.SCAN,
                    writer,
                    testConfigService,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    null,
                    false,
                    false);

            Thread.sleep(6000);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
            Assertions.assertNotNull(archiveChannel, "the channel for " + pvName
                    + " should be created but it is not");
            boolean hasData =
                    !writer.getCollectedSamples().isEmpty();
            Assertions.assertTrue(hasData, "the channel for " + pvName
                    + " should have data but it don't");
			
			ArchiveEngine.destoryPv(pvName, testConfigService);

		} catch (Exception e) {
			logger.error("Exception", e);
		}
	}
    
    /**
     * test of creating the channel for the pv in monitor mode
     */
    @Test
    public void singleMonitorChannel() {
        String pvName = pvPrefix + "test_1";
        MemBufWriter writer = new MemBufWriter(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE);

        try {
            ArchiveEngine.archivePV(
                    pvName,
                    0.1F,
                    SamplingMethod.MONITOR,
                    writer,
                    testConfigService,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    null,
                    false,
                    false);
			Thread.sleep(5000);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
            Assertions.assertNotNull(archiveChannel, "the channel for " + pvName
                    + " should be created but it is not");
            boolean hasData =
                    !writer.getCollectedSamples().isEmpty();
            Assertions.assertTrue(hasData, "the channel for " + pvName
                    + " should have data but it don't");

			ArchiveEngine.destoryPv(pvName, testConfigService);
		} catch (Exception e) {
			logger.error("Exception", e);
		}
	}
    
    /**
     * test of starting or stopping archiving one pv
     */
    @Test
    public void stopAndRestartChannel() {

        String pvName = pvPrefix + ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":test_2";
        MemBufWriter writer = new MemBufWriter(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE);
        
		try {

            PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
            typeInfo.setSamplingMethod(SamplingMethod.SCAN);
            typeInfo.setSamplingPeriod(1);
            typeInfo.setDataStores(new String[]{"blackhole://localhost"});
            testConfigService.updateTypeInfoForPV(pvName, typeInfo);

            ArchiveEngine.archivePV(
                    pvName,
                    0.1F,
                    SamplingMethod.SCAN,
                    writer,
                    testConfigService,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    null,
                    false,
                    false);
			Thread.sleep(2000);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
            Assertions.assertNotNull(archiveChannel, "the channel for " + pvName
                    + " should be created but it is not");
            boolean hasData =
                    !writer.getCollectedSamples().isEmpty();
            Assertions.assertTrue(hasData, "the channel for " + pvName
                    + " should have data but it don't");
			ArchiveEngine.pauseArchivingPV(pvName, testConfigService);
			Thread.sleep(2000);
			archiveChannel.getSampleBuffer().getCurrentSamples().clear();
            writer.clear();
            Thread.sleep(2000);
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName, testConfigService);
            Assertions.assertTrue(tempPVMetrics == null || !tempPVMetrics.isConnected(), "the channel for " + pvName
                    + " should be stopped but it is not");
            boolean hasData2 = !archiveChannel.getSampleBuffer()
                    .getCurrentSamples().isEmpty();
            Assertions.assertFalse(hasData2, "the channel for " + pvName
                    + " should not have data but it has");

            ArchiveEngine.resumeArchivingPV(pvName, testConfigService, writer);
            Thread.sleep(12000);
			PVMetrics tempPVMetrics3 = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
            Assertions.assertTrue(tempPVMetrics3.isConnected(), "the channel for " + pvName
                    + " should be restarted but it is not");
			archiveChannel = testConfigService.getEngineContext().getChannelList().get(pvName);
            boolean hasData3 =
                    !writer.getCollectedSamples().isEmpty();
            Assertions.assertTrue(hasData3, "the channel for " + pvName
                    + " should have data but it don't");

        } catch (Exception e) {
            Assertions.fail(e.getMessage());
            logger.error("Exception", e);
        }
    }

    /**
     * test of creating channels for 1000 pvs in scan mode
     */
    @Test
    public void create1000ScanChannel() {
        FakeWriter writer = new FakeWriter();
        try {
            int startInt = 1000;
            int nOfPVs = 1000;
            List<String> pvNames = new ArrayList<>();

            for (int m = startInt; m < startInt + nOfPVs; m++) {
                pvNames.add(pvPrefix + "test_" + m);
            }
            for (String pvName : pvNames) {

                ArchiveEngine.archivePV(
                        pvName,
                        0.1F,
                        SamplingMethod.SCAN,
                        writer,
                        testConfigService,
                        ArchDBRTypes.DBR_SCALAR_DOUBLE,
                        null,
                        false,
                        false);
                Thread.sleep(10);
            }
            Thread.sleep(2000);
            int num = 0;
            for (String pvName : pvNames) {
                PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName, testConfigService);
                if (tempPVMetrics.isConnected()) num++;
            }
            Assertions.assertEquals(nOfPVs, num, "Only " + num + " of 1000 of channels in scan mode connected successfully");
            for (String pvName : pvNames) {
                ArchiveEngine.destoryPv(pvName, testConfigService);
            }

        } catch (Exception e) {
            logger.error("Exception", e);
        }
    }

    /**
     * test of creating channels for 1000 pvs in monitor mode
     */
    @Test
    public void create1000MonitorChannel() {
        FakeWriter writer = new FakeWriter();

        try {
            int startInt = 3000;
            int nOfPVs = 1000;
            List<String> pvNames = new ArrayList<>();

            for (int m = startInt; m < startInt + nOfPVs; m++) {
                pvNames.add(pvPrefix + "test_" + m);
            }
            for (String pvName : pvNames) {
                ArchiveEngine.archivePV(
                        pvName,
                        0.1F,
                        SamplingMethod.MONITOR,
                        writer,
                        testConfigService,
                        ArchDBRTypes.DBR_SCALAR_DOUBLE,
                        null,
                        false,
                        false);
                Thread.sleep(10);
            }

            Thread.sleep(2000);
            int num = 0;
            for (String pvName : pvNames) {
                PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName, testConfigService);
                if (tempPVMetrics.isConnected()) num++;
            }
            Assertions.assertEquals(nOfPVs, num, "Only " + num + " of 1000 of channels in scan mode connected successfully");

            for (String pvName : pvNames) {
                ArchiveEngine.destoryPv(pvName, testConfigService);
            }
        } catch (Exception e) {
            logger.error("Exception", e);
        }
    }

    /**
     * test of getting pv combined data of previous and current ArrayListEventStream
     */
    @Test
    public void getPVdata() {
        FakeWriter writer = new FakeWriter();

        String pvName = pvPrefix + "test_5001";
        try {

            ArchiveEngine.archivePV(
                    pvName,
                    2,
                    SamplingMethod.SCAN,
                    writer,
                    testConfigService,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    null,
                    false,
                    false);
            Thread.sleep(2000);
            ArrayListEventStream samples = testConfigService
                    .getEngineContext()
                    .getChannelList()
                    .get(pvName)
                    .getPVData();

            Assertions.assertTrue(!samples.isEmpty(), "there is no data in sample buffer");
			ArchiveEngine.destoryPv(pvName, testConfigService);
		} catch (Exception e) {
			logger.error("Exception", e);
		}
	}
}
