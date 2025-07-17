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
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * test for changing archiving parameters of pvs.
 * @author Luofeng Li
 *
 */
@Tag("localEpics")
public class ChangeArchivalParametersTest {
    private static Logger logger = LogManager.getLogger(ChangeArchivalParametersTest.class.getName());
    private static final String pvPrefix =
            ChangeArchivalParametersTest.class.getSimpleName().substring(0, 10);
    private SIOCSetup ioc = null;
    private ConfigServiceForTests testConfigService;

    @BeforeEach
    public void setUp() throws Exception {
        ioc = new SIOCSetup(pvPrefix);
        ioc.startSIOCWithDefaultDB();
        testConfigService = new ConfigServiceForTests(-1);
        Thread.sleep(3000);
    }

    @AfterEach
    public void tearDown() throws Exception {

        testConfigService.shutdownNow();
        ioc.stopSIOC();
    }

    /**
     * test of changing one pv from scan mode to scan mode ,but with a different sample period
     */
    @Test
    public void changeArchivalParametersFromScanToScan() {

        String pvName = pvPrefix + "test_0";
        MemBufWriter writer = new MemBufWriter(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE);

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

            Thread.sleep(5000);

            ArchiveEngine.changeArchivalParameters(
                    pvName, 8, SamplingMethod.SCAN, testConfigService, writer, false, false);

            Thread.sleep(11000);

            // ArchiveChannel
            // archiveChannel=testConfigService.getEngineContext().getChannelList().get(pvName);
            PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName, testConfigService);
            double period = tempPVMetrics.getSamplingPeriod();
            boolean isMonitor = tempPVMetrics.isMonitor();
            Assertions.assertTrue(
                    !isMonitor, "the " + pvName + " should be archived in scan mode but it is monitor mode");
            Assertions.assertTrue((period - 8) == 0, "the new sample period is " + period + " that is not 8");

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
        ArchiveChannel archiveChannel =
                testConfigService.getEngineContext().getChannelList().get(pvName);
        int valueNumber = archiveChannel.getSampleBuffer().getCurrentSamples().size();
        try {
            valueNumber =
                    valueNumber + writer.getCollectedSamples().stream().toList().size();

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
        Assertions.assertTrue(valueNumber > 0, "there is no data in sample buffer");
    }

    /**
     * test of changing pv from scan mode to monitor mode
     */
    @Test
    public void changeArchivalParametersFromScanToMonitor() {

        String pvName = pvPrefix + "test_1";
        MemBufWriter writer = new MemBufWriter(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE);
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

            Thread.sleep(5000);

            ArchiveEngine.changeArchivalParameters(
                    pvName, 0.1F, SamplingMethod.MONITOR, testConfigService, writer, false, false);

            Thread.sleep(5000);

            PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName, testConfigService);
            boolean isMonitor = tempPVMetrics.isMonitor();
            Assertions.assertTrue(
                    isMonitor, "the " + pvName + " should be archived in monitor mode but it is scan mode");
            ArchiveChannel archiveChannel =
                    testConfigService.getEngineContext().getChannelList().get(pvName);
            int valueNumber =
                    archiveChannel.getSampleBuffer().getCurrentSamples().size();
            valueNumber =
                    valueNumber + writer.getCollectedSamples().stream().toList().size();
            Assertions.assertTrue(valueNumber > 0, "there is no data in sample buffer");

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }

    /**
     * test of changing pv from monitor mode to scan mode
     */
    @Test
    public void changeArchivalParametersFromMonitorToScan() {

        String pvName = pvPrefix + "test_2";
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
            ArchiveEngine.changeArchivalParameters(
                    pvName, 2, SamplingMethod.SCAN, testConfigService, writer, false, false);
            Thread.sleep(5000);
            PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName, testConfigService);
            boolean isMonitor = tempPVMetrics.isMonitor();
            Assertions.assertTrue(
                    !isMonitor, "the " + pvName + " should be archived in scan mode but it is monitor mode");
            ArchiveChannel archiveChannel =
                    testConfigService.getEngineContext().getChannelList().get(pvName);
            int valueNumber =
                    archiveChannel.getSampleBuffer().getCurrentSamples().size();
            valueNumber =
                    valueNumber + writer.getCollectedSamples().stream().toList().size();
            Assertions.assertTrue(valueNumber > 0, "there is no data in sample buffer");

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }

    /**
     * test of  changing pv from monitor mode to monitor mode
     */
    @Test
    public void changeArchivalParametersFromMonitorToMonitor() {

        String pvName = pvPrefix + "test_3";
        MemBufWriter writer = new MemBufWriter(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE);
        try {

            ArchiveEngine.archivePV(
                    pvName,
                    2,
                    SamplingMethod.MONITOR,
                    writer,
                    testConfigService,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    null,
                    false,
                    false);
            ArchiveEngine.changeArchivalParameters(
                    pvName, 0.1F, SamplingMethod.MONITOR, testConfigService, writer, false, false);
            Thread.sleep(5000);
            ArchiveEngine.changeArchivalParameters(
                    pvName, 2, SamplingMethod.MONITOR, testConfigService, writer, false, false);
            Thread.sleep(5000);
            PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName, testConfigService);
            boolean isMonitor = tempPVMetrics.isMonitor();
            Assertions.assertTrue(
                    isMonitor, "the " + pvName + " should be archived in monitor mode but it is scan mode");
            ArchiveChannel archiveChannel =
                    testConfigService.getEngineContext().getChannelList().get(pvName);
            int valueNumber =
                    archiveChannel.getSampleBuffer().getCurrentSamples().size();
            valueNumber =
                    valueNumber + writer.getCollectedSamples().stream().toList().size();
            Assertions.assertTrue(valueNumber > 0, "there is no data in sample buffer");

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }
}
