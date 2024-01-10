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
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * test of year changing.
 * When year changes, we should create a new ArrayListEventStream
 * @author Luofeng Li
 */
@Tag("localEpics")
public class YearListenerTest {
	private static final Logger logger = LogManager.getLogger(YearListenerTest.class.getName());
    private final String pvPrefix = YearListenerTest.class.getSimpleName();
    private SIOCSetup ioc = null;
	private DefaultConfigService testConfigService;
	private final FakeWriter writer = new FakeWriter();

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

    @Test
    public void singlePvYearChangeListener() {
        //change your time of your computer to 2011-12-31 23:58:00
        try {

            ArchiveEngine.archivePV(pvPrefix + "test_0", 2,
                    SamplingMethod.SCAN,
                    writer,
                    testConfigService,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    null, false, false);
        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }


}
