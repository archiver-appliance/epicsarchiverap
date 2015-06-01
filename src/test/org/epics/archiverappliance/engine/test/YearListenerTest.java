/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
/**
 * test of year changing.
 * When year changes, we should create a new ArrayListEventStream
 * @author Luofeng Li
 *
 */
public class YearListenerTest  extends TestCase{
	private static Logger logger = Logger.getLogger(YearListenerTest.class.getName());
	private SIOCSetup ioc = null;
	private ScheduledThreadPoolExecutor scheduler;
	private DefaultConfigService testConfigService;
	private WriterTest writer = new WriterTest();

	@Before
	public void setUp() throws Exception {
		ioc = new SIOCSetup();
		ioc.startSIOCWithDefaultDB();
		scheduler = (ScheduledThreadPoolExecutor) Executors
				.newScheduledThreadPool(1);
		testConfigService = new ConfigServiceForTests(new File("./bin"));
		testConfigService.getEngineContext().setScheduler(scheduler);
		Thread.sleep(3000);
	}

	@After
	public void tearDown() throws Exception {

		testConfigService.shutdownNow();
		ioc.stopSIOC();

	}

	@Test
	public void testAll() {
		singlePvYearChangeListener();
	}
	private void singlePvYearChangeListener()
	{
		//change your time of your computer to 2011-12-31 23:58:00
		try {
			
				ArchiveEngine.archivePV("test_0", 2,
						 SamplingMethod.SCAN,
						 10,  writer,
						 testConfigService,
						 ArchDBRTypes.DBR_SCALAR_DOUBLE,
						 null, false, false);
		}catch (Exception e) {
			// 
			logger.error("Exception", e);
		}
	}
	

}
