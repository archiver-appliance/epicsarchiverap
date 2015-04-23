
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
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.metadata.MetaCompletedListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
/**
 * test of getting meta data
 * @author Luofeng Li
 *
 */
public class MetaTest extends TestCase {
	private static Logger logger = Logger.getLogger(MetaTest.class.getName());
	private SIOCSetup ioc = null;
	private ScheduledThreadPoolExecutor scheduler;
	private ConfigServiceForTests testConfigService;

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
		singlePVMeta();
	}
/**
 * test of getting meta data for one pv.
 */
	private void singlePVMeta() {

		try {

			String metaFied[] = { "MDEL", "ADEL", "RTYP" };
			ArchiveEngine.getArchiveInfo("test_0", testConfigService, metaFied, false,
					new MetaCompletedListener() {
						@Override
						public void completed(MetaInfo metaInfo) {
							System.out.println(metaInfo.toString());
							String MDELStr = metaInfo.getOtherMetaInfo().get(
									"MDEL");
							String ADELStr = metaInfo.getOtherMetaInfo().get(
									"ADEL");
							String RTYPStr = metaInfo.getOtherMetaInfo().get(
									"RTYP");
							assertTrue("MDEL of meta data should not be null",
									MDELStr != null);
							assertTrue("ADEL of meta data should not be null",
									ADELStr != null);
							assertTrue("RTYP of meta data should not be null",
									RTYPStr != null);
						}

					});

			Thread.sleep(70000);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}
}
