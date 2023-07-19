
/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.metadata.MetaCompletedListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;


/**
 * test of getting meta data
 * @author Luofeng Li
 *
 */
@Tag("localEpics")
public class MetaTest {
	private static Logger logger = LogManager.getLogger(MetaTest.class.getName());
	private SIOCSetup ioc = null;
	private ConfigServiceForTests testConfigService;

	@BeforeEach
	public void setUp() throws Exception {
		ioc = new SIOCSetup();
		ioc.startSIOCWithDefaultDB();
		testConfigService = new ConfigServiceForTests(new File("./bin"));
		Thread.sleep(3000);
	}

	@AfterEach
	public void tearDown() throws Exception {
		testConfigService.shutdownNow();
		ioc.stopSIOC();
	}

	@Test
	public void testAll() throws Exception {
		singlePVMeta();
		testAliasNames();
	}
/**
 * test of getting meta data for one pv.
 */
	private void singlePVMeta() {
		CountDownLatch latch = new CountDownLatch(1);

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
							Assertions.assertTrue(MDELStr != null, "MDEL of meta data should not be null");
							Assertions.assertTrue(ADELStr != null, "ADEL of meta data should not be null");
							Assertions.assertTrue(RTYPStr != null, "RTYP of meta data should not be null");
							latch.countDown();
						}

					});

			Assertions.assertTrue(latch.await(70, TimeUnit.SECONDS));

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}
	
	class AliasNames { 
		String expectedName; 
		String metaGetAliasName;
		String metaGetOtherInfoName;
		AliasNames(String expectedName) { 
			this.expectedName = expectedName;
		}
	}
	
	public void testAliasNames() {
		HashMap<String, AliasNames> aliasNames = new HashMap<String, AliasNames>();
		aliasNames.put("UnitTestNoNamingConvention:sine", new AliasNames("UnitTestNoNamingConvention:sine"));
		aliasNames.put("UnitTestNoNamingConvention:sine.DESC", new AliasNames("UnitTestNoNamingConvention:sine.DESC"));
		aliasNames.put("UnitTestNoNamingConvention:sine.HIHI", new AliasNames("UnitTestNoNamingConvention:sine.HIHI"));
		aliasNames.put("UnitTestNoNamingConvention:sinealias", new AliasNames("UnitTestNoNamingConvention:sine"));
		aliasNames.put("UnitTestNoNamingConvention:sinealias.DESC", new AliasNames("UnitTestNoNamingConvention:sine.DESC"));
		aliasNames.put("UnitTestNoNamingConvention:sinealias.HIHI", new AliasNames("UnitTestNoNamingConvention:sine.HIHI"));

		CountDownLatch latch = new CountDownLatch(6);
		testAliasNamesForPV(latch, "UnitTestNoNamingConvention:sine", aliasNames);
		testAliasNamesForPV(latch, "UnitTestNoNamingConvention:sine.DESC", aliasNames);
		testAliasNamesForPV(latch, "UnitTestNoNamingConvention:sine.HIHI", aliasNames);
		testAliasNamesForPV(latch, "UnitTestNoNamingConvention:sinealias", aliasNames);
		testAliasNamesForPV(latch, "UnitTestNoNamingConvention:sinealias.DESC", aliasNames);
		testAliasNamesForPV(latch, "UnitTestNoNamingConvention:sinealias.HIHI", aliasNames);

		try { 
			Assertions.assertTrue(latch.await(90, TimeUnit.SECONDS), "MetaGet did not complete for all PV's " + latch.getCount());
		} catch(InterruptedException ex) { 
			logger.error(ex);
		}
		
		for(String pvName : aliasNames.keySet()) {
			AliasNames aliasName = aliasNames.get(pvName);
			Assertions.assertTrue(aliasName.expectedName.equals(aliasName.metaGetAliasName), "AliasName for " + pvName + " is not " + aliasName.expectedName + ". Instead it is " + aliasName.metaGetAliasName);
			Assertions.assertTrue(aliasName.expectedName.equals(aliasName.metaGetOtherInfoName), "NAME info hashmap for " + pvName + " is not " + aliasName.expectedName + ". Instead it is " + aliasName.metaGetOtherInfoName);
		}
	}
	
	/**
	 * Test the NAME and NAME$ for various PV's and fields of PV's
	 */
	private void testAliasNamesForPV(final CountDownLatch latch, final String pvName, HashMap<String, AliasNames> aliasNames) { 
		String metaFied[] = { "MDEL", "ADEL", "RTYP" };
		try { 
			ArchiveEngine.getArchiveInfo(pvName, testConfigService, metaFied, false, new MetaCompletedListener() {
				@Override
				public void completed(MetaInfo metaInfo) {
					logger.info("Metadata completed for " + pvName + "aliasName " + metaInfo.getAliasName() + "Name: " + metaInfo.getOtherMetaInfo().get("NAME"));
					aliasNames.get(pvName).metaGetAliasName = metaInfo.getAliasName();
					aliasNames.get(pvName).metaGetOtherInfoName = metaInfo.getOtherMetaInfo().get("NAME");
					latch.countDown();
				}});
		} catch(Exception ex) {
			logger.error(ex);
			Assertions.assertTrue(false, "Exception thrown " + ex.getMessage());
		}
	}
	
	
}
