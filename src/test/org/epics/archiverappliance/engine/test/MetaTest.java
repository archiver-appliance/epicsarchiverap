
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
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * test of getting meta data
 *
 * @author Luofeng Li
 *
 */
@Tag("localEpics")
public class MetaTest {
    private static final Logger logger = LogManager.getLogger(MetaTest.class.getName());
    private static final String pvPrefix = MetaTest.class.getSimpleName();
    private static SIOCSetup ioc = null;
    private static ConfigServiceForTests testConfigService;

    @BeforeAll
    public static void setUp() throws Exception {
        ioc = new SIOCSetup(pvPrefix);
        ioc.startSIOCWithDefaultDB();
        testConfigService = new ConfigServiceForTests(-1);
        Thread.sleep(3000);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        testConfigService.shutdownNow();
        ioc.stopSIOC();
    }


    /**
     * test of getting meta data for one pv.
     */
    @Test
    public void singlePVMeta() {
        CountDownLatch latch = new CountDownLatch(1);

        try {

            String[] metaFied = {"MDEL", "ADEL", "RTYP"};
            ArchiveEngine.getArchiveInfo(pvPrefix + "test_0", testConfigService, metaFied, false,
		            metaInfo -> {
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
		            });

			Assertions.assertTrue(latch.await(70, TimeUnit.SECONDS));

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    public void testAliasNames(boolean usePVAccess) {
        HashMap<String, AliasNames> aliasNames = new HashMap<String, AliasNames>();
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sine", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine"));
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sine.DESC", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine.DESC"));
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sine.HIHI", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine.HIHI"));
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sinealias", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine"));
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sinealias.DESC", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine.DESC"));
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sinealias.HIHI", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine.HIHI"));

        CountDownLatch latch = new CountDownLatch(6);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sine", aliasNames, usePVAccess);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sine.DESC", aliasNames, usePVAccess);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sine.HIHI", aliasNames, usePVAccess);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sinealias", aliasNames, usePVAccess);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sinealias.DESC", aliasNames, usePVAccess);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sinealias.HIHI", aliasNames, usePVAccess);

        try {
            Assertions.assertTrue(latch.await(90, TimeUnit.SECONDS), "MetaGet did not complete for all PV's " + latch.getCount());
        } catch(InterruptedException ex) {
            logger.error(ex);
        }

        for(String pvName : aliasNames.keySet()) {
            AliasNames aliasName = aliasNames.get(pvName);
            Assertions.assertEquals(aliasName.expectedName, aliasName.metaGetAliasName, "AliasName for " + pvName + " is not " + aliasName.expectedName + ". Instead it is " + aliasName.metaGetAliasName);
            Assertions.assertEquals(aliasName.expectedName, aliasName.metaGetOtherInfoName, "NAME info hashmap for " + pvName + " is not " + aliasName.expectedName + ". Instead it is " + aliasName.metaGetOtherInfoName);
        }
    }

    /**
     * Test the NAME and NAME$ for various PV's and fields of PV's
     */
    private void testAliasNamesForPV(final CountDownLatch latch, final String pvName, HashMap<String, AliasNames> aliasNames, boolean usePVAccess) {
        String[] metaFied = {"MDEL", "ADEL", "RTYP"};
        try {
            ArchiveEngine.getArchiveInfo(pvName, testConfigService, metaFied, usePVAccess, metaInfo -> {
                logger.info("Metadata completed for " + pvName + "aliasName " + metaInfo.getAliasName() + "Name: " + metaInfo.getOtherMetaInfo().get("NAME"));
                aliasNames.get(pvName).metaGetAliasName = metaInfo.getAliasName();
                aliasNames.get(pvName).metaGetOtherInfoName = metaInfo.getOtherMetaInfo().get("NAME");
                latch.countDown();
            });
        } catch (Exception ex) {
            logger.error(ex);
            Assertions.fail("Exception thrown " + ex.getMessage());
        }
    }

    static class AliasNames {
        String expectedName;
        String metaGetAliasName;
        String metaGetOtherInfoName;

        AliasNames(String expectedName) {
            this.expectedName = expectedName;
        }
    }

}
