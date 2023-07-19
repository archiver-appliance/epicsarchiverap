/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;


/**
 * test of conditional archiving
 * @author Luofeng Li
 *
 */
@Tag("localEpics")
public class ControlledPVTest {
	private static Logger logger = LogManager.getLogger(ControlledPVTest.class.getName());
            private SIOCSetup ioc=null;
            private ConfigServiceForTests  testConfigService;
            private FakeWriter writer= new FakeWriter();
            @BeforeEach
              public void setUp() throws Exception {
                    ioc=new SIOCSetup();
                    ioc.startSIOCWithDefaultDB();
                    testConfigService=new ConfigServiceForTests(new File("./bin"));
                    Thread.sleep(10000);
             }
              @AfterEach
        public void tearDown() throws Exception {
            	  testConfigService.shutdownNow();
            	  ioc.stopSIOC();
         }
        @Test 
        public void testAll()
        {
                 controlledPV1000pvs();
        }
   /**
    * test of creating channels for 1000 pvs , controlled by one pv, to start archiving or stop   
    */
        private void controlledPV1000pvs()
    {

        
           
            try {
                 
                  SIOCSetup.caput("test:enable0", 0);
                  Thread.sleep(2000);
                  SIOCSetup.caput("test:enable1", 1);
                  Thread.sleep(2000);
                    for(int i=0;i<1000;i++)
                    {
               
                            String pvnameenable="";
                            if(i<600) 
                            {
                                    pvnameenable="test:enable0";
                            }
                            else
                            {
                                    pvnameenable="test:enable1";
                            }
                            
                            String pvName = "test_"+i;
							ArchiveEngine.archivePV(pvName, 2,
                                             SamplingMethod.SCAN,
                                             60,  writer,
                                             testConfigService,
                                             ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                             null,pvnameenable, false, false);
							testConfigService.updateTypeInfoForPV(pvName, new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1));
                         
                    }
                   
                    Thread.sleep(5000);
                    ConcurrentHashMap<String, ArchiveChannel> channelList=testConfigService.getEngineContext().getChannelList();
                    int enablePVs=0;
                    for(String pvName:channelList.keySet())
                    {
                            ArchiveChannel tempChannel=channelList.get(pvName);
                            if (tempChannel.isEnabled()) enablePVs++;
                    }
                    
                    Assertions.assertTrue(enablePVs==400, "400 channels should be started ,but only "+enablePVs+" were started");
                    Assertions.assertTrue(!channelList.get("test_0").getPVMetrics().isConnected(), "test_0 should be unconnected but it is connected");
                    Assertions.assertTrue(!channelList.get("test_500").getPVMetrics().isConnected(), "test_500 should be unconnected but it is connected");
                    Assertions.assertTrue(channelList.get("test_700").getPVMetrics().isConnected(), "test_700 should be connected but it is unconnected");
                    Assertions.assertTrue(channelList.get("test_900").getPVMetrics().isConnected(), "test_900 should be connected but it is unconnected");
                    SIOCSetup.caput("test:enable0", 1);
                    Thread.sleep(2000);
                    int enablePVs2=0;
                    for(String pvName:channelList.keySet())
                    {
                            ArchiveChannel tempChannel=channelList.get(pvName);
                            if (tempChannel.isEnabled()) enablePVs2++;
                    }
                    
                    Assertions.assertTrue(enablePVs2==1000, "1000 channels should be started ,but only "+enablePVs2+" were started");
                    Assertions.assertTrue(channelList.get("test_0").getPVMetrics().isConnected(), "test_0 should be connected but it is unconnected");
                    Assertions.assertTrue(channelList.get("test_500").getPVMetrics().isConnected(), "test_500 should be connected but it is unconnected");
                    Assertions.assertTrue(channelList.get("test_700").getPVMetrics().isConnected(), "test_700 should be connected but it is unconnected");
                    Assertions.assertTrue(channelList.get("test_900").getPVMetrics().isConnected(), "test_900 should be connected but it is unconnected");
                    
                    
                    SIOCSetup.caput("test:enable1", 0);
                    Thread.sleep(2000);
                    int disablePVs3=0;
                    for(String pvName:channelList.keySet())
                    {
                            ArchiveChannel tempChannel=channelList.get(pvName);
                            if (!tempChannel.isEnabled()) disablePVs3++;
                    }
                    
                    Assertions.assertTrue(disablePVs3==400, "400 channels should be stopped ,but only "+disablePVs3+" were stopeed");
                    Assertions.assertTrue(channelList.get("test_0").getPVMetrics().isConnected(), "test_0 should be connected but it is unconnected");
                    Assertions.assertTrue(channelList.get("test_500").getPVMetrics().isConnected(), "test_500 should be connected but it is unconnected");
                    Assertions.assertTrue(!channelList.get("test_700").getPVMetrics().isConnected(), "test_700 should be unconnected but it is connected");
                    Assertions.assertTrue(!channelList.get("test_900").getPVMetrics().isConnected(), "test_900 should be unconnected but it is connected");
                    
                    
                    SIOCSetup.caput("test:enable0", 0);
                    Thread.sleep(2000);
                    int disablePVs4=0;
                    for(String pvName:channelList.keySet())
                    {
                            ArchiveChannel tempChannel=channelList.get(pvName);
                            if (!tempChannel.isEnabled()) disablePVs4++;
                    }
                    
                    Assertions.assertTrue(disablePVs4==1000, "1000 channels should be stopped ,but only "+disablePVs4+" were stopeed");
                    Assertions.assertTrue(!channelList.get("test_0").getPVMetrics().isConnected(), "test_0 should be unconnected but it is connected");
                    Assertions.assertTrue(!channelList.get("test_500").getPVMetrics().isConnected(), "test_500 should be unconnected but it is connected");
                    Assertions.assertTrue(!channelList.get("test_700").getPVMetrics().isConnected(), "test_700 should be unconnected but it is connected");
                    Assertions.assertTrue(!channelList.get("test_900").getPVMetrics().isConnected(), "test_900 should be unconnected but it is connected");
                    
                                                     
            } catch (Exception e) {
                    // 
                    logger.error("Exception", e);
            }
    }

                
        

}