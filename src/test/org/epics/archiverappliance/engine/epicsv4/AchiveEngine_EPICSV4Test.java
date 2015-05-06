package org.epics.archiverappliance.engine.epicsv4;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.pv.EPICSV4.ArchiveEngine_EPICSV4;
import org.epics.archiverappliance.engine.pv.EPICSV4.EngineContext_EPICSV4;
import org.epics.archiverappliance.engine.test.WriterTest;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;

public class AchiveEngine_EPICSV4Test extends TestCase{
	private static Logger logger = Logger.getLogger(AchiveEngine_EPICSV4Test.class.getName());
	
	
	public void testSingleScanChannel() throws Exception
	{
		
			//EpicsDemo11
			//ArchiveEngine.createScannedChannel("EpicsDemo11", 0.5F, 10);
			
			ScheduledThreadPoolExecutor scheduler =(ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(1);
			ConfigServiceForTests  testConfigService=new ConfigServiceForTests(new File("./bin"));
		
			//scheduler.execute(new MainRunnableTest(scheduler));
			
			try {
				EngineContext_EPICSV4.getInstance(testConfigService).setScheduler(scheduler);
				// PVContext.getContext().attachCurrentThread();
				 WriterTest writer= new WriterTest();
				 System.out.println(Thread.currentThread());
					
				
				 //TrainIoc:test
					//TrainIoc:test100  EpicsDemo66666666
				 ArchiveEngine_EPICSV4.archivePV("rf", 2,
							 SamplingMethod.SCAN,
							 5,  writer,
							 testConfigService,
							 ArchDBRTypes.DBR_V4_GENERIC_BYTES );
				
				//ArchiveEngine.createScannedChannel("EpicsDemo11", 2, 10);//EpicsDemo66666666
			} catch (Exception e) {
				// 
				logger.error("Exception", e);
			}
			}
	
	
	
	
	public void testSingleMonitorChannel() throws Exception
	{
		
			//EpicsDemo11
			//ArchiveEngine.createScannedChannel("EpicsDemo11", 0.5F, 10);
			
			ScheduledThreadPoolExecutor scheduler =(ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(1);
			ConfigServiceForTests  testConfigService=new ConfigServiceForTests(new File("./bin"));
		
			//scheduler.execute(new MainRunnableTest(scheduler));
			
			try {
				EngineContext_EPICSV4.getInstance(testConfigService).setScheduler(scheduler);
				// PVContext.getContext().attachCurrentThread();
				 WriterTest writer= new WriterTest();
				 System.out.println(Thread.currentThread());
					
				
				 //TrainIoc:test
					//TrainIoc:test100  EpicsDemo66666666
				 ArchiveEngine_EPICSV4.archivePV("rf", 2,
							 SamplingMethod.MONITOR,
							 5,  writer,
							 testConfigService,
							 ArchDBRTypes.DBR_V4_GENERIC_BYTES );
				
				//ArchiveEngine.createScannedChannel("EpicsDemo11", 2, 10);//EpicsDemo66666666
			} catch (Exception e) {
				// 
				logger.error("Exception", e);
			}
			}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args)  throws Exception {
		// 
        new AchiveEngine_EPICSV4Test().testSingleScanChannel();
	}

}
