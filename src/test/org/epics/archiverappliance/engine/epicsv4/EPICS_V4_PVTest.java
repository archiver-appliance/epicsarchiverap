package org.epics.archiverappliance.engine.epicsv4;



import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.engine.pv.EPICSV4.EPICS_V4_PV;
import org.epics.archiverappliance.engine.pv.EPICSV4.PVContext_EPIVCSV4;

public class EPICS_V4_PVTest extends TestCase {
	private static Logger logger = Logger.getLogger(EPICS_V4_PVTest.class.getName());
	public void testEPICSV4PV()
	{
		
		EPICS_V4_PV pv4=new EPICS_V4_PV("rf");
		try {
			pv4.start();
			Thread.sleep(10000);
			pv4.stop();
			PVContext_EPIVCSV4.destoryChannelAccess();
		} catch (Exception e) {
			// 
			logger.error("Exception", e);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// 
       new EPICS_V4_PVTest().testEPICSV4PV();
	}

}
