/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.LinkedList;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

/**
 * Test retrieval across year spans when some of the data is missing. 
 * We generate data for these time periods
 * <ol>
 * <li>Sep 2011 - Oct 2011</li>
 * <li>Jun 2012 - Jul 2012</li>
 * </ol>
 * 
 * We then make requests for various time periods and check the first sample and number of samples. 
 * 
 * @author mshankar
 *
 */
public class MissingDataYearSpanRetrievalTest {
	private static Logger logger = Logger.getLogger(MissingDataYearSpanRetrievalTest.class.getName());
	String testSpecificFolder = "MissingDataYearSpanRetrieval";
	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":" + testSpecificFolder + ":mdata_yspan";
	File dataFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator +  "ArchUnitTest" + File.separator + testSpecificFolder);

	PBCommonSetup pbSetup = new PBCommonSetup();
	PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
	TomcatSetup tomcatSetup = new TomcatSetup();
	
	private LinkedList<Timestamp> generatedTimeStamps = new LinkedList<Timestamp>();

	@Before
	public void setUp() throws Exception {
		pbSetup.setUpRootFolder(pbplugin);
		logger.info("Data folder is " + dataFolder.getAbsolutePath());
		FileUtils.deleteDirectory(dataFolder);
		generateData();
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
	}
	
	private void generateData() throws IOException {
		{
			// Generate some data for Sep 2011 - Oct 2011, one per day
			Timestamp sep2011 = TimeUtils.convertFromISO8601String("2011-09-01T00:00:00.000Z");
			int sep201101secsIntoYear = TimeUtils.getSecondsIntoYear(TimeUtils.convertToEpochSeconds(sep2011));
			short year = 2011;
			ArrayListEventStream strm = new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
			for(int day = 0; day < 30; day++) { 
				strm.add(new SimulationEvent(sep201101secsIntoYear + day*86400, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));
				generatedTimeStamps.add(TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(year, sep201101secsIntoYear + day*86400, 0)));
			}
			try(BasicContext context = new BasicContext()) {
				pbplugin.appendData(context, pvName, strm);
			}
		}
		
		{
			// Generate some data for Jun 2012 - Jul 2012, one per day
			Timestamp jun2012 = TimeUtils.convertFromISO8601String("2012-06-01T00:00:00.000Z");
			int jun201201secsIntoYear = TimeUtils.getSecondsIntoYear(TimeUtils.convertToEpochSeconds(jun2012));
			short year = 2012;
			ArrayListEventStream strm = new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
			for(int day = 0; day < 30; day++) { 
				strm.add(new SimulationEvent(jun201201secsIntoYear + day*86400, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));
				generatedTimeStamps.add(TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(year, jun201201secsIntoYear + day*86400, 0)));
			}
			try(BasicContext context = new BasicContext()) {
				pbplugin.appendData(context, pvName, strm);
			}
		}
		
	}

	@After
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
		FileUtils.deleteDirectory(dataFolder);
	}

	/**
	 * <pre>
	 * .....Sep,2011.....Oct,2011..............Jan,1,2012..........Jun,2012......Jul,2012...............Dec,2012......
	 * [] - should return no data
	 * ...[.....] should return data whose first value should be Sep 1, 2011
	 * ............[.....] should return data whose first value is start time - 1
	 * .................[...........] should return data whose first value is start time - 1
	 * ...................................[.] should return one sample for the last day of Sept, 2011
	 * ...................................[...................] should return one sample for the last day of Sept, 2011
	 * ................................................[..]  should return one sample for the last day of Sept, 2011
	 * ................................................[...............]  should return may samples with the first sample as the last day of Sept, 2011
	 * ..................................................................[......]  should return may samples all from 2012
	 * ..........................................................................................[..] should return one sample for the last day of Jun, 2012
	 * ...........................................................................................................................[..] should return one sample for the last day of Jun, 2012
	 * <pre>
	 * @throws IOException
	 */
	@Test
	public void testMissingDataYearSpan()  throws Exception  {
		testRetrieval("2011-06-01T00:00:00.000Z", "2011-07-01T00:00:00.000Z", 0, null, -1, "Before all data");
		testRetrieval("2011-08-10T00:00:00.000Z", "2011-09-15T10:00:00.000Z", 15, "2011-09-01T00:00:00.000Z",  0,  "Aug/10/2011 - Sep/15/2011");
		testRetrieval("2011-09-10T00:00:00.000Z", "2011-09-15T10:00:00.000Z", 6,  "2011-09-09T00:00:00.000Z",  8,  "Sep/10/2011 - Sep/15/2011");
		testRetrieval("2011-09-10T00:00:00.000Z", "2011-10-15T10:00:00.000Z", 22, "2011-09-09T00:00:00.000Z",  8,  "Sep/10/2011 - Oct/15/2011");
		testRetrieval("2011-10-10T00:00:00.000Z", "2011-10-15T10:00:00.000Z", 1,  "2011-09-30T00:00:00.000Z",  29, "Oct/10/2011 - Oct/15/2011");
		testRetrieval("2011-10-10T00:00:00.000Z", "2012-01-15T10:00:00.000Z", 1,  "2011-09-30T00:00:00.000Z",  29, "Oct/10/2011 - Jan/15/2012");
		testRetrieval("2012-01-10T00:00:00.000Z", "2012-01-15T10:00:00.000Z", 1,  "2011-09-30T00:00:00.000Z",  29, "Jan/10/2012 - Jan/15/2012");
		testRetrieval("2012-01-10T00:00:00.000Z", "2012-06-15T10:00:00.000Z", 16, "2011-09-30T00:00:00.000Z",  29, "Jan/10/2012 - Jun/15/2012");
		testRetrieval("2012-06-10T00:00:00.000Z", "2012-06-15T10:00:00.000Z", 6,  "2012-06-09T00:00:00.000Z",  38, "Jun/10/2012 - Jun/15/2012");
		testRetrieval("2012-09-10T00:00:00.000Z", "2012-09-15T10:00:00.000Z", 1,  "2012-06-30T00:00:00.000Z",  59, "Sep/10/2012 - Sep/15/2012");
		testRetrieval("2013-01-10T00:00:00.000Z", "2013-01-15T10:00:00.000Z", 1,  "2012-06-30T00:00:00.000Z",  59, "Jan/10/2013 - Jan/15/2013");
		
//		logger.info("Try now...");
//		try { Thread.sleep(300*1000); } catch(Throwable t) {} 
//		logRetrieval("2011-10-10T00:00:00.000Z", "2011-10-15T10:00:00.000Z", new File("/tmp/Oct10.dat"));
	} 
	
	/**
	 * @param startStr - Start time of request
	 * @param endStr - End time of request
	 * @param expectedMinEventCount - How many events we expect at a minimum
	 * @param firstTimeStampExpectedStr - The time stamp of the first event
	 * @param firstTSIndex - If present, the index into generatedTimeStamps for the first event. Set to -1 if you want to skip this check.
	 * @param msg - msg to add to log msgs and the like
	 * @throws IOException
	 */
	private void testRetrieval(String startStr, String endStr, int expectedMinEventCount, String firstTimeStampExpectedStr, int firstTSIndex, String msg) throws IOException {
		Timestamp start = TimeUtils.convertFromISO8601String(startStr);
		Timestamp end = TimeUtils.convertFromISO8601String(endStr);
		Timestamp firstTimeStampExpected = null;
		if(firstTimeStampExpectedStr != null) { 
			firstTimeStampExpected = TimeUtils.convertFromISO8601String(firstTimeStampExpectedStr);
		}
		if(firstTSIndex != -1) { 
			assertTrue("Incorrect specification - Str is " + firstTimeStampExpectedStr + " and from array " + TimeUtils.convertToISO8601String(generatedTimeStamps.get(firstTSIndex)) + " for " + msg, firstTimeStampExpected.equals(generatedTimeStamps.get(firstTSIndex)));
		}
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		Timestamp obtainedFirstSample = null;
		int eventCount = 0;
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, start, end, null)) {
			if(stream != null) {
				for(Event e : stream) {
					if(obtainedFirstSample == null) { 
						obtainedFirstSample = e.getEventTimeStamp();
					}
					assertTrue("Expecting sample with timestamp " 
							+ TimeUtils.convertToISO8601String(generatedTimeStamps.get(firstTSIndex + eventCount)) 
							+ " got " 
							+ TimeUtils.convertToISO8601String(e.getEventTimeStamp())
							+ " for " + msg, e.getEventTimeStamp().equals(generatedTimeStamps.get(firstTSIndex + eventCount)));
					eventCount++;
				}
			} else { 
				logger.info("Stream is null for " + msg);
			}
		}
		
		assertTrue("Expecting at least " + expectedMinEventCount + " got " + eventCount + " for " + msg, eventCount >= expectedMinEventCount); 
		if(firstTimeStampExpected != null) { 
			if(obtainedFirstSample == null) { 
				fail("Expecting at least one value for " + msg);
			} else { 
				assertTrue("Expecting first sample to be " 
						+ TimeUtils.convertToISO8601String(firstTimeStampExpected) 
						+ " got " 
						+ TimeUtils.convertToISO8601String(obtainedFirstSample)
						+ " for " + msg, firstTimeStampExpected.equals(obtainedFirstSample));
			}
		} else { 
			if(obtainedFirstSample != null) { 
				fail("Expecting no values for " + msg + " Got value from " + TimeUtils.convertToISO8601String(obtainedFirstSample));
			}
		}
	}
	
//	private void logRetrieval(String startStr, String endStr, File dataFile) throws Exception { 
//		StringWriter buf = new StringWriter();
//		buf.append("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw")
//		.append("?pv=").append(pvName)
//		.append("&from=").append(startStr)
//		.append("&to=").append(endStr);
//		String getURL = buf.toString();
//		logger.info("URL to fetch data is " + getURL);
//		URL url = new URL(getURL);
//		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
//		connection.connect();
//		if(connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
//			try(InputStream is = new BufferedInputStream(connection.getInputStream()); OutputStream os = new FileOutputStream(dataFile)) { 
//				if(is.available() <= 0) { logger.error("Available <= 0"); }
//				byte[] databuf= new byte[1024];
//				int bytesRead = is.read(databuf);
//				while(bytesRead > 0) { 
//					os.write(databuf, 0, bytesRead);
//					bytesRead = is.read(databuf);
//				}
//			}
//		}
//	}
}
