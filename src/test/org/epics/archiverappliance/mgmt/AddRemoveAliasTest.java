package org.epics.archiverappliance.mgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Check addAlias and removeAlias functionality.
 * We test after the PV workflow is complete. 
 *  
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class AddRemoveAliasTest {
	private static Logger logger = LogManager.getLogger(AddRemoveAliasTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();

	@BeforeEach
	public void setUp() throws Exception {
		siocSetup.startSIOCWithDefaultDB();
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
	}

	@AfterEach
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();
	}

	@Test
	public void testSimpleArchivePV() throws Exception {
        String pvNameToArchive = "UnitTestNoNamingConvention:sine";
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);
		 
		SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 2.0);
		Thread.sleep(2*1000);
		SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 3.0);
		Thread.sleep(2*1000);
		SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 4.0);
		Thread.sleep(2*1000);
		logger.info("Done updating UnitTestNoNamingConvention:sine.HIHI");
		Thread.sleep(2*60*1000);
		
		// Test retrieval of data using the real name and the aliased name
		testRetrievalCount("UnitTestNoNamingConvention:sine", true);
		testRetrievalCount("UnitTestNoNamingConvention:arandomalias", false);
		testRetrievalCount("UnitTestNoNamingConvention:sine.HIHI", true);
		testRetrievalCount("UnitTestNoNamingConvention:arandomalias.HIHI", false);
		
		JSONAware addAliasStatus = GetUrlContent.getURLContentWithQueryParameters(
			mgmtURL + "addAlias", 
			Map.of(
				"pv", pvNameToArchive, 
				"aliasname", "UnitTestNoNamingConvention:arandomalias"
			), 
			false
		);

		logger.debug("Add alias response " + addAliasStatus.toJSONString());
		
		Thread.sleep(2*1000);
		testRetrievalCount("UnitTestNoNamingConvention:sine", true);
		testRetrievalCount("UnitTestNoNamingConvention:arandomalias", true);
		testRetrievalCount("UnitTestNoNamingConvention:sine.HIHI", true);
		testRetrievalCount("UnitTestNoNamingConvention:arandomalias.HIHI", true);
		
		JSONAware removeAliasStatus = GetUrlContent.getURLContentWithQueryParameters(
			mgmtURL + "removeAlias", 
			Map.of(
				"pv", pvNameToArchive, 
				"aliasname", "UnitTestNoNamingConvention:arandomalias"
			), 
			false
		);
		logger.debug("Remove alias response " + removeAliasStatus.toJSONString());
		
		Thread.sleep(2*1000);
		testRetrievalCount("UnitTestNoNamingConvention:sine", true);
		testRetrievalCount("UnitTestNoNamingConvention:arandomalias", false);
		testRetrievalCount("UnitTestNoNamingConvention:sine.HIHI", true);
		testRetrievalCount("UnitTestNoNamingConvention:arandomalias.HIHI", false);
	}

	/**
	 * Make sure we get some data when retriving under the given name
	 * @param pvName
	 * @param expectingData - true if we are expecting any data at all.
	 * @throws IOException
	 */
	private void testRetrievalCount(String pvName, boolean expectingData) throws IOException {
		 RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		Instant end = TimeUtils.plusDays(TimeUtils.now(), 3);
		Instant start = TimeUtils.minusDays(end, 6);
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName}, start, end, null)) {
			 long previousEpochSeconds = 0;
			 int eventCount = 0;

			 // We are making sure that the stream we get back has times in sequential order...
			 if(stream != null) {
				 for(Event e : stream) {
					 long actualSeconds = e.getEpochSeconds();
					 Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
					 previousEpochSeconds = actualSeconds;
					 eventCount++;
				 }
			 }

			 logger.info("Got " + eventCount + " event for pv " + pvName);
			 if(expectingData) { 
				 Assertions.assertTrue(eventCount > 0, "When asking for data using " + pvName + ", event count is 0. We got " + eventCount);
			 } else { 
				 Assertions.assertTrue(eventCount == 0, "When asking for data using " + pvName + ", event count is 0. We got " + eventCount);
			 }
		 }
	}
}
