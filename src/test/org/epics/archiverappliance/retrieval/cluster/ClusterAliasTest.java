package org.epics.archiverappliance.retrieval.cluster;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Test data retrieval with aliasing and clustering.
 * We archive a PV with an alias and then test retrieval using the real name and the alias name.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class ClusterAliasTest {
	private static final Logger logger = LogManager.getLogger(ClusterAliasTest.class.getName());
	File persistenceFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ClusterAliasTest");
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();

	@BeforeEach
	public void setUp() throws Exception {
		if(persistenceFolder.exists()) {
			FileUtils.deleteDirectory(persistenceFolder);
		}
		persistenceFolder.mkdirs();
		siocSetup.startSIOCWithDefaultDB();
		System.getProperties().put(ConfigService.ARCHAPPL_PERSISTENCE_LAYER, "org.epics.archiverappliance.config.persistence.JDBM2Persistence");
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFolder.getPath() + File.separator + "testconfig.jdbm2");
		tomcatSetup.setUpClusterWithWebApps(this.getClass().getSimpleName(), 2);
	}

	@AfterEach
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();
		FileUtils.deleteDirectory(persistenceFolder);
	}

	@Test
	public void testSimpleArchivePV() throws Exception {
		String pvNameToArchive = "UnitTestNoNamingConvention:sinealias";
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 20, mgmtURL, 15);

		// Wait for the fields to connect using "Connected channels for the extra fields"
		PVAccessUtil.waitForPVDetail(pvNameToArchive, "Connected channels for the extra fields", "7", 10, mgmtURL, 15);

		SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 2.0);
		Thread.sleep(2*1000);
		SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 3.0);
		Thread.sleep(2*1000);
		SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 4.0);
		Thread.sleep(2*1000);
		logger.info("Done updating UnitTestNoNamingConvention:sine.HIHI");
		Thread.sleep(2*60*1000);

		String aapl0 = checkInPersistence("UnitTestNoNamingConvention:sine", 0);
		String aapl1 = checkInPersistence("UnitTestNoNamingConvention:sine", 1);
		Assertions.assertTrue(aapl0.equals(aapl1), "Expecting the same appliance identity in both typeinfos, instead it is " + aapl0 + " in cluster member 0 and " + aapl1 + " in cluster member 1");
		testRetrievalCount("UnitTestNoNamingConvention:sinealias");
		testRetrievalCount("UnitTestNoNamingConvention:sine");		 
		testRetrievalCount("UnitTestNoNamingConvention:sinealias.HIHI");
		testRetrievalCount("UnitTestNoNamingConvention:sine.HIHI");		 
	}
	
	private String checkInPersistence(String pvName, int clusterIndex) throws Exception {
		logger.info("Checking for pvtype info in persistence for cluster member " + clusterIndex);
		String persistenceFile = persistenceFolder.getPath() + File.separator + "testconfig.jdbm2";
		String persistenceFileForMember = persistenceFile.replace(".jdbm2", "_appliance" + clusterIndex + ".jdbm2");
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFileForMember);
		JDBM2Persistence persistenceLayer = new JDBM2Persistence();
		PVTypeInfo typeInfo = persistenceLayer.getTypeInfo(pvName);
		Assertions.assertTrue("Expecting the pv typeinfo to be in persistence for cluster member " + clusterIndex + typeInfo != null);
		return typeInfo.getApplianceIdentity();
	}
	
	
	/**
	 * Make sure we get some data when retriving under the given name
	 * @throws IOException
	 */
	private void testRetrievalCount(String pvName) throws IOException {
		testRetrievalCountOnServer(pvName, "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		testRetrievalCountOnServer(pvName, "http://localhost:" + (ConfigServiceForTests.RETRIEVAL_TEST_PORT+1) + "/retrieval/data/getData.raw");
	}
	
	
	private void testRetrievalCountOnServer(String pvName, String serverRetrievalURL) throws IOException { 
		 RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(serverRetrievalURL);
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
			 Assertions.assertTrue(eventCount > 0, "When asking for data using " + pvName + ", event count is 0. We got " + eventCount);
		 }
	}

}
