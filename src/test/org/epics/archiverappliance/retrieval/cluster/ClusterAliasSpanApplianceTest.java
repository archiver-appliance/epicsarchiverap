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
import java.net.URLEncoder;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Test data retrieval with aliasing and clustering.
 * We archive a PV with an the real name and then add the alias into the other appliance and then test retrieval
 * @author mshankar
 *
 */
@Tag("localEpics")
@Tag("integration")
public class ClusterAliasSpanApplianceTest {
	private static final Logger logger = LogManager.getLogger(ClusterAliasSpanApplianceTest.class.getName());
	File persistenceFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ClusterAliasSpanApplianceTest");
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
        String pvNameToArchive = "UnitTestNoNamingConvention:sine";
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(mgmtURL + "/archivePV", GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive)))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);

		 String aapl0 = checkInPersistence("UnitTestNoNamingConvention:sine", 0);
		 String aapl1 = checkInPersistence("UnitTestNoNamingConvention:sine", 1);
		 Assertions.assertTrue(aapl0.equals(aapl1), "Expecting the same appliance identity in both typeinfos, instead it is " + aapl0 + " in cluster member 0 and " + aapl1 + " in cluster member 1");
		 SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 2.0);
		 Thread.sleep(2*1000);
		 SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 3.0);
		 Thread.sleep(2*1000);
		 SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 4.0);
		 Thread.sleep(2*1000);
		 logger.info("Done updating UnitTestNoNamingConvention:sine.HIHI");
		 Thread.sleep(2*60*1000);
		 
		 // We have the appliance identity in either aapl0 or aapl1
		 // Manually add an alias into the other appliance.
		 addAliasIntoOtherAppliance("UnitTestNoNamingConvention:sine", "UnitTestNoNamingConvention:sinealias", aapl0);
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
		Assertions.assertTrue(typeInfo != null, "Expecting the pv typeinfo to be in persistence for cluster member " + clusterIndex);
		return typeInfo.getApplianceIdentity();
	}
	
	
	/**
	 * Make sure we get some data when retrieving under the given name
	 * @throws IOException
	 */
	private void testRetrievalCount(String pvName) throws IOException {
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
			 Assertions.assertTrue(eventCount > 0, "When asking for data using " + pvName + ", event count is 0. We got " + eventCount);
		 }
	}
	
	
	/**
	 * Manually add an alias entry into the other appliance for this PV.
	 * @param pvName - The name of the pv with the typeinfo
	 * @param aliasName - The name of the alias
	 * @param thisAppliance - The appliance that contains the pvTypeInfo. We'll add the alias into the other appliance
	 */
	private void addAliasIntoOtherAppliance(String pvName, String aliasName, String thisAppliance) throws Exception { 
		String otherAppliance = thisAppliance.equals("appliance0") ? "appliance1" : "appliance0";
		int clusterIndex = thisAppliance.equals("appliance0") ? 1 : 0;
		logger.info("Adding alias " + aliasName + " for PV " + pvName + " in appliance " + otherAppliance);
		int port = ConfigServiceForTests.RETRIEVAL_TEST_PORT + clusterIndex;
		String addAliasURL = "http://localhost:" + port + "/mgmt/bpl/addAlias?pv=" 
				+ URLEncoder.encode(pvName, "UTF-8") 
				+ "&aliasname=" 
				+ URLEncoder.encode(aliasName, "UTF-8")
				+ "&useThisAppliance=true";
		JSONObject status = GetUrlContent.getURLContentAsJSONObject(addAliasURL);
		logger.info(status.toJSONString());
		
		// Check in persistence for the presence of the alias.
		// Aliases should ideally be stored in the appliance that contains the PVTypeinfo
		// In this case, we are intentionally skipping this constraint. 
		// So, we only check in the appliance where we ask to addAlias
		logger.info("Checking for alias in persistence for cluster member " + clusterIndex);
		String persistenceFile = persistenceFolder.getPath() + File.separator + "testconfig.jdbm2";
		String persistenceFileForMember = persistenceFile.replace(".jdbm2", "_appliance" + clusterIndex + ".jdbm2");
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFileForMember);
		JDBM2Persistence persistenceLayer = new JDBM2Persistence();
		Assertions.assertTrue(pvName.equals(persistenceLayer.getAliasNamesToRealName(aliasName)), "Alias " + aliasName + " does not have entry in persistence in cluster file " + persistenceFile);
	}	
}
