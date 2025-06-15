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
import java.util.stream.Collectors;

/**
 * This relates to issue - https://github.com/slacmshankar/epicsarchiverap/issues/69
 * We want to create a policy that has extra fields that are not in getFieldsArchivedAsPartOfStream
 * When archiving PV's with this policy, we should check
 * <ol>
 * <li>Fields that are part of getFieldsArchivedAsPartOfStream should be in the .VAL's archiveFields</li>
 * <li>Fields that are NOT part of getFieldsArchivedAsPartOfStream should NOT be in the .VAL's archiveFields</li>
 * <li>Fields that are NOT part of getFieldsArchivedAsPartOfStream should have separate PVTypeInfo's.</li>
 * <li>Just to make things interesting, let's throw in EPICS aliases as well.</li>
 * </ol>
 * 
 * The best RTYP to use test this is the MOTOR record; however this has a lot of dependencies. 
 * So, we approximate this using a couple of CALC records in the UnitTestPVs
 * If we <code>caput ArchUnitTest:fieldtst:cnt 0.0</code>, we should see...
 * <pre><code>
 * $ camonitor ArchUnitTest:fieldtst ArchUnitTest:fieldtst.C
 * ArchUnitTest:fieldtst          2018-11-14 15:36:26.730758 0  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:26.730758 3.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:26.730758 0  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:57.730752 0  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:57.730752 0.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:58.730694 0.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:58.730694 1  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:59.730766 1  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:36:59.730766 1.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:00.730725 1.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:00.730725 2  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:01.730730 2  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:01.730730 2.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:02.730723 2.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:02.730723 3  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:03.730723 3  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:03.730723 3.5  
 * ArchUnitTest:fieldtst.C        2018-11-14 15:37:04.730761 3.5  
 * </code></pre>
 * Rather unfortunate about the duplicate timestamps; but we should at least be able to test to make sure we capture all the data.
 * 
 * So, we archive ArchUnitTest:fieldtstalias from within a special policies file which add the .C archiveField
 * Make sure all the listed conditions are true for ArchUnitTest:fieldtst and ArchUnitTest:fieldtst.C. 
 * Make sure we can get the data for ArchUnitTest:fieldtst and ArchUnitTest:fieldtst.C (and for the alias as well).
 * 
 *   
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class ArchiveFieldsNotInStreamTest {
	private static Logger logger = LogManager.getLogger(ArchiveFieldsNotInStreamTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();

	@BeforeEach
	public void setUp() throws Exception {
		System.getProperties().put("ARCHAPPL_POLICIES", System.getProperty("user.dir") + "/src/test/org/epics/archiverappliance/mgmt/ArchiveFieldsNotInStream.py");
		siocSetup.startSIOCWithDefaultDB();
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
	}

	@AfterEach
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();
	}

	@Test
	public void testArchiveFieldsPV() throws Exception {
        String[] fieldsToArchive = { "ArchUnitTest:fieldtstalias", "ArchUnitTest:fieldtstalias.C" };
        List<JSONObject> arSpecs = List.of(fieldsToArchive).stream().map((x) -> new JSONObject(Map.of("pv", x))).collect(Collectors.toList());
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        logger.info("Archiving multiple PVs");
        GetUrlContent.postDataAndGetContentAsJSONArray(mgmtURL + "archivePV", GetUrlContent.from(arSpecs));
        for(String pv : fieldsToArchive) {
            PVAccessUtil.waitForStatusChange(pv, "Being archived", 20, mgmtURL, 15);
        }

		// Check that we have PVTypeInfo's for the main PV. Also check the archiveFields.
		JSONObject valInfo = GetUrlContent.getURLContentAsJSONObject("http://localhost:17665/mgmt/bpl/getPVTypeInfo?pv=ArchUnitTest:fieldtst", true);
		logger.debug(valInfo.toJSONString());
		@SuppressWarnings("unchecked")
		List<String> archiveFields = (List<String>) valInfo.get("archiveFields");
		Assertions.assertTrue(archiveFields.contains("HIHI"), "TypeInfo should contain the HIHI field but it does not");
		Assertions.assertTrue(archiveFields.contains("LOLO"), "TypeInfo should contain the LOLO field but it does not");
		Assertions.assertTrue(!archiveFields.contains("DESC"), "TypeInfo should not contain the DESC field but it does");
		Assertions.assertTrue(!archiveFields.contains("C"), "TypeInfo should not contain the C field but it does");

		JSONObject C_Info = GetUrlContent.getURLContentAsJSONObject("http://localhost:17665/mgmt/bpl/getPVTypeInfo?pv=ArchUnitTest:fieldtst.C", true);
		Assertions.assertTrue(C_Info != null, "Did not find a typeinfo for ArchUnitTest:fieldtst.C");
		logger.debug(C_Info.toJSONString());

		// This test is very sensitive to older data ( PB files ) handing around etc since we are comparing actual values
		testRetrievalCount("ArchUnitTest:fieldtst", new double[] { 0.0 } );
		SIOCSetup.caput("ArchUnitTest:fieldtst:cnt", "0.0");
		Thread.sleep(2*60*1000);
		testRetrievalCount("ArchUnitTest:fieldtst", new double[] { 0.0 } );
		testRetrievalCount("ArchUnitTest:fieldtst.C", new double[] { 3.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5 } );
		testRetrievalCount("ArchUnitTest:fieldtstalias", new double[] { 0.0 } );
		testRetrievalCount("ArchUnitTest:fieldtstalias.C", new double[] { 3.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5 } );
	}
	
	private void testRetrievalCount(String pvName, double[] expectedValues) throws IOException {
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
        Instant end = TimeUtils.plusDays(TimeUtils.now(), 1);
        Instant start = TimeUtils.minusDays(end, 2);
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName }, start, end, null)) {
			long previousEpochSeconds = 0;
			int eventCount = 0;
			Assertions.assertTrue(stream != null, "Got a null event stream for PV " + pvName);
			for(Event e : stream) {
				long actualSeconds = e.getEpochSeconds();
				logger.debug("For " + pvName + " got value " + e.getSampleValue().getValue().doubleValue());
				Assertions.assertTrue(actualSeconds > previousEpochSeconds, "Got a sample at or before the previous sample " + actualSeconds + " ! >= " + previousEpochSeconds);
				previousEpochSeconds = actualSeconds;
				Assertions.assertTrue(Math.abs(Math.abs(e.getSampleValue().getValue().doubleValue()) -  Math.abs(expectedValues[eventCount])) < 0.001, "Got " + e.getSampleValue().getValue().doubleValue() + " expecting " +  expectedValues[eventCount] + " at " + eventCount);
				eventCount++;
			}

			Assertions.assertTrue(eventCount == expectedValues.length, "Expecting " + expectedValues.length + " got " + eventCount + " for pv " + pvName);
		}
	}
}
