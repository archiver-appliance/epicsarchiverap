package org.epics.archiverappliance.mgmt.pva;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.mgmt.pva.actions.NTUtil;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetApplianceInfo;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVATable;
import org.epics.pva.data.nt.PVAURI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/**
 * Test the pvAccess mgmt service's ability to start archiving a pv
 * 
 * @author Kunal Shroff
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class PvaSuiteTstGetApplianceInfo {

	private static final Logger logger = LogManager.getLogger(PvaSuiteTstGetApplianceInfo.class.getName());

	private static PVAClient pvaClient;
	private static PVAChannel pvaChannel;
	@BeforeClass
	public static void setup() throws Exception {
		pvaClient = new PVAClient();
		pvaChannel = pvaClient.getChannel(PVA_MGMT_SERVICE);
		pvaChannel.connect().get(5, TimeUnit.SECONDS);

	}

	@AfterClass
	public static void cleanup() {

		pvaChannel.close();
		pvaClient.close();	}

	/**
	 * 
	 */
	@Test
	public void getApplianceInfo() {
		PVAURI uri = new PVAURI("uri", "pva", PvaGetApplianceInfo.NAME);

		try {
			PVAStructure result = pvaChannel.invoke(uri).get(30, TimeUnit.SECONDS);
			/*
			  INFO: resultsepics:nt/NTTable:1.0
			  string[] labels [Key,Value]
			  structure value
			  string[] Key [identity,mgmtURL,engineURL,retrievalURL,etlURL]
			  string[] Value [appliance0,http://localhost:17665/mgmt/bpl,http://localhost:17665/engine/bpl,http://localhost:17665/retrieval/bpl,http://localhost:17665/etl/bpl]

			 */
			//TODO compare the result with the above expected result
			String[] expextedKeys = new String[] {"identity","mgmtURL","engineURL","retrievalURL","etlURL"};
			String[] expectedValues = new String[] {
					"appliance0",
					"http://localhost:17665/mgmt/bpl",
					"http://localhost:17665/engine/bpl",
					"http://localhost:17665/retrieval/bpl",
					"http://localhost:17665/etl/bpl"};
			logger.info("results" + result.toString());
			assertArrayEquals(expextedKeys, NTUtil.extractStringArray(PVATable.fromStructure(result).getColumn("Key")));
			assertArrayEquals(expectedValues, NTUtil.extractStringArray(PVATable.fromStructure(result).getColumn("Value")));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}	
}
