package org.epics.archiverappliance.mgmt.pva;

import static org.epics.archiverappliance.mgmt.pva.actions.NTUtil.*;
import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.junit.Assert.assertArrayEquals;

import org.apache.logging.log4j.Logger;

import org.apache.logging.log4j.LogManager;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetApplianceInfo;
import org.epics.nt.NTTable;
import org.epics.nt.NTURI;
import org.epics.pvaccess.client.rpc.RPCClient;
import org.epics.pvaccess.client.rpc.RPCClientFactory;
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the the pvAccess mgmt service's ability to start archiving a pv
 * 
 * @author Kunal Shroff
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class PvaSuiteTstGetApplianceInfo {

	private static Logger logger = LogManager.getLogger(PvaSuiteTstGetApplianceInfo.class.getName());
	private static RPCClient client;

	@BeforeClass
	public static void setup() {
		client = RPCClientFactory.create(PVA_MGMT_SERVICE);

	}

	@AfterClass
	public static void cleanup() {
		client.destroy();
	}

	/**
	 * 
	 */
	@Test
	public void getApplianceInfo() {
		NTURI uri = NTURI.createBuilder().create();
		uri.getPVStructure().getStringField("path").put(PvaGetApplianceInfo.NAME);
		try {
			PVStructure result = client.request(uri.getPVStructure(), 30);
			/**
			 * INFO: resultsepics:nt/NTTable:1.0 
			 * string[] labels [Key,Value]
			 * structure value
			 * string[] Key [identity,mgmtURL,engineURL,retrievalURL,etlURL]
			 * string[] Value [appliance0,http://localhost:17665/mgmt/bpl,http://localhost:17665/engine/bpl,http://localhost:17665/retrieval/bpl,http://localhost:17665/etl/bpl]
			 * 
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
			assertArrayEquals(expextedKeys, extractStringArray(NTTable.wrap(result).getColumn(PVStringArray.class, "Key")));
			assertArrayEquals(expectedValues, extractStringArray(NTTable.wrap(result).getColumn(PVStringArray.class, "Value")));
		} catch (RPCRequestException e) {
			e.printStackTrace();
		}
	}	
}
