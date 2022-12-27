package org.epics.archiverappliance.mgmt.pva;

import java.util.logging.Logger;

import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.nt.NTURI;
import org.epics.pvaccess.client.rpc.RPCClient;
import org.epics.pvaccess.client.rpc.RPCClientFactory;
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvdata.pv.PVStructure;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.epics.archiverappliance.mgmt.pva.actions.PvaGetAllPVs.NAME;

/**
 * Test the the pvAccess mgmt service's ability to start archiving a pv
 * 
 * @author Kunal Shroff
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class PvaSuiteTstGetAll {

	private static Logger logger = Logger.getLogger(PvaSuiteTstGetAll.class.getName());
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
	public void addPV() {
		NTURI uri = NTURI.createBuilder().create();
		uri.getPVStructure().getStringField("scheme").put("pva");
		uri.getPVStructure().getStringField("path").put(NAME);
		try {
			PVStructure result = client.request(uri.getPVStructure(), 30);
			logger.info("results" +result.toString());
		} catch (RPCRequestException e) {
			e.printStackTrace();
		}
	}
}
