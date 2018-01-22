package org.epics.archiverappliance.mgmt.pva;

import java.util.logging.Logger;

import org.epics.nt.NTURI;
import org.epics.pvaccess.client.rpc.RPCClient;
import org.epics.pvaccess.client.rpc.RPCClientFactory;
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvdata.pv.PVStructure;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.epics.archiverappliance.mgmt.pva.actions.PvaGetAllPVs.NAME;

/**
 * Test the the pvAccess mgmt service's ability to start archiving a pv
 * 
 * @author Kunal Shroff
 *
 */
public class PvaGetAllPVTest {

	private static Logger logger = Logger.getLogger(PvaGetAllPVTest.class.getName());
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
		logger.info("Test");
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
