package org.epics.archiverappliance.mgmt.pva;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.junit.Assert.assertTrue;

import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.pvaccess.client.rpc.RPCClient;
import org.epics.pvaccess.client.rpc.RPCClientFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Check if the pva management rpc service has started and is accessible.
 * 
 * @author Kunal Shroff
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class PvaSuiteTstMgmtServiceStartup {

	private static RPCClient client;

	/**
	 * Check that the client was able to connect to the pva RPC service for the archiver mgmt
	 */
	@Test
	public void testSerivceStartup() {
		client = RPCClientFactory.create(PVA_MGMT_SERVICE);
		assertTrue(client.waitConnect(30));
	}
}
