package org.epics.archiverappliance.mgmt.pva;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.junit.Assert.assertTrue;

import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 * Check if the pva management rpc service has started and is accessible.
 * 
 * @author Kunal Shroff
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class PvaSuiteTstMgmtServiceStartup {

	/**
	 * Check that the client was able to connect to the pva RPC service for the archiver mgmt
	 */
	@Test
	public void testSerivceStartup() throws Exception {
		PVAClient pvaClient = new PVAClient();
		PVAChannel pvaChannel = pvaClient.getChannel(PVA_MGMT_SERVICE);
		pvaChannel.connect().get(30, TimeUnit.SECONDS);
		assertTrue(pvaChannel.isConnected());
	}
}
