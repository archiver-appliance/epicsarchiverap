package org.epics.archiverappliance.mgmt.pva;

import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVAURI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetAllPVs;
import static org.junit.Assert.*;

/**
 * Test the pvAccess mgmt service's ability to start archiving a pv
 * 
 * @author Kunal Shroff
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class PvaSuiteTstGetAll {

	private static final Logger logger = LogManager.getLogger(PvaSuiteTstGetAll.class.getName());

	private static PVAClient pvaClient;
	private static PVAChannel pvaChannel;

	@BeforeClass
	public static void setup() throws Exception {
		pvaClient = new PVAClient();
		pvaChannel = pvaClient.getChannel(PVA_MGMT_SERVICE);
		pvaChannel.connect().get(15, TimeUnit.SECONDS);

	}

	@AfterClass
	public static void cleanup() {

		pvaChannel.close();
		pvaClient.close();
	}

	/**
	 * 
	 */
	@Test
	public void addPV() {
		PVAURI uri = new PVAURI("uri", "pva", PvaGetAllPVs.NAME);
		try {
			PVAStructure result = pvaChannel.invoke(uri).get(30, TimeUnit.SECONDS);
			assertNotNull(result);
			logger.info("results" + result);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
