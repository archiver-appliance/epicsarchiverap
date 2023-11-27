package org.epics.archiverappliance.mgmt.pva;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetAllPVs;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVAURI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;

/**
 * Test the pvAccess mgmt service's ability to start archiving a pv
 * 
 * @author Kunal Shroff
 *
 */
@Tag("integration")@Tag("localEpics")
public class PvaSuiteTstGetAll {

	private static final Logger logger = LogManager.getLogger(PvaSuiteTstGetAll.class.getName());

	private static PVAClient pvaClient;
	private static PVAChannel pvaChannel;

	@BeforeAll
	public static void setup() throws Exception {
		pvaClient = new PVAClient();
		pvaChannel = pvaClient.getChannel(PVA_MGMT_SERVICE);
		pvaChannel.connect().get(15, TimeUnit.SECONDS);

	}

	@AfterAll
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
			Assertions.assertNotNull(result);
			logger.info("results" + result);
		} catch (Exception e) {
			e.printStackTrace();
			Assertions.fail(e.getMessage());
		}
	}
}
