package org.epics.archiverappliance.mgmt.pva;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.mgmt.pva.actions.NTUtil;
import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetArchivedPVs;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVATable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * {@link PvaGetArchivedPVs}
 * 
 * @author Kunal Shroff
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class PvaGetArchivedPVsTest {

	private static final Logger logger = LogManager.getLogger(PvaGetArchivedPVsTest.class.getName());

	static TomcatSetup tomcatSetup = new TomcatSetup();
	static SIOCSetup siocSetup = new SIOCSetup();

	private static PVAClient pvaClient;
	private static PVAChannel pvaChannel;

	@BeforeClass
	public static void setup() {
		logger.info("Set up for the PvaGetArchivedPVsTest");
		try {
			siocSetup.startSIOCWithDefaultDB();
			tomcatSetup.setUpWebApps(PvaTest.class.getSimpleName());

			pvaClient = new PVAClient();
			pvaChannel = pvaClient.getChannel(PVA_MGMT_SERVICE);
			pvaChannel.connect().get(5, TimeUnit.SECONDS);
		} catch (Exception e) {
			logger.log(Level.FATAL, e.getMessage(), e);
		}
	}

	@AfterClass
	public static void tearDown() {
		logger.info("Tear Down for the PvaGetArchivedPVsTest");
		try {
			pvaChannel.close();
			pvaClient.close();
			tomcatSetup.tearDown();
			siocSetup.stopSIOC();
		} catch (Exception e) {
			logger.log(Level.FATAL, e.getMessage(), e);
		}
	}

	@Test
	public void archivedPVTest() {
		List<String> pvNamesAll = new ArrayList<String>(1000);
		List<String> pvNamesEven = new ArrayList<String>(500);
		List<String> pvNamesOdd = new ArrayList<String>(500);
		List<String> expectedStatus = new ArrayList<String>(1000);
		for (int i = 0; i < 1000; i++) {
			pvNamesAll.add("test_" + i);
			if (i % 2 == 0) {
				pvNamesEven.add("test_" + i);
				expectedStatus.add("Archived");
			} else {
				pvNamesOdd.add("test_" + i);
				expectedStatus.add("Not Archived");
			}
		}

		try {
			// Submit all the even named pv's to be archived
			PVATable archivePvReqTable = PVATable.PVATableBuilder.aPVATable().name(PvaArchivePVAction.NAME)
					.descriptor(PvaArchivePVAction.NAME)
					.addColumn(new PVAStringArray("pv", pvNamesEven.toArray(new String[pvNamesEven.size()])))
					.build();
			pvaChannel.invoke(archivePvReqTable).get(30, TimeUnit.SECONDS);

			// Wait 2 mins for the pv's to start archiving
			Thread.sleep(2*60*1000);

			archivePvReqTable = PVATable.PVATableBuilder.aPVATable().name(PvaArchivePVAction.NAME)
					.descriptor(PvaGetArchivedPVs.NAME)
					.addColumn(new PVAStringArray("pv", pvNamesAll.toArray(new String[pvNamesAll.size()])))
					.build();
			PVAStructure result = pvaChannel.invoke(archivePvReqTable).get(30, TimeUnit.SECONDS);
			assertArrayEquals(pvNamesAll.toArray(new String[1000]),
					NTUtil.extractStringArray(PVATable.fromStructure(result).getColumn("pv")));
			assertArrayEquals(expectedStatus.toArray(new String[1000]),
					NTUtil.extractStringArray(PVATable.fromStructure(result).getColumn("status")));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

}
