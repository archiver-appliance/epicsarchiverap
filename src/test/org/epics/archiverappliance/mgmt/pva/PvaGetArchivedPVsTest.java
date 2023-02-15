package org.epics.archiverappliance.mgmt.pva;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.epics.archiverappliance.mgmt.pva.actions.NTUtil.extractStringArray;
import static org.junit.Assert.assertArrayEquals;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetArchivedPVs;
import org.epics.nt.NTTable;
import org.epics.pvaccess.client.rpc.RPCClient;
import org.epics.pvaccess.client.rpc.RPCClientFactory;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
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

	private static Logger logger = Logger.getLogger(PvaGetArchivedPVsTest.class.getName());

	static TomcatSetup tomcatSetup = new TomcatSetup();
	static SIOCSetup siocSetup = new SIOCSetup();

	private static RPCClient client;

	@BeforeClass
	public static void setup() {
		logger.info("Set up for the PvaGetArchivedPVsTest");
		try {
			siocSetup.startSIOCWithDefaultDB();
			tomcatSetup.setUpWebApps(PvaTest.class.getSimpleName());

			Thread.sleep(3*60*1000);
		
			logger.info(ZonedDateTime.now(ZoneId.systemDefault())
					+ " Waiting three mins for the service setup to complete");
			client = RPCClientFactory.create(PVA_MGMT_SERVICE);
		} catch (Exception e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	@AfterClass
	public static void tearDown() {
		logger.info("Tear Down for the PvaGetArchivedPVsTest");
		try {
			client.destroy();
			tomcatSetup.tearDown();
			siocSetup.stopSIOC();
		} catch (Exception e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
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
			NTTable archivePvReqTable = NTTable.createBuilder().addDescriptor().addColumn("pv", ScalarType.pvString).create();
			archivePvReqTable.getDescriptor().put(PvaArchivePVAction.NAME);
			archivePvReqTable.getColumn(PVStringArray.class, "pv").put(0, pvNamesEven.size(), pvNamesEven.toArray(new String[pvNamesEven.size()]), 0);
			PVStructure result = client.request(archivePvReqTable.getPVStructure(), 30);
			
			Thread.sleep(2*60*1000);
			
			// Wait 2 mins for the pv's to start archiving
			archivePvReqTable = NTTable.createBuilder().addDescriptor().addColumn("pv", ScalarType.pvString).create();
			archivePvReqTable.getDescriptor().put(PvaGetArchivedPVs.NAME);
			archivePvReqTable.getColumn(PVStringArray.class, "pv").put(0, pvNamesAll.size(), pvNamesAll.toArray(new String[pvNamesAll.size()]), 0);
			result = client.request(archivePvReqTable.getPVStructure(), 30);
			assertArrayEquals(pvNamesAll.toArray(new String[1000]),
					extractStringArray(NTTable.wrap(result).getColumn(PVStringArray.class, "pv")));
			assertArrayEquals(expectedStatus.toArray(new String[1000]),
					extractStringArray(NTTable.wrap(result).getColumn(PVStringArray.class, "status")));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
