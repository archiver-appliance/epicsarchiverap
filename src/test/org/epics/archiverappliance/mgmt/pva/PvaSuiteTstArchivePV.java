package org.epics.archiverappliance.mgmt.pva;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.epics.archiverappliance.mgmt.pva.actions.NTUtil.extractStringArray;
import static org.junit.Assert.assertArrayEquals;

import java.util.logging.Logger;

import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.nt.NTTable;
import org.epics.pvaccess.client.rpc.RPCClient;
import org.epics.pvaccess.client.rpc.RPCClientFactory;
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * 
 * @author Kunal Shroff
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class PvaSuiteTstArchivePV {

	private static Logger logger = Logger.getLogger(PvaSuiteTstArchivePV.class.getName());
	private static RPCClient client;

	@BeforeClass
	public static void setup() {
		client = RPCClientFactory.create(PVA_MGMT_SERVICE);

	}

	@AfterClass
	public static void cleanup() {
		client.destroy();
	}

	@Test
	public void addSinglePVtest() {
		NTTable archivePvReqTable = NTTable.createBuilder()
									.addDescriptor()
									.addColumn("pv", ScalarType.pvString)
									.addColumn("samplingperiod", ScalarType.pvString)
									.addColumn("samplingmethod", ScalarType.pvString)
									.create();
		archivePvReqTable.getDescriptor().put(PvaArchivePVAction.NAME);
		archivePvReqTable.getColumn(PVStringArray.class, "pv")
							.put(0, 2, new String[] {"UnitTestNoNamingConvention:sine","UnitTestNoNamingConvention:cosine"}, 0);
		archivePvReqTable.getColumn(PVStringArray.class, "samplingperiod")
							.put(0, 2, new String[] {"1.0","2.0"}, 0);
		archivePvReqTable.getColumn(PVStringArray.class, "samplingmethod")
							.put(0, 2, new String[] {"SCAN","MONITOR"}, 0);

		try {
			PVStructure result = client.request(archivePvReqTable.getPVStructure(), 30);
			/**
			 * Expected result string
			 * { "pvName": "mshankar:arch:sine", "status": "Archive request submitted" }
			 * { "pvName": "mshankar:arch:cosine", "status": "Archive request submitted" }
			 */
			String[] expextedKePvNames = new String[] { "UnitTestNoNamingConvention:sine", "UnitTestNoNamingConvention:cosine" };
			String[] expectedStatus = new String[] { "Archive request submitted", "Archive request submitted" };
			logger.info("results" + result.toString());
			assertArrayEquals(expextedKePvNames,
					extractStringArray(NTTable.wrap(result).getColumn(PVStringArray.class, "pvName")));
			assertArrayEquals(expectedStatus,
					extractStringArray(NTTable.wrap(result).getColumn(PVStringArray.class, "status")));
			
			// Try submitting the request again...this time you should get a "already submitted" status response.
			Thread.sleep(60000L);
			String[] expectedSuccessfulStatus = new String[] { "Already submitted", "Already submitted" };
			result = client.request(archivePvReqTable.getPVStructure(), 30);
			logger.info("results" + result.toString());
			assertArrayEquals(expextedKePvNames,
					extractStringArray(NTTable.wrap(result).getColumn(PVStringArray.class, "pvName")));
			assertArrayEquals(expectedSuccessfulStatus,
					extractStringArray(NTTable.wrap(result).getColumn(PVStringArray.class, "status")));

		} catch (RPCRequestException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
