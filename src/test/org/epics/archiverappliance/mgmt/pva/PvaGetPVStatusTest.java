package org.epics.archiverappliance.mgmt.pva;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.awaitility.Awaitility;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.mgmt.pva.actions.NTUtil;
import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetArchivedPVs;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetPVStatus;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.MustBeArrayException;
import org.epics.pva.data.nt.PVATable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.awaitility.pollinterval.FibonacciPollInterval.fibonacci;
import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.junit.Assert.*;

/**
 * {@link PvaGetArchivedPVs}
 *
 * @author Kunal Shroff
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class PvaGetPVStatusTest {

	private static final Logger logger = LogManager.getLogger(PvaGetPVStatusTest.class.getName());

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

            logger.info(ZonedDateTime.now(ZoneId.systemDefault())
                    + " Waiting three mins for the service setup to complete");
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

    private PVAStructure getCurrentStatus(List<String> pvNames, PVAChannel pvaChannel) throws ExecutionException,
            InterruptedException, TimeoutException, MustBeArrayException {

        PVATable archivePvStatusReqTable = PVATable.PVATableBuilder.aPVATable().name(PvaGetPVStatus.NAME)
                .descriptor(PvaGetPVStatus.NAME)
                .addColumn(new PVAStringArray("pv", pvNames.toArray(new String[pvNames.size()])))
                .build();
        return pvaChannel.invoke(archivePvStatusReqTable).get(30, TimeUnit.SECONDS);
    }

    @Test
    public void archivedPVTest() {
        List<String> pvNamesAll = new ArrayList<String>(10);
        List<String> pvNamesEven = new ArrayList<String>(5);
        List<String> pvNamesOdd = new ArrayList<String>(5);
        HashMap<String, String> expectedStatus = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String pvName = "test_" + i;
            pvNamesAll.add(pvName);
            if (i % 2 == 0) {
                pvNamesEven.add(pvName);
                expectedStatus.put(pvName, "Being archived");
            } else {
                pvNamesOdd.add(pvName);
                expectedStatus.put(pvName, "Not being archived");
            }
        }

        try {
            // Submit all the even named pv's to be archived
            PVATable archivePvStatusReqTable = PVATable.PVATableBuilder.aPVATable().name(PvaArchivePVAction.NAME)
                    .descriptor(PvaArchivePVAction.NAME)
                    .addColumn(new PVAStringArray("pv", pvNamesEven.toArray(new String[pvNamesEven.size()])))
                    .build();
            pvaChannel.invoke(archivePvStatusReqTable).get(30, TimeUnit.SECONDS);


            Awaitility.await()
                    .pollInterval(fibonacci(TimeUnit.SECONDS))
                    .atMost(5, TimeUnit.MINUTES)
                    .untilAsserted(() ->
                            assertEquals(
                                    expectedStatus,
                                    getStatuses(pvNamesAll)
                            )
                    );

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    private HashMap<String, String> getStatuses(List<String> pvNamesAll) throws ExecutionException, InterruptedException, TimeoutException, MustBeArrayException {
        var statuses = NTUtil.extractStringArray(PVATable
                .fromStructure(getCurrentStatus(pvNamesAll, pvaChannel))
                .getColumn("status"));
        var pvs = NTUtil.extractStringArray(PVATable
                .fromStructure(getCurrentStatus(pvNamesAll, pvaChannel))
               .getColumn("pv"));
        var result = new HashMap<String, String>();
        for (int i = 0; i<pvs.length; i++) {
            result.put(pvs[i], statuses[i]);
        }
        return result;
    }

}
