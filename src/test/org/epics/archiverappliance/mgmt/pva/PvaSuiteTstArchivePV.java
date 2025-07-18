package org.epics.archiverappliance.mgmt.pva;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.MustBeArrayException;
import org.epics.pva.data.nt.PVATable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.epics.archiverappliance.mgmt.pva.PvaTest.pvPrefix;
import static org.epics.archiverappliance.mgmt.pva.actions.NTUtil.extractStringArray;

/**
 *
 * @author Kunal Shroff
 *
 */
@Tag("integration")
@Tag("localEpics")
public class PvaSuiteTstArchivePV {

    private static final Logger logger = LogManager.getLogger(PvaSuiteTstArchivePV.class.getName());

    private static PVAClient pvaClient;
    private static PVAChannel pvaChannel;

    @BeforeAll
    public static void setup() throws ExecutionException, InterruptedException, TimeoutException {
        try {
            pvaClient = new PVAClient();
        } catch (Exception e) {
            logger.log(Level.FATAL, e.getMessage(), e);
        }
        pvaChannel = pvaClient.getChannel(PVA_MGMT_SERVICE);
        pvaChannel.connect().get(5, TimeUnit.SECONDS);
    }

    @AfterAll
    public static void cleanup() {
        pvaChannel.close();
        pvaClient.close();
    }

    @Test
    public void addSinglePVtest() throws MustBeArrayException {
        PVATable archivePvReqTable = PVATable.PVATableBuilder.aPVATable()
                .name(PvaArchivePVAction.NAME)
                .descriptor(PvaArchivePVAction.NAME)
                .addColumn(new PVAStringArray(
                        "pv",
                        pvPrefix + "UnitTestNoNamingConvention:sine",
                        pvPrefix + "UnitTestNoNamingConvention:cosine"))
                .addColumn(new PVAStringArray("samplingperiod", "1.0", "2.0"))
                .addColumn(new PVAStringArray("samplingmethod", "SCAN", "MONITOR"))
                .build();

        try {
            PVAStructure result = pvaChannel.invoke(archivePvReqTable).get(30, TimeUnit.SECONDS);
            /*
             Expected result string
             { "pvName": "mshankar:arch:sine", "status": "Archive request submitted" }
             { "pvName": "mshankar:arch:cosine", "status": "Archive request submitted" }
            */
            String[] expextedKePvNames = new String[] {
                pvPrefix + "UnitTestNoNamingConvention:sine", pvPrefix + "UnitTestNoNamingConvention:cosine"
            };
            String[] expectedStatus = new String[] {"Archive request submitted", "Archive request submitted"};
            logger.info("results" + result.toString());
            Assertions.assertArrayEquals(
                    expextedKePvNames,
                    extractStringArray(PVATable.fromStructure(result).getColumn("pvName")));
            Assertions.assertArrayEquals(
                    expectedStatus,
                    extractStringArray(PVATable.fromStructure(result).getColumn("status")));

            // Try submitting the request again...this time you should get a "already submitted" status response.
            Thread.sleep(60000L);
            String[] expectedSuccessfulStatus = new String[] {"Already submitted", "Already submitted"};
            result = pvaChannel.invoke(archivePvReqTable).get(30, TimeUnit.SECONDS);
            logger.info("results" + result.toString());
            Assertions.assertArrayEquals(
                    expextedKePvNames,
                    extractStringArray(PVATable.fromStructure(result).getColumn("pvName")));
            Assertions.assertArrayEquals(
                    expectedSuccessfulStatus,
                    extractStringArray(PVATable.fromStructure(result).getColumn("status")));

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            Assertions.fail(e.getMessage());
        }
    }
}
