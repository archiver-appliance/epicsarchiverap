package org.epics.archiverappliance.mgmt.pva;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;

import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

/**
 * Check if the pva management rpc service has started and is accessible.
 *
 * @author Kunal Shroff
 *
 */
@Tag("integration")
@Tag("localEpics")
@ExtendWith(PvaTestSetupExtension.class)
public class PvaSuiteTstMgmtServiceStartup {

    /**
     * Check that the client was able to connect to the pva RPC service for the archiver mgmt
     */
    @Test
    public void testSerivceStartup() throws Exception {
        PVAClient pvaClient = new PVAClient();
        PVAChannel pvaChannel = pvaClient.getChannel(PVA_MGMT_SERVICE);
        pvaChannel.connect().get(30, TimeUnit.SECONDS);
        Assertions.assertTrue(pvaChannel.isConnected());
    }
}
