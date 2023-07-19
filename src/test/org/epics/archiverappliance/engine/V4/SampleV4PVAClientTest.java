package org.epics.archiverappliance.engine.V4;


import org.epics.archiverappliance.SIOCSetup;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.epics.pva.data.PVADouble;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.server.PVAServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Integration test of the pvAccess library
 */
@Tag("localEpics")
public class SampleV4PVAClientTest {

    private SIOCSetup ioc;

    @BeforeEach
    public void setUp() throws Exception {
        ioc = new SIOCSetup();
        ioc.startSIOCWithDefaultDB();

    }

    @AfterEach
    public void tearDown() throws Exception {
        ioc.stopSIOC();
    }

    @Test
    public void testGet() throws Exception {
        PVAClient client = new PVAClient();
        PVAChannel channel = client.getChannel("UnitTestNoNamingConvention:sine:calc");
        channel.connect().get(5, TimeUnit.SECONDS);
        PVAStructure value = channel.read("").get(5, TimeUnit.SECONDS);
        Assertions.assertFalse(new PVADouble("value", Double.NaN) == value.get("value"));
        channel.close();
        client.close();
    }

    @Test
    public void testServer() throws Exception {
        PVAServer server = new PVAServer();
        String pvName = UUID.randomUUID().toString();
        PVAString value = new PVAString(UUID.randomUUID().toString());
        PVAStructure data = new PVAStructure(pvName, pvName, value);
        server.createPV(pvName, data);

        server.close();
    }
}
