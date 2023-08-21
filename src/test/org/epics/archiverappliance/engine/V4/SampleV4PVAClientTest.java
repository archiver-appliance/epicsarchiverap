package org.epics.archiverappliance.engine.V4;

import static org.junit.Assert.assertFalse;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.epics.pva.data.PVADouble;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.server.PVAServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration test of the pvAccess library
 */
@Category(LocalEpicsTests.class )
public class SampleV4PVAClientTest {

    private SIOCSetup ioc;

    @Before
    public void setUp() throws Exception {
        ioc = new SIOCSetup();
        ioc.startSIOCWithDefaultDB();

    }

    @After
    public void tearDown() throws Exception {
        ioc.stopSIOC();
    }

    @Test
    public void testGet() throws Exception {
        PVAClient client = new PVAClient();
        PVAChannel channel = client.getChannel("UnitTestNoNamingConvention:sine:calc");
        channel.connect().get(5, TimeUnit.SECONDS);
        PVAStructure value = channel.read("").get(5, TimeUnit.SECONDS);
        assertFalse(new PVADouble("value", Double.NaN) == value.get("value"));
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
