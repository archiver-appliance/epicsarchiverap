package org.epics.archiverappliance.engine.V4;

import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVATimeStamp;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Basic unit tests of the pvaAccess library
 */
public class SamplePVATest {

    @Test
    public void testPVAData() throws Exception {
        PVAString pvString = new PVAString("pvString", "pvStringValue");
        PVAStructure s = new PVAStructure("pvName", "structName", pvString);

        PVAStructure clone = s.cloneData();
        assertEquals(s, clone);

        clone.get("pvString").setValue("newPVStringValue");
        assertNotEquals("Assert modified structure different to unmodified", clone, s);

        assertEquals(new PVAString("pvString", "newPVStringValue"), clone.get("pvString"));
    }

    @Test
    public void testPVTimeStamp() throws Exception {
        Instant now = Instant.now();
        PVATimeStamp timeStamp = new PVATimeStamp(now);
        PVAStructure s = new PVAStructure("pvStructure", "StructType", timeStamp);

        assertEquals(s.get("timeStamp"), timeStamp);
        Instant after = Instant.now();
        timeStamp.set(after);

        assertEquals(s.get("timeStamp"), new PVATimeStamp(after));
        assertNotEquals("Assert new timestamp different to old", now, after);
    }
}
