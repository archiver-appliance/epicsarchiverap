package org.epics.archiverappliance.engine.V4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.time.Instant;

import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVATimeStamp;
import org.junit.Test;

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
        assertFalse("Assert modified structure different to unmodified", clone.equals(s));

        assertEquals(clone.get("pvString"), new PVAString("pvString", "newPVStringValue"));
    }

    @Test
    public void testPVTimeStamp() throws Exception {
        Instant now = Instant.now();
        PVATimeStamp timeStamp = new PVATimeStamp(now);
        PVAStructure s = new PVAStructure("pvStructure", "StructType", timeStamp);

        assertEquals(((PVATimeStamp) s.get("timeStamp")), timeStamp);
        Instant after = Instant.now();
        timeStamp.set(after);

        assertEquals(((PVATimeStamp) s.get("timeStamp")), new PVATimeStamp(after));
        assertFalse("Assert new timestamp different to old", now.equals(after));
    }
}
