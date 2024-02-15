package org.epics.archiverappliance.engine.V4;

import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVATimeStamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

/**
 * Basic unit tests of the pvaAccess library
 */
public class SamplePVATest {

    @Test
    public void testPVAData() throws Exception {
        PVAString pvString = new PVAString("pvString", "pvStringValue");
        PVAStructure s = new PVAStructure("pvName", "structName", pvString);

        PVAStructure clone = s.cloneData();
        Assertions.assertEquals(s, clone);

        clone.get("pvString").setValue("newPVStringValue");
        Assertions.assertNotEquals(clone, s, "Assert modified structure different to unmodified");

        Assertions.assertEquals(new PVAString("pvString", "newPVStringValue"), clone.get("pvString"));
    }

    @Test
    public void testPVTimeStamp() throws Exception {
        Instant now = Instant.now();
        PVATimeStamp timeStamp = new PVATimeStamp(now);
        PVAStructure s = new PVAStructure("pvStructure", "StructType", timeStamp);

        Assertions.assertEquals(s.get("timeStamp"), timeStamp);
        Instant after = Instant.now();
        timeStamp.set(after);

        Assertions.assertEquals(s.get("timeStamp"), new PVATimeStamp(after));
        Assertions.assertNotEquals(now, after, "Assert new timestamp different to old");
    }
}
