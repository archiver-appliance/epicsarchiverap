package org.epics.archiverappliance.common.remotable;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class RemotableEventStreamDescTest {
    @Test
    public void testMergeFromWithNullTypeInfo() throws Exception {
        RemotableEventStreamDesc desc =
                new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, "testPV", (short) 2026);
        HashMap<String, String> engineMetadata = new HashMap<>();
        engineMetadata.put("EGU", "Volts");
        engineMetadata.put("PREC", "3");

        // Should not throw NPE and should successfully attach engineMetadata
        desc.mergeFrom(null, engineMetadata);

        assertEquals("Volts", desc.getHeaders().get("EGU"));
        assertEquals("3", desc.getHeaders().get("PREC"));
    }
}
