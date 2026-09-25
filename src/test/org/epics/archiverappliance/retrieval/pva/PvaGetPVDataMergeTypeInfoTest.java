package org.epics.archiverappliance.retrieval.pva;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;

public class PvaGetPVDataMergeTypeInfoTest {

    @Test
    public void testMergeTypeInfoWithNullTypeInfo() throws Exception {
        PvaGetPVData pvaGetPVData = new PvaGetPVData();

        RemotableEventStreamDesc desc =
                new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, "testPVA", (short) 2026);
        HashMap<String, String> engineMetadata = new HashMap<>();
        engineMetadata.put("EGU", "Amps");
        engineMetadata.put("PREC", "4");

        // Access the private mergeTypeInfo method
        Method mergeTypeInfoMethod = PvaGetPVData.class.getDeclaredMethod(
                "mergeTypeInfo", PVTypeInfo.class, org.epics.archiverappliance.EventStreamDesc.class, HashMap.class);
        mergeTypeInfoMethod.setAccessible(true);

        // Invoke with null typeInfo
        mergeTypeInfoMethod.invoke(pvaGetPVData, null, desc, engineMetadata);

        // Verify the metadata was attached to the description
        assertEquals("Amps", desc.getHeaders().get("EGU"));
        assertEquals("4", desc.getHeaders().get("PREC"));
    }
}
