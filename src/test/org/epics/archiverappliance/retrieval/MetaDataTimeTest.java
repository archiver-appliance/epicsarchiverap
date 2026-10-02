package org.epics.archiverappliance.retrieval;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

class MetaDataTimeTest {

    private PVTypeInfo typeInfo;
    private String pvName = "testPV";

    @BeforeEach
    void setUp() {
        typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_STRING, true, 1);
        typeInfo.setSamplingMethod(PolicyConfig.SamplingMethod.MONITOR);
    }

    @Test
    void testFromRequestStringsAndGetMetadataNone() {
        MetaDataTime metaDataTime = MetaDataTime.fromRequestStrings(null, null, null);
        assertEquals(Map.of(), metaDataTime.getMetadata(null, null, null, null, null, null));

        metaDataTime = MetaDataTime.fromRequestStrings("false", "false", "false");
        assertEquals(Map.of(), metaDataTime.getMetadata(null, null, null, null, null, null));
    }

    @Test
    void testMergeMetaData() throws Exception {
        RemotableEventStreamDesc remoteDesc =
                new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, (short) 2026);
        ;
        Map<String, String> metadata = Map.of("key", "value");

        MetaDataTime.mergeMetaData(typeInfo, remoteDesc, metadata);

        assertEquals("value", remoteDesc.getHeaders().get("key"));
    }

    @Test
    void testMergeNullInfo() throws Exception {
        RemotableEventStreamDesc remoteDesc =
                new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, (short) 2026);

        Map<String, String> metadata = Map.of("key", "value");

        boolean result = MetaDataTime.tryMergeMetaData(null, remoteDesc, metadata);

        assertFalse(result);
        assertEquals("value", remoteDesc.getHeaders().get("key"));
    }
}
