package org.epics.archiverappliance.utils.ui;

import static org.epics.archiverappliance.utils.ui.URIUtils.pluginString;
import static org.junit.jupiter.api.Assertions.*;

import edu.stanford.slac.archiverappliance.plain.URLKey;
import org.junit.jupiter.api.Test;

import java.util.Map;

class URIUtilsTest {

    @Test
    void testPluginString() throws Exception {
        String resultURL = pluginString(
                "pb",
                "localhost",
                Map.of(URLKey.NAME, "STS", URLKey.ROOT_FOLDER, "root", URLKey.PARTITION_GRANULARITY, "PARTITION_HOUR"));
        assertTrue(resultURL.contains("pb://localhost?"));
        assertTrue(resultURL.contains("name=STS"));
        assertTrue(resultURL.contains("rootFolder=root"));
        assertTrue(resultURL.contains("partitionGranularity=PARTITION_HOUR"));
    }
}
