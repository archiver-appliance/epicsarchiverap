package org.epics.archiverappliance.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PVNameToKeyConverterTest {

    @Test
    public void testKeyName() throws Exception {
        DefaultConfigService configService = new ConfigServiceForTests(-1);
        String expectedKeyName = "A/B/C/D+";
        String keyName = configService.getPVNameToKeyConverter().convertPVNameToKey("A:B:C-D");
        Assertions.assertEquals(
                expectedKeyName, keyName, "We were expecting " + expectedKeyName + " instead we got " + keyName);
    }
}
