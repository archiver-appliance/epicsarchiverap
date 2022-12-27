package org.epics.archiverappliance.config;

import static org.junit.Assert.assertTrue;

import java.io.File;
import org.junit.Test;

public class PVNameToKeyConverterTest {

	@Test
	public void testKeyName() throws Exception {
        DefaultConfigService configService = new ConfigServiceForTests(new File("./bin"));
        String expectedKeyName = "A/B/C/D:";
        String keyName = configService.getPVNameToKeyConverter().convertPVNameToKey("A:B:C-D"); 
		assertTrue("We were expecting " + expectedKeyName + " instead we got " + keyName, expectedKeyName.equals(keyName));
	}

}
