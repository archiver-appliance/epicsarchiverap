package edu.stanford.slac.archiverappliance.PlainPB;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;

public class PlainPBURLRepresentationTest {

	@Test
	public void testToAndFromURL() throws Exception {
		PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin();
		PBCommonSetup srcSetup = new PBCommonSetup();

		srcSetup.setUpRootFolder(etlSrc, "SimpleETLTestSrc_"+PartitionGranularity.PARTITION_HOUR, PartitionGranularity.PARTITION_HOUR);
		String urlRep = etlSrc.getURLRepresentation();
		ConfigService configService = new ConfigServiceForTests(new File("./bin"));
		PlainPBStoragePlugin after = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(urlRep, configService);
		assertTrue("Source folders are not the same" + after.getRootFolder() + etlSrc.getRootFolder(), after.getRootFolder().equals(etlSrc.getRootFolder()));
	}

}
