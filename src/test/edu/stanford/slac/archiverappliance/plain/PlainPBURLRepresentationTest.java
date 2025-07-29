package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.plain.PlainPBStoragePlugin;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PlainPBURLRepresentationTest {
    @Test
    public void testToAndFromURL() throws Exception {
        PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin();
        PBCommonSetup srcSetup = new PBCommonSetup();

        srcSetup.setUpRootFolder(
                etlSrc, "SimpleETLTestSrc_" + PartitionGranularity.PARTITION_HOUR, PartitionGranularity.PARTITION_HOUR);
        String urlRep = etlSrc.getURLRepresentation();
        ConfigService configService = new ConfigServiceForTests(-1);
        PlainPBStoragePlugin after =
                (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(urlRep, configService);
        assert after != null;
        Assertions.assertEquals(
                after.getRootFolder(),
                etlSrc.getRootFolder(),
                "Source folders are not the same" + after.getRootFolder() + etlSrc.getRootFolder());
    }
}
