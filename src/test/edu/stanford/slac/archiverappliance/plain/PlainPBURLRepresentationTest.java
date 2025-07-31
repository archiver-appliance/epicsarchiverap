package edu.stanford.slac.archiverappliance.plain;

import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PlainPBURLRepresentationTest {
    @Test
    public void testToAndFromURL() throws Exception {
        PlainStoragePlugin etlSrc = new PlainStoragePlugin();
        PlainCommonSetup srcSetup = new PlainCommonSetup();

        srcSetup.setUpRootFolder(
                etlSrc, "SimpleETLTestSrc_" + PartitionGranularity.PARTITION_HOUR, PartitionGranularity.PARTITION_HOUR);
        String urlRep = etlSrc.getURLRepresentation();
        ConfigService configService = new ConfigServiceForTests(-1);
        PlainStoragePlugin after =
                (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(urlRep, configService);
        assert after != null;
        Assertions.assertEquals(
                after.getRootFolder(),
                etlSrc.getRootFolder(),
                "Source folders are not the same" + after.getRootFolder() + etlSrc.getRootFolder());
    }
}
