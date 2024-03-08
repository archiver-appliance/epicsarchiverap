package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.PB.data.PlainCommonSetup;
import edu.stanford.slac.archiverappliance.plain.PathNameUtility.StartEndTimeFromName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.time.Instant;

/**
 * Tests that the PB plugin appendData stores data in clean partitions.
 * That is, the events do not leak into before or after the times as determined from the partition names
 * @author mshankar
 *
 */
public class CleanPartitionsTest {
    private static final Logger logger = LogManager.getLogger(CleanPartitionsTest.class.getName());
    private ConfigService configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
    }

    @ParameterizedTest
    @EnumSource(PlainStorageType.class)
    public void testCleanPartitions(PlainStorageType plainStorageType) throws Exception {
        for (PartitionGranularity granularity : PartitionGranularity.values()) {
            PlainStoragePlugin storagePlugin = new PlainStoragePlugin(plainStorageType);
            PlainCommonSetup srcSetup = new PlainCommonSetup();

            srcSetup.setUpRootFolder(storagePlugin, "TestCleanPartitions_" + granularity, granularity);

            String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "CleanPartition"
                    + storagePlugin.getPartitionGranularity();
            SimulationEventStream simstream = new SimulationEventStream(
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    new SineGenerator(0),
                    Instant.now(),
                    Instant.now().plusSeconds(granularity.getApproxSecondsPerChunk() * 3L),
                    granularity.getApproxSecondsPerChunk() / 3);
            try (BasicContext context = new BasicContext()) {
                storagePlugin.appendData(context, pvName, simstream);
            }
            logger.info("Done creating src data for PV " + pvName + " for granularity "
                    + storagePlugin.getPartitionGranularity());

            Path[] allPaths = PathNameUtility.getAllPathsForPV(
                    new ArchPaths(),
                    storagePlugin.getRootFolder(),
                    pvName,
                    storagePlugin.getExtensionString(),
                    PathResolver.BASE_PATH_RESOLVER,
                    configService.getPVNameToKeyConverter());
            Assertions.assertTrue(allPaths.length > 1);
            for (Path pbFile : allPaths) {
                FileInfo fileInfo = storagePlugin.fileInfo(pbFile);
                StartEndTimeFromName chunkTimes = PathNameUtility.determineTimesFromFileName(
                        pvName,
                        pbFile.getFileName().toString(),
                        storagePlugin.getPartitionGranularity(),
                        configService.getPVNameToKeyConverter());
                // Make sure that the first and last event in the file as obtained from FileInfo fit into the times as
                // determined from the name
                Assertions.assertTrue(
                        fileInfo.getFirstEvent().getEventTimeStamp().isAfter(chunkTimes.pathDataStartTime.toInstant())
                                || fileInfo.getFirstEvent()
                                        .getEventTimeStamp()
                                        .equals(chunkTimes.pathDataStartTime.toInstant()),
                        "Start time as determined by FileInfo "
                                + fileInfo.getFirstEvent().getEventTimeStamp()
                                + " is earlier than earliest time as determined by partition name"
                                + chunkTimes.pathDataStartTime);
                Assertions.assertTrue(
                        fileInfo.getLastEvent().getEventTimeStamp().isBefore(chunkTimes.pathDataEndTime.toInstant())
                                || fileInfo.getLastEvent()
                                        .getEventTimeStamp()
                                        .equals(chunkTimes.pathDataEndTime.toInstant()),
                        "End time as determined by FileInfo "
                                + fileInfo.getLastEvent().getEventTimeStamp()
                                + " is later than latest time as determined by partition name"
                                + chunkTimes.pathDataEndTime);
            }
            srcSetup.deleteTestFolder();
        }
    }
}
