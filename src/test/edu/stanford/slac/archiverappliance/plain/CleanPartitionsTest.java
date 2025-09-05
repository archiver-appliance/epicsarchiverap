package edu.stanford.slac.archiverappliance.plain;

import static edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin.pbFileExtension;

import edu.stanford.slac.archiverappliance.plain.PathNameUtility.StartEndTimeFromName;
import edu.stanford.slac.archiverappliance.plain.pb.PBFileInfo;
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
import org.junit.jupiter.api.Test;

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

    @Test
    public void testCleanPartitions() throws Exception {
        for (PartitionGranularity granularity : PartitionGranularity.values()) {
            PlainStoragePlugin pbPlugin = new PlainStoragePlugin();
            PlainCommonSetup srcSetup = new PlainCommonSetup();

            srcSetup.setUpRootFolder(pbPlugin, "TestCleanPartitions_" + granularity, granularity);

            String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "CleanPartition"
                    + pbPlugin.getPartitionGranularity();
            SimulationEventStream simstream = new SimulationEventStream(
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    new SineGenerator(0),
                    Instant.now(),
                    Instant.now().plusSeconds(granularity.getApproxSecondsPerChunk() * 3L),
                    granularity.getApproxSecondsPerChunk() / 3);
            try (BasicContext context = new BasicContext()) {
                pbPlugin.appendData(context, pvName, simstream);
            }
            logger.info("Done creating src data for PV " + pvName + " for granularity "
                    + pbPlugin.getPartitionGranularity());

            Path[] allPaths = PathNameUtility.getAllPathsForPV(
                    new ArchPaths(),
                    pbPlugin.getRootFolder(),
                    pvName,
                    pbFileExtension,
                    pbPlugin.getPlainFileHandler().getPathResolver(),
                    configService.getPVNameToKeyConverter());
            for (Path pbFile : allPaths) {
                PBFileInfo fileInfo = new PBFileInfo(pbFile);
                StartEndTimeFromName chunkTimes = PathNameUtility.determineTimesFromFileName(
                        pvName,
                        pbFile.getFileName().toString(),
                        pbPlugin.getPartitionGranularity(),
                        configService.getPVNameToKeyConverter());
                // Make sure that the first and last event in the file as obtained from PBFileInfo fit into the times as
                // determined from the name
                Assertions.assertTrue(
                        fileInfo.getFirstEvent().getEventTimeStamp().isAfter(chunkTimes.pathDataStartTime.toInstant())
                                || fileInfo.getFirstEvent()
                                        .getEventTimeStamp()
                                        .equals(chunkTimes.pathDataStartTime.toInstant()),
                        "Start time as determined by PBFileinfo "
                                + fileInfo.getFirstEvent().getEventTimeStamp()
                                + " is earlier than earliest time as determined by partition name"
                                + chunkTimes.pathDataStartTime);
                Assertions.assertTrue(
                        fileInfo.getLastEvent().getEventTimeStamp().isBefore(chunkTimes.pathDataEndTime.toInstant())
                                || fileInfo.getLastEvent()
                                        .getEventTimeStamp()
                                        .equals(chunkTimes.pathDataEndTime.toInstant()),
                        "End time as determined by PBFileinfo "
                                + fileInfo.getLastEvent().getEventTimeStamp()
                                + " is later than latest time as determined by partition name "
                                + chunkTimes.pathDataEndTime);
            }
            srcSetup.deleteTestFolder();
        }
    }
}
