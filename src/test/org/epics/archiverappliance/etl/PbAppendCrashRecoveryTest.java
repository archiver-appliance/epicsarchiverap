package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.ScalarInt;
import edu.stanford.slac.archiverappliance.PB.data.PBScalarInt;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import edu.stanford.slac.archiverappliance.plain.PathResolver;
import edu.stanford.slac.archiverappliance.plain.pb.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.plain.pb.PBAppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.pb.PBEventFileWriter;
import edu.stanford.slac.archiverappliance.plain.pb.PBFileInfo;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PbAppendCrashRecoveryTest {

    @Test
    public void testAppendAfterCorruptionDoesNotEmbedGarbage() throws Exception {
        Path testDir = Files.createTempDirectory("pb_crash_test");
        try {
            String pvName = "TESTPV";
            short year = TimeUtils.getCurrentYear();
            int initialCount = 5;
            Path pbFile = testDir.resolve(pvName + ":" + year + ".pb");

            // 1. Write a clean file with initialCount events
            try (PBEventFileWriter writer = new PBEventFileWriter(pvName, pbFile, ArchDBRTypes.DBR_SCALAR_INT, year)) {
                for (int i = 0; i < initialCount; i++) {
                    byte[] escaped = LineEscaper.escapeNewLines(ScalarInt.newBuilder()
                            .setSecondsintoyear(i * 60).setNano(0).setVal(i).build().toByteArray());
                    writer.append(new PBScalarInt(year, new ByteArray(escaped)));
                }
            }
            long cleanSize = Files.size(pbFile);

            // Diagnostic: verify the clean file is readable before corrupting it
            PBFileInfo cleanInfo = new PBFileInfo(pbFile);
            Assertions.assertNotNull(cleanInfo.getLastEvent(), "clean file: last event must be readable");

            // 2. Simulate crash mid-write: append partial proto bytes with no terminating \n
            byte[] garbage = ScalarInt.newBuilder()
                    .setSecondsintoyear(999 * 60).setNano(0).setVal(999).build().toByteArray();
            Files.write(pbFile, garbage, StandardOpenOption.APPEND);
            Assertions.assertTrue(Files.size(pbFile) > cleanSize);

            // 3. PBFileInfo must detect corruption: truncationPoint == cleanSize, not the corrupt end
            PBFileInfo info = new PBFileInfo(pbFile);
            Assertions.assertNotNull(info.getLastEvent(), "last valid event must be found");
            Assertions.assertTrue(info.getTruncationPoint() < Files.size(pbFile),
                    "truncationPoint must be before corrupt tail");
            Assertions.assertEquals(cleanSize, info.getTruncationPoint(),
                    "truncationPoint must equal the pre-corruption file size");

            // 4. Append new events via production path:
            // partitionBoundaryAwareAppendData → preparePartition → updateStateBasedOnExistingFile
            // (truncates corrupt tail) → writer.append for each event
            int appendCount = 3;
            PVNameToKeyMapping pv2key = new PVNameToKeyMapping() {
                @Override public String convertPVNameToKey(String pv) { return pv + ":"; }
                @Override public String[] breakIntoParts(String pv) { return new String[]{pv}; }
                @Override public void initialize(ConfigService cs) {}
                @Override public PVNameToKeyMapping overrideTerminator(char c) { return this; }
            };
            ArrayListEventStream appendStream = new ArrayListEventStream(
                    appendCount, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_INT, pvName, year));
            for (int i = initialCount; i < initialCount + appendCount; i++) {
                appendStream.add(new PBScalarInt(year, ScalarInt.newBuilder()
                        .setSecondsintoyear(i * 60).setNano(0).setVal(i)));
            }
            PBAppendDataStateData stateData = new PBAppendDataStateData(
                    PartitionGranularity.PARTITION_YEAR, testDir.toString(), "test",
                    null, pv2key, PathResolver.BASE_PATH_RESOLVER);
            try (BasicContext ctx = new BasicContext()) {
                stateData.partitionBoundaryAwareAppendData(ctx, pvName, appendStream, ".pb", null);
            }

            // Assert corrupt tail removed and new events appended:
            // file must have grown past cleanSize (new data present) but not contain garbage
            Assertions.assertTrue(Files.size(pbFile) > cleanSize,
                    "file must be larger than clean baseline after appending new events");
            int count = 0;
            try (FileBackedPBEventStream stream = new FileBackedPBEventStream(
                    pvName, pbFile, ArchDBRTypes.DBR_SCALAR_INT,
                    0, Files.size(pbFile))) {
                for (Event e : stream) {
                    e.getEventTimeStamp(); // throws PBParseException if garbage embedded
                    count++;
                }
            }
            Assertions.assertEquals(initialCount + appendCount, count,
                    "event count must equal initial + appended after corruption recovery");
        } finally {
            Files.walk(testDir).sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
