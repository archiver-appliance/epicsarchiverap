package org.epics.archiverappliance.utils.nio.gztar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;

import java.io.Closeable;
import java.io.File;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.Callable;

/*
 * Performance test. Generate a decades worth of 1Hz PV data into a gztar plugin and then measure the performance for data reqrieval.
 * This can take a really lomg time especially with debug logging turned on; that's why this is not a unit test
 * For best results run this from within gradle; create a minimal_log4j2.xml first
 * <code>tasks.register('gzTarPerf', JavaExec) {
        group = 'application'
        description = 'Runs the GZTar performance'
		dependsOn("compileJava")
        classpath = sourceSets.test.runtimeClasspath
        mainClass = 'edu.stanford.slac.archiverappliance.PB.compression.gztar.Perf'
		jvmArgs = [
			"-Dlog4j2.configurationFile=/work/minimal_log4j2.xml"
		]
}</code>
    Then one can do a
    <code>gradle gzTarPerf --args "/work/test 10 60"</code>
 */
public class Perf implements Closeable {
    private static final Logger logger = LogManager.getLogger();
    private static final String pvName = "epics:arch:gztartest";
    private static final String chunkKey = pvName.replace(":", File.separator);
    private final String rootFolderStr;
    private ConfigServiceForTests configService;

    public Perf(String rootFolder) throws Exception {
        this.rootFolderStr = rootFolder;
        configService = new ConfigServiceForTests(1);
    }

    private void appendAndTestForYear(short forYear, int expectedCatalogEntryCount, int skipSeconds) throws Exception {
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=Test&rootFolder=" + URLEncoder.encode("gztar://" + rootFolderStr, "UTF-8")
                        + "&partitionGranularity=PARTITION_DAY",
                configService);
        try (BasicContext context = new BasicContext()) {
            short year = forYear;
            for (int day = 0; day < 365; day++) {
                ArrayListEventStream testData = new ArrayListEventStream(
                        24 * 60 * 60, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
                int startofdayinseconds = day * 24 * 60 * 60;
                for (int secondintoday = 0; secondintoday < 24 * 60 * 60; secondintoday += skipSeconds) {
                    Instant dataTs = TimeUtils.convertFromYearSecondTimestamp(
                            new YearSecondTimestamp(forYear, startofdayinseconds + secondintoday, 0));
                    testData.add(new POJOEvent(
                                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                    dataTs,
                                    new ScalarValue<Long>(dataTs.getEpochSecond()),
                                    0,
                                    0)
                            .makeClone());
                }
                storagePlugin.appendData(context, pvName, testData);
            }
        }

        EAATar tarFile = new EAATar(rootFolderStr + File.separator + chunkKey + ".tar");
        Map<String, TarEntry> entries = tarFile.loadCatalog();
    }

    private int testRetrieval(Instant start, Instant end, int expectedEventCount) throws Exception {
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=Test&rootFolder=" + URLEncoder.encode("gztar://" + rootFolderStr, "UTF-8")
                        + "&partitionGranularity=PARTITION_DAY",
                configService);
        logger.debug(
                "Looking for data between {} and {}",
                TimeUtils.convertToISO8601String(start),
                TimeUtils.convertToISO8601String(end));
        int eventCount = 0;
        try (BasicContext context = new BasicContext()) {
            for (Callable<EventStream> callable :
                    storagePlugin.getDataForPV(context, pvName, start, end, new DefaultRawPostProcessor())) {
                try (EventStream strm = callable.call()) {
                    for (Event ev : strm) {
                        // logger.debug("Found event at {} {}",
                        // TimeUtils.convertToISO8601String(ev.getEventTimeStamp()),
                        // TimeUtils.convertToHumanReadableString(ev.getEventTimeStamp()));
                        eventCount++;
                    }
                }
            }
            logger.debug("Done counting events");
        }
        return eventCount;
    }

    public void close() {
        this.configService.shutdownNow();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                    "Usage: java edu.stanford.slac.archiverappliance.PB.compression.gztar.Perf <FolderName> <NumYears> <NumDaysForRetrieval>");
            return;
        }

        String folderName = args[0];
        if (!Files.isDirectory(Paths.get(folderName))) {
            System.err.println("Please specify a root folder that exists");
            return;
        }
        int numYears = Integer.parseInt(args[1]);
        int numDays = Integer.parseInt(args[2]);

        short currentYear = TimeUtils.getCurrentYear();

        Perf perf = new Perf(folderName);
        if (!Files.exists(Paths.get(folderName + File.separator + chunkKey + ".tar"))) {
            for (int y = numYears; y >= 0; y--) {
                perf.appendAndTestForYear((short) (currentYear - y), 365 * (numYears - y + 1), 1);
            }
        } else {
            System.out.println("GZTar file already exists");
        }

        // Test retrieval
        Instant end = TimeUtils.getStartOfYear(currentYear).plusSeconds(24 * 60 * 60 * 90);
        perf.testRetrieval(end.minus(1, ChronoUnit.DAYS), end, (1 * 24 * 60 * 60) + 1 + 1); // Precompile
        for (int days = 1; days < numDays; days++) {
            Instant start = end.minus(days, ChronoUnit.DAYS);
            long before = System.currentTimeMillis();
            int events = perf.testRetrieval(
                    start,
                    end,
                    (days * 24 * 60 * 60)
                            + 1
                            + 1); // One for the last known event and one for the event that's exactly at the end time.
            long after = System.currentTimeMillis();
            System.out.println(
                    "Took " + (after - before) + "(ms) to retrieve " + events + " events for " + days + " days");
        }

        perf.close();
    }
}
