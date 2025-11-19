/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.saverestore;

import static edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin.PB_PLUGIN_IDENTIFIER;
import static org.epics.archiverappliance.utils.ui.URIUtils.pluginString;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Test a use case reported by William Colocho at SLAC.
 * We have a slowly changing PV that gets updated rarely sometimes.
 * The file is large enough for us to use FileBackedPBEventStream for getDataAtTime.
 * We ask for samples at specified times and make sure we get the latest sample at that time.
 * @author mshankar
 *
 */
@Tag("integration")
public class ExactSampleTest {
    private static final String pvName = ExactSampleTest.class.getSimpleName();
    private static final short prevYear = (short) (TimeUtils.getCurrentYear() - 1);
    private static final TomcatSetup tomcatSetup = new TomcatSetup();
    private static final String ltsFolderName = System.getenv("ARCHAPPL_LONG_TERM_FOLDER");
    private static final File ltsFolder = new File(ltsFolderName);
    private ConfigServiceForTests configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());

        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }

        addPVToCluster();
        generateOneFileWithWellKnownPoints();
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();

        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
    }

    private PVTypeInfo addPVToCluster() throws Exception {
        // Load a sample PVTypeInfo from a prototype file.
        JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(new File(
                "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json"))));
        PVTypeInfo srcPVTypeInfo = new PVTypeInfo();
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        decoder.decode(srcPVTypeInfoJSON, srcPVTypeInfo);
        PVTypeInfo newPVTypeInfo = new PVTypeInfo(pvName, srcPVTypeInfo);
        newPVTypeInfo.setPaused(true);
        newPVTypeInfo.setApplianceIdentity("appliance0");
        newPVTypeInfo.setChunkKey(pvName + ":");
        Assertions.assertEquals(
                newPVTypeInfo.getPvName(),
                pvName,
                "Expecting PV typeInfo for " + pvName + "; instead it is " + srcPVTypeInfo.getPvName());
        JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
        GetUrlContent.postDataAndGetContentAsJSONObject(
                ConfigServiceForTests.MGMT_URL + "/putPVTypeInfo?pv="
                        + URLEncoder.encode(pvName, StandardCharsets.UTF_8) + "&createnew=true",
                encoder.encode(newPVTypeInfo));
        return newPVTypeInfo;
    }

    private void generateDataBetweenDays(PlainStoragePlugin plugin, int startDay, int endDay) throws IOException {
        ArrayListEventStream strm = new ArrayListEventStream(
                86400, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, prevYear));

        for (int i = startDay; i < endDay; i++) {
            strm.add(new POJOEvent(
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(prevYear, i, 0)),
                    new ScalarValue<Double>((double) i),
                    0,
                    0));
        }
        try (BasicContext context = new BasicContext()) {
            plugin.appendData(context, pvName, strm);
        }
    }

    private static record TestPoint(long epochSeconds, double value) {}
    ;

    // 2025-10-27T00:47:48.534Z        Oct/26/2025 17:47:48 PDT        10.515  0       0
    // 2025-10-27T13:02:21.585Z        Oct/27/2025 06:02:21 PDT        9.8     0       0
    // 2025-10-28T00:55:07.757Z        Oct/27/2025 17:55:07 PDT        10.515  0       0
    // 2025-10-28T13:02:45.939Z        Oct/28/2025 06:02:45 PDT        9.8     0       0
    // 2025-10-29T01:02:36.468Z        Oct/28/2025 18:02:36 PDT        11.0    0       0
    // 2025-10-29T01:11:43.048Z        Oct/28/2025 18:11:43 PDT        12.254  0       0
    // 2025-10-29T12:59:20.510Z        Oct/29/2025 05:59:20 PDT        9.091   0       0
    // 2025-10-31T01:10:36.986Z        Oct/30/2025 18:10:36 PDT        12.254  0       0
    // 2025-10-31T13:17:38.084Z        Oct/31/2025 06:17:38 PDT        9.091   0       0

    private TestPoint[] knownPoints = new TestPoint[] {
        new TestPoint(Instant.parse(prevYear + "-10-27T00:47:48.534Z").getEpochSecond(), 10.515),
        new TestPoint(Instant.parse(prevYear + "-10-27T13:02:21.585Z").getEpochSecond(), 9.8),
        new TestPoint(Instant.parse(prevYear + "-10-28T00:55:07.757Z").getEpochSecond(), 10.515),
        new TestPoint(Instant.parse(prevYear + "-10-28T13:02:45.939Z").getEpochSecond(), 9.8),
        new TestPoint(Instant.parse(prevYear + "-10-29T01:02:36.468Z").getEpochSecond(), 11.0),
        new TestPoint(Instant.parse(prevYear + "-10-29T01:11:43.048Z").getEpochSecond(), 12.254),
        new TestPoint(Instant.parse(prevYear + "-10-29T12:59:20.510Z").getEpochSecond(), 9.091),
        new TestPoint(Instant.parse(prevYear + "-10-31T01:10:36.986Z").getEpochSecond(), 12.254),
        new TestPoint(Instant.parse(prevYear + "-10-31T13:17:38.084Z").getEpochSecond(), 9.091)
    };

    private void generateOneFileWithWellKnownPoints() throws IOException {
        PlainStoragePlugin plugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pluginString(
                        PB_PLUGIN_IDENTIFIER,
                        "localhost",
                        "name=" + pvName + "&rootFolder=" + ltsFolderName + "&partitionGranularity=PARTITION_YEAR"),
                configService);
        // We generate 1Hz data all the way till mid Oct
        generateDataBetweenDays(plugin, 0, 290);

        // Now add the known points in October
        ArrayListEventStream strm = new ArrayListEventStream(
                86400, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, prevYear));
        for (TestPoint tp : knownPoints) {
            strm.add(new POJOEvent(
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    Instant.ofEpochSecond(tp.epochSeconds),
                    new ScalarValue<Double>(tp.value),
                    0,
                    0));
        }
        try (BasicContext context = new BasicContext()) {
            plugin.appendData(context, pvName, strm);
        }

        // 1Hz data rest of the year
        generateDataBetweenDays(plugin, 310, 365);
    }

    @SuppressWarnings("unchecked")
    private void getDataAtTime(Instant when, Instant expectedTimestamp, double expectedValue) throws IOException {
        JSONArray pvs = new JSONArray();
        pvs.add(pvName);
        JSONObject resp = GetUrlContent.postDataAndGetContentAsJSONObject(
                ConfigServiceForTests.GETDATAATTIME_URL + "?at=" + TimeUtils.convertToISO8601String(when), pvs);
        JSONObject dataForPV = (JSONObject) resp.get(pvName);
        Assertions.assertNotNull(dataForPV, "Expecting data for PV " + pvName + " for time " + when);
        long sampleSecs = (long) dataForPV.get("secs");
        Assertions.assertEquals(
                expectedTimestamp.getEpochSecond(),
                sampleSecs,
                "Expected timestamp mismatch for PV " + pvName + " for time " + when + " Expecting "
                        + TimeUtils.convertToISO8601String(expectedTimestamp) + " but got "
                        + TimeUtils.convertToISO8601String(Instant.ofEpochSecond(sampleSecs))
                        + "with value " + dataForPV.get("val"));
        Assertions.assertEquals(
                expectedValue,
                (double) dataForPV.get("val"),
                "Expected value mismatch for PV " + pvName + " for time " + when
                        + " Expecting " + expectedValue + " but got "
                        + dataForPV.get("val"));
    }

    @Test
    public void testGetDataAtKnownTimes() throws Exception {
        // William's test case was 2025-10-30T04:00:03.000-07:00
        // This is UTC 2025-10-30T11:00:03.000Z
        getDataAtTime(
                Instant.parse(prevYear + "-10-30T11:00:03.000Z"),
                Instant.parse(prevYear + "-10-29T12:59:20.510Z"),
                9.091);
    }
}
