/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.saverestore;

import edu.stanford.slac.archiverappliance.plain.PlainPBStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.GetDataAtTime;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Test GetDataAtTime's internal method getDataAtTimeForPVFromStores
 * We want to make sure we pick up both the .VAL's and the .FIELDS's in various siutuations.
 * Generate a PB file with a days worth of 1Hz PV data ending at the current time.
 * Approx 12 hrs before the current time, add in all the fields as if it were the daily refresh of the metafields.
 * At 3, 6, 9 hrs before the current time, update some field as if someone did a caput
 * If we ask for getDataAtTimeForPVFromStores at various points in time, we should get various results.
 * @author mshankar
 *
 */
public class GetDataAtTimeForPVFromStoresTest {
    private static final Logger logger = LogManager.getLogger(GetDataAtTimeForPVFromStoresTest.class.getName());
    private static String pvName = "GetDataAtTimeForPVFromStoresTest";
    private static ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
    private static ConfigServiceForTests configService;
    private static short currentYear = TimeUtils.getCurrentYear();
    private static Instant now = TimeUtils.now();
    private static Instant yesterday = now.minus(86400, ChronoUnit.SECONDS);
    private static Instant ago_3hrs = now.minus(60 * 60 * 3, ChronoUnit.SECONDS);
    private static Instant ago_6hrs = now.minus(60 * 60 * 6, ChronoUnit.SECONDS);
    private static Instant ago_9hrs = now.minus(60 * 60 * 9, ChronoUnit.SECONDS);
    private static Instant ago_12hrs = now.minus(60 * 60 * 12, ChronoUnit.SECONDS);

    static File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder()
            + File.separator
            + GetDataAtTimeForPVFromStoresTest.class.getSimpleName());
    static String storagePBPluginString =
            "pb://localhost?name=" + GetDataAtTimeForPVFromStoresTest.class.getSimpleName() + "&rootFolder="
                    + testFolder.getAbsolutePath() + "&partitionGranularity=PARTITION_YEAR";

    static {
        try {
            configService = new ConfigServiceForTests(-1);
        } catch (ConfigException e) {
            throw new RuntimeException(e);
        }
    }

    private static PlainPBStoragePlugin getStoragePlugin() throws IOException {
        return (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin(storagePBPluginString, configService);
    }

    @BeforeAll
    public static void setUp() throws Exception {
        deleteData();
        createTestData();
    }

    @AfterAll
    public static void tearDown() throws IOException {
        deleteData();
    }

    private static void deleteData() throws IOException {
        FileUtils.deleteDirectory(new File(getStoragePlugin().getRootFolder()));
    }

    private static void createTestData() throws IOException {
        PlainPBStoragePlugin storagePlugin = getStoragePlugin();

        try (BasicContext context = new BasicContext()) {
            ArrayListEventStream events = new ArrayListEventStream(
                    currentYear, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));
            Instant dataTs = yesterday;
            while (dataTs.isBefore(now)) {
                DBRTimeEvent ev = (DBRTimeEvent)
                        new POJOEvent(dbrType, dataTs, new ScalarValue<Long>(dataTs.getEpochSecond()), 0, 0)
                                .makeClone();
                if (dataTs.equals(ago_12hrs)) {
                    logger.info("Daily refresh of meta at -12hrs");
                    ev.addFieldValue("HIHI", "HIHI_@_12");
                    ev.addFieldValue("LOLO", "LOLO_@_12");
                    ev.addFieldValue("HIGH", "HIGH_@_12");
                    ev.addFieldValue("LOW", "LOW_@_12");
                }
                if (dataTs.equals(ago_9hrs)) {
                    logger.info("caput of HIHI at -9hrs");
                    ev.addFieldValue("HIHI", "HIHI_@_9");
                }
                if (dataTs.equals(ago_6hrs)) {
                    logger.info("caput of HIHI and LOLO at -6hrs");
                    ev.addFieldValue("HIHI", "HIHI_@_6");
                    ev.addFieldValue("LOLO", "LOLO_@_6");
                }
                if (dataTs.equals(ago_3hrs)) {
                    logger.info("caput of HIHI at -3hrs");
                    ev.addFieldValue("HIHI", "HIHI_@_3");
                }
                events.add(ev);
                dataTs = dataTs.plus(1, ChronoUnit.SECONDS);
            }
            storagePlugin.appendData(context, pvName, events);
        }

        try {
            PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
            String[] dataStores = new String[] {storagePBPluginString};
            typeInfo.setDataStores(dataStores);
            typeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
            configService.updateTypeInfoForPV(pvName, typeInfo);
            configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        } catch (AlreadyRegisteredException ex) {
            throw new IOException(ex);
        }
    }

    public static Stream<Arguments> provideTimesAndFields() {
        return Stream.of(
                Arguments.of(
                        now,
                        Map.of(
                                "HIHI", "HIHI_@_3",
                                "LOLO", "LOLO_@_6",
                                "HIGH", "HIGH_@_12",
                                "LOW", "LOW_@_12")),
                Arguments.of(
                        now.minus(4, ChronoUnit.HOURS),
                        Map.of(
                                "HIHI", "HIHI_@_6",
                                "LOLO", "LOLO_@_6",
                                "HIGH", "HIGH_@_12",
                                "LOW", "LOW_@_12")),
                Arguments.of(
                        now.minus(7, ChronoUnit.HOURS),
                        Map.of(
                                "HIHI", "HIHI_@_9",
                                "LOLO", "LOLO_@_12",
                                "HIGH", "HIGH_@_12",
                                "LOW", "LOW_@_12")),
                Arguments.of(
                        now.minus(10, ChronoUnit.HOURS),
                        Map.of(
                                "HIHI", "HIHI_@_12",
                                "LOLO", "LOLO_@_12",
                                "HIGH", "HIGH_@_12",
                                "LOW", "LOW_@_12")),
                Arguments.of(now.minus(16, ChronoUnit.HOURS), null));
    }

    @ParameterizedTest
    @MethodSource("provideTimesAndFields")
    public void testGetData(Instant when, Map<String, String> expectedFieldVals) throws Exception {
        testGetDataAsOf(when, expectedFieldVals);
    }

    public void testGetDataAsOf(Instant when, Map<String, String> expectedFieldVals) throws Exception {
        Period searchPeriod = Period.parse("P1D");
        try (BasicContext context = new BasicContext()) {
            HashMap<String, HashMap<String, Object>> pvDatas =
                    GetDataAtTime.testGetDataAtTimeForPVFromStores(pvName, when, searchPeriod, configService);
            HashMap<String, Object> pvData = pvDatas.get(pvName);
            Assertions.assertNotNull(pvData, "Getting at time " + when + " returns null?");
            logger.info(JSONValue.toJSONString(pvDatas));
            Assertions.assertTrue(
                    Math.abs(((long) pvData.get("secs")) - when.getEpochSecond()) < 2,
                    "Expected " + when.getEpochSecond() + " got " + pvData.get("secs"));
            @SuppressWarnings("unchecked")
            HashMap<String, String> metas = (HashMap<String, String>) pvData.get("meta");
            if (expectedFieldVals == null) {
                Assertions.assertNull(metas);
            } else {
                for (String key : expectedFieldVals.keySet()) {
                    Assertions.assertNotNull(metas.get(key));
                    Assertions.assertEquals(metas.get(key), expectedFieldVals.get(key));
                }
                for (String key : metas.keySet()) {
                    // Make sure every key in metas is expected.
                    Assertions.assertTrue(expectedFieldVals.containsKey(key), "Unexpected key " + key);
                }
            }
        }
    }
}
