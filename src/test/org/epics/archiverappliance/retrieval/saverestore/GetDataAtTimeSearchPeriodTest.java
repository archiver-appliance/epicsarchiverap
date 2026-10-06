/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.saverestore;

import static org.epics.archiverappliance.retrieval.GetDataAtTime.getDataAtTimeForPVFromStores;
import static org.epics.archiverappliance.utils.ui.URIUtils.pluginString;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import edu.stanford.slac.archiverappliance.plain.PlainStorageType;
import org.apache.commons.io.FileUtils;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.retrieval.PVWithData;
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
import java.util.stream.Stream;

/**
 * Regression tests for the <code>searchPeriod</code> parameter of getDataAtTime.
 *
 * <p>searchPeriod is parsed with {@link Period#parse(CharSequence)}, so values such as P1M or P1Y
 * are legal periods. However, <code>Instant.plus/minus(Period)</code> only supports the
 * DAYS unit and throws <code>UnsupportedTemporalTypeException</code> for months and years.
 *
 * <p>Setup: one day of hourly samples on 2024-03-01 (UTC), stored with PARTITION_DAY so that
 * the search window computed from searchPeriod determines which partitions are read.
 */
public class GetDataAtTimeSearchPeriodTest {
    private static ConfigServiceForTests configService;

    private static final short DATA_YEAR = 2024;
    private static final Instant DATA_START = Instant.parse("2024-03-01T00:00:00Z");
    private static final int SAMPLE_COUNT = 24;
    private static final Instant LAST_SAMPLE = DATA_START.plus(SAMPLE_COUNT - 1, ChronoUnit.HOURS);

    private static final Instant NEXT_DAY = Instant.parse("2024-03-02T00:00:00Z");
    private static final Instant THREE_MONTHS_LATER = Instant.parse("2024-06-01T00:00:00Z");

    private static final File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder()
            + File.separator
            + GetDataAtTimeSearchPeriodTest.class.getSimpleName());

    private static final String storageString = pluginString(
            PlainStorageType.PARQUET,
            "localhost",
            "name=" + GetDataAtTimeSearchPeriodTest.class.getSimpleName() + "&rootFolder="
                    + testFolder.getAbsolutePath() + "&partitionGranularity=PARTITION_DAY");

    private static final String pvName = GetDataAtTimeSearchPeriodTest.class.getName();

    private static PlainStoragePlugin getStoragePlugin() throws IOException {
        return (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(storageString, configService);
    }

    @BeforeAll
    public static void setUp() throws Exception {
        configService = new ConfigServiceForTests(1);
        deleteData();
        createTestData();
    }

    @AfterAll
    public static void tearDown() throws IOException {
        deleteData();
    }

    private static void deleteData() throws IOException {
        FileUtils.deleteDirectory(testFolder);
    }

    private static void createTestData() throws IOException {
        ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
        PlainStoragePlugin storagePlugin = getStoragePlugin();
        try (BasicContext context = new BasicContext()) {
            ArrayListEventStream events =
                    new ArrayListEventStream(DATA_YEAR, new RemotableEventStreamDesc(dbrType, pvName, DATA_YEAR));
            for (int i = 0; i < SAMPLE_COUNT; i++) {
                Instant ts = DATA_START.plus(i, ChronoUnit.HOURS);
                events.add(
                        new POJOEvent(dbrType, ts, new ScalarValue<>((double) ts.getEpochSecond()), 0, 0).makeClone());
            }
            storagePlugin.appendData(context, pvName, events);
        }

        try {
            PVTypeInfo typeInfo = new PVTypeInfo(pvName, dbrType, true, 1);
            typeInfo.setDataStores(new String[] {storageString});
            typeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
            configService.updateTypeInfoForPV(pvName, typeInfo);
            configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        } catch (AlreadyRegisteredException ex) {
            throw new IOException(ex);
        }
    }

    public static Stream<Arguments> provideSearchPeriods() {
        return Stream.of(
                // Day-based and week periods: was already  working
                Arguments.of(NEXT_DAY, "P1D", true),
                Arguments.of(THREE_MONTHS_LATER, "P1D", false),
                Arguments.of(THREE_MONTHS_LATER, "P30D", false),
                Arguments.of(THREE_MONTHS_LATER, "P92D", true),
                Arguments.of(THREE_MONTHS_LATER, "P1W", false),
                // Month and year based periods
                Arguments.of(NEXT_DAY, "P1M", true),
                Arguments.of(NEXT_DAY, "P1Y", true),
                Arguments.of(THREE_MONTHS_LATER, "P1M", false),
                Arguments.of(THREE_MONTHS_LATER, "P3M", true),
                Arguments.of(THREE_MONTHS_LATER, "P1Y", true),
                Arguments.of(THREE_MONTHS_LATER, "P1Y2M3D", true));
    }

    @ParameterizedTest(name = "at={0} searchPeriod={1} expectData={2}")
    @MethodSource("provideSearchPeriods")
    void testSearchPeriod(Instant atTime, String searchPeriodStr, boolean expectData) {
        Period searchPeriod = Period.parse(searchPeriodStr);
        PVWithData pvWithData = getDataAtTimeForPVFromStores(pvName, atTime, searchPeriod, configService);

        if (!expectData) {
            Assertions.assertNull(
                    pvWithData,
                    "Data from " + LAST_SAMPLE + " should be outside the search window of " + searchPeriodStr + " from "
                            + atTime);
            return;
        }

        Assertions.assertNotNull(pvWithData, "No data returned at " + atTime + " with searchPeriod " + searchPeriodStr);
        Event event = pvWithData.event();
        Assertions.assertNotNull(event);
        Assertions.assertEquals(
                LAST_SAMPLE,
                event.getEventTimeStamp(),
                "Expected the last sample before " + atTime + " with searchPeriod " + searchPeriodStr);
    }
}
