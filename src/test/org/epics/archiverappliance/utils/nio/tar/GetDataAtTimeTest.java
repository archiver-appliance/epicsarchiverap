/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.nio.tar;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.retrieval.GetDataAtTime;
import org.epics.archiverappliance.retrieval.PVWithData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.Period;
import java.util.Map;
import java.util.Random;

/**
 * Test GetDataAtTime using its internal method getDataAtTimeForPVFromStores
 * We generate PVTypeInfo's for a few PVs; generate gztar data and test retrieval
 * We generate one data point per hour 11 seconds past the hour
 * @author mshankar
 *
 */
public class GetDataAtTimeTest {
    private static final Logger logger = LogManager.getLogger();
    // pvName to epochSecond multiplier for value
    private static Map<String, Integer> pvNames =
            Map.of("epics:arch:gztartest0", 1, "epics:arch:gztartest1", 2, "epics:arch:gztartest2", 3);
    private static ConfigServiceForTests configService;
    private static short year = (short) (TimeUtils.getCurrentYear() - 1);
    private static final String rootFolderStr =
            ConfigServiceForTests.getDefaultPBTestFolder() + "/gztar/GetDataAtTimeTest";

    @BeforeAll
    public static void setUp() throws Exception {
        configService = new ConfigServiceForTests(1);
    }

    @AfterAll
    public static void tearDown() throws IOException {
        deleteData();
    }

    private static void deleteData() throws IOException {
        FileUtils.cleanDirectory(new File(rootFolderStr));
        FileUtils.deleteDirectory(new File(rootFolderStr));
    }

    private static void createTestData(String plugin) throws IOException {
        File rootFolder = new File(rootFolderStr);
        FileUtils.deleteDirectory(rootFolder);
        Path pvPath = Paths.get(rootFolderStr, "epics/arch/gztartest");
        logger.debug("Creating folder {}", pvPath.getParent().toFile().toString());
        assert pvPath.getParent().toFile().mkdirs();

        String pluginURI = plugin + "://localhost?name=XLTS&rootFolder="
                + URLEncoder.encode("gztar://" + rootFolderStr, "UTF-8") + "&partitionGranularity=PARTITION_DAY";
        StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(pluginURI, configService);

        for (String pvName : pvNames.keySet()) {
            try (BasicContext context = new BasicContext()) {
                for (int day = 0; day < 365; day++) {
                    ArrayListEventStream testData = new ArrayListEventStream(
                            24 * 60 * 60, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
                    int startofdayinseconds = day * 24 * 60 * 60;
                    for (int secondintoday = 11; secondintoday < 24 * 60 * 60; secondintoday += 3600) {
                        Instant dataTs = TimeUtils.convertFromYearSecondTimestamp(
                                new YearSecondTimestamp(year, startofdayinseconds + secondintoday, 0));
                        testData.add(new POJOEvent(
                                        ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                        dataTs,
                                        new ScalarValue<Long>(dataTs.getEpochSecond() * pvNames.get(pvName)),
                                        0,
                                        0)
                                .makeClone());
                    }
                    storagePlugin.appendData(context, pvName, testData);
                }
            }
            try {
                PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
                String[] dataStores = new String[] {pluginURI};
                typeInfo.setDataStores(dataStores);
                typeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
                configService.updateTypeInfoForPV(pvName, typeInfo);
                configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
            } catch (AlreadyRegisteredException ex) {
                throw new IOException(ex);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"pb", "parquet"})
    public void testGetDataAtTime(String plugin) throws Exception {
        createTestData(plugin);

        Period searchPeriod = Period.parse("P1D");
        Random random = new Random();
        for (int day = 1; day < 365; day += 60) {
            int startofdayinseconds = day * 24 * 60 * 60;
            Instant dataTs = TimeUtils.convertFromYearSecondTimestamp(
                    new YearSecondTimestamp(year, startofdayinseconds + random.nextInt(86400 - 1), 0));
            logger.debug(
                    "Looking for data at {} epoch {}",
                    TimeUtils.convertToHumanReadableString(dataTs),
                    dataTs.getEpochSecond());
            try (BasicContext context = new BasicContext()) {
                for (String pvName : pvNames.keySet()) {
                    PVWithData pvDatas =
                            GetDataAtTime.getDataAtTimeForPVFromStores(pvName, dataTs, searchPeriod, configService);
                    DBRTimeEvent event = (DBRTimeEvent) pvDatas.event();
                    Assertions.assertNotNull(
                            event, "Getting at time " + dataTs + " for PV " + pvName + " returns null?");
                    logger.info(
                            "pvDatas for {} is {}",
                            pvDatas.pvName(),
                            TimeUtils.convertToHumanReadableString(event.getEventTimeStamp()));
                    Assertions.assertTrue(
                            Math.abs((event.getEventTimeStamp().getEpochSecond() - dataTs.getEpochSecond())) <= 7200,
                            "Expected a sample on or after " + (dataTs.getEpochSecond() - 7200) + " got "
                                    + event.getEventTimeStamp().getEpochSecond());
                    Assertions.assertTrue(
                            ((double) event.getEventTimeStamp().getEpochSecond() * pvNames.get(pvName))
                                    == ((double)
                                            event.getSampleValue().getValue().doubleValue()),
                            "Expected value "
                                    + ((double) event.getEventTimeStamp().getEpochSecond() * pvNames.get(pvName))
                                    + " got "
                                    + ((double)
                                            event.getSampleValue().getValue().doubleValue()));
                }
            }
        }
    }
}
