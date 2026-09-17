package org.epics.archiverappliance.retrieval;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.HashMapEvent;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.time.Instant;
import java.time.Period;
import java.util.HashMap;
import java.util.Map;

class MetaDataTimeFetchTest {

    private ConfigService configService;
    private PVTypeInfo typeInfo;

    @BeforeEach
    void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        typeInfo = new PVTypeInfo("testPV", ArchDBRTypes.DBR_SCALAR_STRING, true, 1);
        typeInfo.setSamplingMethod(PolicyConfig.SamplingMethod.MONITOR);
    }

    @Test
    void testGetMetadataStartWithMock() {
        HashMap<String, String> rawMetadata = new HashMap<>(Map.of("key", "startValue"));

        HashMapEvent mockEvent = new HashMapEvent(ArchDBRTypes.DBR_SCALAR_STRING, new HashMap<>());
        mockEvent.setFieldValues(rawMetadata, false);

        PVWithData mockData = new PVWithData("testPV", mockEvent);

        Instant start = Instant.now();
        Instant end = start.plusSeconds(3600);

        try (MockedStatic<GetDataAtTime> mocked = mockStatic(GetDataAtTime.class)) {
            mocked.when(() -> GetDataAtTime.getDataAtTimeForPVFromStores(
                            eq("testPV"), eq(start), any(), eq(configService)))
                    .thenReturn(mockData);

            MetaDataTime metaDataTime = MetaDataTime.fromRequestStrings("true", "false", "false");
            Map<String, String> metadata =
                    metaDataTime.getMetadata(typeInfo, "testPV", null, start, end, configService);

            HashMap<String, String> expected = new HashMap<>(Map.of("key", "startValue"));
            assertEquals(expected, metadata);
        }
    }

    @Test
    void testGetMetadataEndWithMock() {
        HashMap<String, String> rawMetadata = new HashMap<>(Map.of("key", "endValue"));

        HashMapEvent mockEvent = new HashMapEvent(ArchDBRTypes.DBR_SCALAR_STRING, new HashMap<>());
        mockEvent.setFieldValues(rawMetadata, false);

        PVWithData mockData = new PVWithData("testPV", mockEvent);

        Instant start = Instant.now();
        Instant end = start.plusSeconds(3600);

        try (MockedStatic<GetDataAtTime> mocked = mockStatic(GetDataAtTime.class)) {
            mocked.when(() ->
                            GetDataAtTime.getDataAtTimeForPVFromStores(eq("testPV"), eq(end), any(), eq(configService)))
                    .thenReturn(mockData);

            MetaDataTime metaDataTime = MetaDataTime.fromRequestStrings("false", "true", "false");
            Map<String, String> metadata =
                    metaDataTime.getMetadata(typeInfo, "testPV", null, start, end, configService);

            HashMap<String, String> expected = new HashMap<>(Map.of("key", "endValue"));
            assertEquals(expected, metadata);
        }
    }

    @Test
    void testGetMetadataLatestWithMock() {
        ApplianceInfo applianceInfo = mock(ApplianceInfo.class);
        when(applianceInfo.getEngineURL()).thenReturn("http://localhost:8080");

        Map<String, String> expectedMetadata = new JSONObject();
        expectedMetadata.put("key", "latestValue");

        try (MockedStatic<GetUrlContent> mocked = mockStatic(GetUrlContent.class)) {
            mocked.when(() -> GetUrlContent.getURLContentAsJSONObject(anyString()))
                    .thenReturn(expectedMetadata);

            MetaDataTime metaDataTime = MetaDataTime.fromRequestStrings("false", "false", "true");
            Map<String, String> metadata =
                    metaDataTime.getMetadata(typeInfo, "testPV", applianceInfo, null, null, null);
            assertEquals(expectedMetadata, metadata);
        }
    }

    @Test
    void testGetMetadataAllWithMock() {
        ApplianceInfo applianceInfo = mock(ApplianceInfo.class);
        when(applianceInfo.getEngineURL()).thenReturn("http://localhost:8080");

        HashMap<String, String> startMetadata = new HashMap<>(Map.of("sharedKey", "startValue"));
        HashMap<String, String> endMetadata = new HashMap<>(Map.of("sharedKey", "endValue"));
        Map<String, String> latestMetadata = new JSONObject();
        latestMetadata.put("sharedKey", "latestValue");

        HashMapEvent mockStartEvent = new HashMapEvent(ArchDBRTypes.DBR_SCALAR_STRING, new HashMap<>());
        mockStartEvent.setFieldValues(startMetadata, false);
        PVWithData mockStartData = new PVWithData("testPV", mockStartEvent);

        HashMapEvent mockEndEvent = new HashMapEvent(ArchDBRTypes.DBR_SCALAR_STRING, new HashMap<>());
        mockEndEvent.setFieldValues(endMetadata, false);
        PVWithData mockEndData = new PVWithData("testPV", mockEndEvent);

        Instant start = Instant.now();
        Instant end = start.plusSeconds(3600);

        try (MockedStatic<GetDataAtTime> mockedGetData = mockStatic(GetDataAtTime.class);
                MockedStatic<GetUrlContent> mockedGetUrl = mockStatic(GetUrlContent.class)) {

            mockedGetData
                    .when(() -> GetDataAtTime.getDataAtTimeForPVFromStores(
                            eq("testPV"), eq(start), any(), eq(configService)))
                    .thenReturn(mockStartData);

            mockedGetData
                    .when(() ->
                            GetDataAtTime.getDataAtTimeForPVFromStores(eq("testPV"), eq(end), any(), eq(configService)))
                    .thenReturn(mockEndData);

            mockedGetUrl
                    .when(() -> GetUrlContent.getURLContentAsJSONObject(anyString()))
                    .thenReturn(latestMetadata);

            MetaDataTime metaDataTime = MetaDataTime.fromRequestStrings("true", "true", "true");
            Map<String, String> metadata =
                    metaDataTime.getMetadata(typeInfo, "testPV", applianceInfo, start, end, configService);

            assertEquals(3, metadata.size());
            assertEquals("startValue", metadata.get("start_sharedKey"));
            assertEquals("endValue", metadata.get("end_sharedKey"));
            assertEquals("latestValue", metadata.get("sharedKey"));
        }
    }

    @Test
    void testGetMetadataUsesLongSearchPeriod() {
        HashMapEvent mockEvent = new HashMapEvent(ArchDBRTypes.DBR_SCALAR_STRING, new HashMap<>());
        mockEvent.setFieldValues(new HashMap<>(Map.of("key", "longIntervalValue")), false);
        PVWithData mockData = new PVWithData("testPV", mockEvent);
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        Instant end = Instant.parse("2026-02-01T00:00:00Z");

        try (MockedStatic<GetDataAtTime> mocked = mockStatic(GetDataAtTime.class)) {
            mocked.when(() -> GetDataAtTime.getDataAtTimeForPVFromStores(
                            eq("testPV"), eq(start), eq(Period.ofDays(31)), eq(configService)))
                    .thenReturn(mockData);

            MetaDataTime metaDataTime = MetaDataTime.fromRequestStrings("true", "false", "false");
            Map<String, String> metadata = metaDataTime.getMetadata(
                    typeInfo, "testPV", null, start, end, MetaDataTime.searchPeriodBetween(start, end), configService);

            assertEquals(Map.of("key", "longIntervalValue"), metadata);
            mocked.verify(() ->
                    GetDataAtTime.getDataAtTimeForPVFromStores("testPV", start, Period.ofDays(31), configService));
        }
    }
}
