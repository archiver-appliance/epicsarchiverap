package org.epics.archiverappliance.engine.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class ArchiveChannelMetadataPeriodTest {
    @Test
    void shouldUseDefaultRefreshPeriodWhenUnset() {
        Assertions.assertEquals(
                ArchiveChannel.SAVE_META_DATA_PERIOD_SECS, ArchiveChannel.resolveMetaDataPeriodSecs(null));
        Assertions.assertEquals(
                ArchiveChannel.SAVE_META_DATA_PERIOD_SECS, ArchiveChannel.resolveMetaDataPeriodSecs(new Properties()));
    }

    @Test
    void shouldClampRefreshPeriodToTwelveHours() {
        Properties properties = new Properties();
        properties.setProperty(ArchiveChannel.SAVE_META_DATA_PERIOD_SECS_PROPERTY, "1");
        Assertions.assertEquals(
                ArchiveChannel.MIN_SAVE_META_DATA_PERIOD_SECS, ArchiveChannel.resolveMetaDataPeriodSecs(properties));
    }

    @Test
    void shouldUseConfiguredRefreshPeriodWhenValid() {
        Properties properties = new Properties();
        properties.setProperty(ArchiveChannel.SAVE_META_DATA_PERIOD_SECS_PROPERTY, "50000");
        Assertions.assertEquals(50000, ArchiveChannel.resolveMetaDataPeriodSecs(properties));
    }
}
