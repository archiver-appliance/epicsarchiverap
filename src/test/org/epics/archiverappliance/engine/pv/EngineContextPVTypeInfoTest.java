package org.epics.archiverappliance.engine.pv;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.PVTypeInfoEvent;
import org.epics.archiverappliance.config.PVTypeInfoEvent.ChangeType;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.model.MonitoredArchiveChannel;
import org.epics.archiverappliance.engine.test.FakeWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EngineContextPVTypeInfoTest {
    private ConfigServiceForTests configService;

    @AfterEach
    void tearDown() {
        if (configService != null) {
            configService.shutdownNow();
        }
    }

    @Test
    void deletedTypeInfoStopsAndRemovesChannelAfterTypeInfoLookupIsEmpty() throws Exception {
        configService = new ConfigServiceForTests(-1);
        EngineContext engineContext = configService.getEngineContext();
        String pvName = "EngineContextPVTypeInfoTest:deletedPV";
        ArchiveChannel channel = new MonitoredArchiveChannel(
                pvName,
                new FakeWriter(),
                2,
                null,
                1.0,
                configService,
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                null,
                engineContext.assignJCACommandThread(pvName, null),
                false);
        engineContext.getChannelList().put(pvName, channel);

        PVTypeInfo deletedTypeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, false, 1);
        PVTypeInfoEvent event = new PVTypeInfoEvent(pvName, deletedTypeInfo, ChangeType.TYPEINFO_DELETED);

        assertDoesNotThrow(() -> configService.getETLLookup().pvTypeInfoChanged(event));
        assertDoesNotThrow(() -> engineContext.pvTypeInfoChanged(event));
        assertFalse(engineContext.getChannelList().containsKey(pvName));
    }
}
