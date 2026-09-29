package org.epics.archiverappliance.engine.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.ConnectionLossFields;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tag("localEpics")
public class CnxLostFieldValuesTest {
    private static final Logger logger = LogManager.getLogger(CnxLostFieldValuesTest.class.getName());
    private SIOCSetup ioc = null;
    private ConfigServiceForTests testConfigService;
    private RecordingWriter writer = new RecordingWriter();

    static class RecordingWriter implements Writer {
        public List<Event> recordedEvents = new ArrayList<>();

        @Override
        public int appendData(BasicContext context, String arg0, EventStream stream) throws IOException {
            int count = 0;
            for (Event e : stream) {
                recordedEvents.add(e.makeClone());
                count++;
            }
            return count;
        }

        @Override
        public Event getLastKnownEvent(BasicContext context, String pvName) throws IOException {
            return null;
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        ioc = new SIOCSetup("");
        ioc.startSIOCWithDefaultDB();
        testConfigService = new ConfigServiceForTests(-1);
    }

    @AfterEach
    public void tearDown() throws Exception {
        testConfigService.shutdownNow();
        ioc.stopSIOC();
    }

    @Test
    public void testHeaders() throws Exception {
        String pvName = "UnitTestNoNamingConvention:inactive1";
        ArchiveEngine.archivePV(
                pvName,
                2,
                SamplingMethod.MONITOR,
                writer,
                testConfigService,
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                null,
                false,
                false);
        // Do not wait for connection, just wait for status change to mimic CnxLostTest's setupPV
        logger.info("Started archiving! Not waiting for connection...");

        logger.info("Sending caput 1.0");
        SIOCSetup.caput(pvName, "1.0");
        Thread.sleep(1000);

        logger.info("Sending caput 2.0");
        SIOCSetup.caput(pvName, "2.0");
        Thread.sleep(1000);

        logger.info("Shutting down engine to flush events...");
        testConfigService.getEngineContext().getChannelList().get(pvName).stop();

        logger.info("Flushed " + writer.recordedEvents.size() + " events.");
        boolean firstEventHasStartup = false;
        for (int i = 0; i < writer.recordedEvents.size(); i++) {
            Event e = writer.recordedEvents.get(i);
            DBRTimeEvent dbr = (DBRTimeEvent) e;
            Map<String, String> fields = dbr.getFields();
            boolean hasStartup = fields != null && fields.containsKey(ConnectionLossFields.STARTUP.getFieldName());
            if (i == 0) {
                firstEventHasStartup = hasStartup;
            }
            logger.info("Event " + i + ": value=" + dbr.getSampleValue().getValue() + " hasStartup=" + hasStartup);
        }
        assertTrue(firstEventHasStartup, "First event should have startup field");
    }
}
