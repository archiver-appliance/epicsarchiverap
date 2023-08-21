/*
 * Copyright (C) 2023 European Spallation Source ERIC.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 */
package org.epics.archiverappliance.engine.V4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.test.MemBufWriter;
import org.epics.pva.data.PVADouble;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVATimeStamp;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.ServerPV;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.getReceivedEvents;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.startArchivingPV;
import static org.junit.Assert.*;

/**
 * Test to check the metadata stored with the pv as it changes
 * and to check that the data is stored once a day.
 */
public class ChangedFieldsTest {

    private static final Logger logger = LogManager.getLogger(ChangedFieldsTest.class.getName());

    private ConfigService configService;
    private PVAServer pvaServer;

    @Before
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(new File("./bin"));
        pvaServer = new PVAServer();
    }

    @After
    public void tearDown() {
        configService.shutdownNow();
        pvaServer.close();
    }

    /**
     * Test for a pv changing other values than 'value'
     * <p>
     *
     * @throws InterruptedException interruptions
     */
    @Test
    public void testSetFieldValues() throws Exception {

        String pvName = "PV:" + ChangedFieldsTest.class.getName() + ":"
                + UUID.randomUUID();

        logger.info("Starting pvAccess test for pv " + pvName);
        Instant firstInstant = Instant.now();
        PVATimeStamp timeStamp = new PVATimeStamp(firstInstant);
        String struct_name = "epics:nt/NTScalar:1.0";
        var value = new PVAString("value", "value string");
        var alarm = new PVAStructure("alarm", "alarm_t",
                new PVAInt("status", 0), new PVAInt("severity", 0));

        var limitLow = new PVADouble("limitLow", 1.0);
        var limitHigh = new PVADouble("limitHigh", 1.0);
        var description = new PVAString("description", "DESC");
        var units = new PVAString("units", "kHz");
        var display = new PVAStructure("display", "display_t",
                limitLow,
                limitHigh,
                description,
                units);

        var c_limitLow = new PVADouble("limitLow", 1.0);
        var c_limitHigh = new PVADouble("limitHigh", 1.0);
        var minStep = new PVADouble("minStep", 1.0);
        var control = new PVAStructure("control", "control_t",
                c_limitLow,
                c_limitHigh,
                minStep);

        var lowAlarmLimit = new PVAInt("lowAlarmLimit", 1);
        var lowWarningLimit = new PVAInt("lowWarningLimit", 1);
        var highWarningLimit = new PVAInt("highWarningLimit", 1);
        var highAlarmLimit = new PVAInt("highAlarmLimit", 1);
        var hysteresis = new PVAInt("hysteresis", 1);
        var valueAlarm = new PVAStructure("valueAlarm", "valueAlarm_t",
                lowAlarmLimit,
                lowWarningLimit,
                highWarningLimit,
                highAlarmLimit,
                hysteresis);

        var extraString = new PVAString("extra", "extra value");
        var extraArchivedString = new PVAString("extraArchived", "extra archived value");

        PVAStructure pvaStructure = new PVAStructure("struct name", struct_name, value,
                timeStamp, alarm, display, control, valueAlarm, extraString, extraArchivedString);

        HashMap<Instant, HashMap<String, String>> expectedInstantFieldValues = new HashMap<>();
        HashMap<String, String> initFieldValues = new HashMap<>();
        initFieldValues.put("cnxlostepsecs", "0");
        initFieldValues.put("startup", "true");
        initFieldValues.put("cnxregainedepsecs", Long.toString(firstInstant.getEpochSecond() + 1));
        initFieldValues.put("timeStamp.nanoseconds", Integer.toString(firstInstant.getNano()));
        initFieldValues.put("timeStamp.userTag", "0");
        initFieldValues.put("timeStamp.secondsPastEpoch", Long.toString(firstInstant.getEpochSecond()));
        initFieldValues.put("alarm.status", "0");
        initFieldValues.put("alarm.severity", "0");
        initFieldValues.put("display.limitLow", "1.0");
        initFieldValues.put("display.limitHigh", "1.0");
        initFieldValues.put("display.description", "DESC");
        initFieldValues.put("display.units", "kHz");
        initFieldValues.put("control.limitLow", "1.0");
        initFieldValues.put("control.limitHigh", "1.0");
        initFieldValues.put("control.minStep", "1.0");
        initFieldValues.put("valueAlarm.lowAlarmLimit", "1");
        initFieldValues.put("valueAlarm.lowWarningLimit", "1");
        initFieldValues.put("valueAlarm.highWarningLimit", "1");
        initFieldValues.put("valueAlarm.highAlarmLimit", "1");
        initFieldValues.put("valueAlarm.hysteresis", "1");
        initFieldValues.put("extra", "extra value");
        initFieldValues.put("extraArchived", "extra archived value");

        expectedInstantFieldValues.put(firstInstant, initFieldValues);

        ServerPV serverPV = pvaServer.createPV(pvName, pvaStructure);

        var type = ArchDBRTypes.DBR_SCALAR_STRING;
        MemBufWriter writer = new MemBufWriter(pvName, type);
        startArchivingPV(pvName, writer, configService, type, true, new String[]{"extraArchived", "noMetaField"});
        long samplingPeriodMilliSeconds = 100;
        try {
            value.setValue(new PVAString("value", "2 value string"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
        Instant instant = Instant.now();
        timeStamp.set(instant);
        try {
            serverPV.update(pvaStructure);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        expectedInstantFieldValues.put(instant, new HashMap<>());

        // Update all fields except value

        Thread.sleep(samplingPeriodMilliSeconds);
        try {
            limitLow.set(2.0);
            limitHigh.set(2.0);
            description.set("2.0");
            units.set("2.0");
            c_limitLow.set(2.0);
            c_limitHigh.set(2.0);
            minStep.set(2.0);
            lowAlarmLimit.set(2);
            lowWarningLimit.set(2);
            highWarningLimit.set(2);
            highAlarmLimit.set(2);
            hysteresis.set(2);

        } catch (Exception e) {
            fail(e.getMessage());
        }
        instant = Instant.now();
        timeStamp.set(instant);
        try {
            serverPV.update(pvaStructure);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        HashMap<String, String> allFields = new HashMap<>();
        expectedInstantFieldValues.put(instant, allFields);

        // Update extra field
        Thread.sleep(samplingPeriodMilliSeconds);
        try {
            extraString.set("2.0");

        } catch (Exception e) {
            fail(e.getMessage());
        }
        instant = Instant.now();
        timeStamp.set(instant);
        try {
            serverPV.update(pvaStructure);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        // Update extraArchived field
        Thread.sleep(samplingPeriodMilliSeconds);
        try {
            extraArchivedString.set("extraArchived2");

        } catch (Exception e) {
            fail(e.getMessage());
        }
        instant = Instant.now();
        timeStamp.set(instant);
        try {
            serverPV.update(pvaStructure);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        HashMap<String, String> extraFields = new HashMap<>();
        extraFields.put("extraArchived", "extraArchived2");
        expectedInstantFieldValues.put(instant, extraFields);

        Thread.sleep(samplingPeriodMilliSeconds);

        // Update one field
        try {
            limitHigh.set(3.0);

        } catch (Exception e) {
            fail(e.getMessage());
        }
        instant = Instant.now();
        timeStamp.set(instant);
        try {
            serverPV.update(pvaStructure);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        HashMap<String, String> oneField = new HashMap<>();
        oneField.put("HOPR", "3.0");
        expectedInstantFieldValues.put(instant, oneField);

        Thread.sleep(samplingPeriodMilliSeconds);

        double secondsToBuffer = configService.getEngineContext().getWritePeriod();
        // Need to wait for the writer to write all the received data.
        Thread.sleep((long) secondsToBuffer * 1000);
        Map<Instant, HashMap<String, String>> actualValues = getReceivedEvents(writer, configService).entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, (e) -> ((DBRTimeEvent) e.getValue()).getFields()));

        logger.info("expectedValues: " + expectedInstantFieldValues);
        logger.info("actualValues: " + actualValues);
        for (Map.Entry<Instant, HashMap<String, String>> e : expectedInstantFieldValues.entrySet()) {
            var actualMap = actualValues.get(e.getKey());
            logger.info("For time " + e.getKey() + " expected " + e.getValue() + " actual " + actualMap);
            for (var v : e.getValue().entrySet()) {
                if (v.getKey().equals("cnxregainedepsecs")) {
                    assertTrue(Math.abs(Float.parseFloat(v.getValue()) - Float.parseFloat(actualMap.get(v.getKey()))) < 10);
                } else {
                    assertEquals(v.getValue(), actualMap.get(v.getKey()));
                }
            }

        }
    }

}
