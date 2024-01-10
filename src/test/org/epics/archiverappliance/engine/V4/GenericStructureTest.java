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
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.engine.test.MemBufWriter;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVATimeStamp;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.ServerPV;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.bytesToString;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.convertBytesToPVAStructure;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.getReceivedValues;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.startArchivingPV;
import static org.epics.archiverappliance.engine.V4.PVAccessUtil.updateStructure;
import static org.junit.Assert.assertEquals;

/**
 * Checks the storage of a generic PVAAccess structure format
 * and that it can be reliably decoded.
 */
public class GenericStructureTest {

    private static final Logger logger = LogManager.getLogger(GenericStructureTest.class.getName());

    private ConfigService configService;
    private PVAServer pvaServer;

    @Before
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        pvaServer = new PVAServer();
    }

    @After
    public void tearDown() {
        configService.shutdownNow();
        pvaServer.close();
    }

    private String structureJson(Instant instant, String value) {
        String json = "{\"alarm\"" +
                ":{\"severity\":0,\"status\":0}," +
                "\"structure\":{\"level 1\":\"%s\"," +
                "\"level 2\":{\"level 2.1\":\"level 2.1 0\"," +
                "\"level 2.2\":\"level 2.2 0\"}}," +
                "\"timeStamp\":{\"nanoseconds\":%d,\"secondsPastEpoch\":%d,\"userTag\":0}}";
        return String.format(json,value, instant.getNano()  ,instant.getEpochSecond());
    }

    private PVAStructure testStructure(Instant instant) {
        PVATimeStamp timeStamp = new PVATimeStamp(instant);
        String struct_name = "epics:nt/NTScalar:1.0";
        var level1 = new PVAString("level 1", "level 1 0");
        var level21 = new PVAString("level 2.1", "level 2.1 0");
        var level22 = new PVAString("level 2.2", "level 2.2 0");
        var level2 = new PVAStructure("level 2", "struct_level_2", level21, level22);
        var value = new PVAStructure("structure", "structure_name", level1, level2);
        var alarm = new PVAStructure("alarm", "alarm_t",
                new PVAInt("status", 0), new PVAInt("severity", 0));

        return new PVAStructure("struct name", struct_name, value,
                timeStamp, alarm);

    }


    /**
     * Test that output of a generic structure stays consistent.
     *
     * @throws Exception From dealing with pvaStructures
     */
    @Test
    public void testGenericStructureDecoding() throws Exception {

        String pvName = "PV:" + GenericStructureTest.class.getSimpleName() + ":"
                + UUID.randomUUID();

        logger.info("Starting pvAccess test for pv " + pvName);

        Instant firstInstant = Instant.ofEpochSecond(1600000000, 100);
        PVAStructure pvaStructure = testStructure(firstInstant);
        logger.info("Expected Structure bytes: " + bytesToString(encodedStructure(pvaStructure)));

        HashMap<Instant, PVAStructure> expectedStructure = new HashMap<>();
        expectedStructure.put(firstInstant, pvaStructure.cloneData());
        ServerPV serverPV = pvaServer.createPV(pvName, pvaStructure);

        var type = ArchDBRTypes.DBR_V4_GENERIC_BYTES;
        MemBufWriter writer = new MemBufWriter(pvName, type);
        startArchivingPV(pvName, writer,configService, type);

        var entry = updateStructure(pvaStructure, serverPV);
        expectedStructure.put(entry.getKey(), entry.getValue());

        Map<Instant, SampleValue> actualValues = getReceivedValues(writer, configService);
        Map<Instant, PVAData> convertedActualValues = convertBytesToPVAStructure(actualValues);

        assertEquals(expectedStructure, convertedActualValues);
    }


    private ByteBuffer encodedStructure(PVAStructure expectedStructure) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        expectedStructure.encodeType(buffer, new BitSet());
        expectedStructure.encode(buffer);
        buffer.flip();
        return buffer;
    }




    /**
     * Test for a pv changing other values than 'value'
     *
     * @throws InterruptedException If interrupted in operation
     */
    @Test
    public void testJSONOutput() throws Exception {

        String pvName = "PV:" + ChangedFieldsTest.class.getName() + ":"
                + UUID.randomUUID();

        logger.info("Starting pvAccess test for pv " + pvName);

        Instant firstInstant = Instant.ofEpochSecond(1000, 100);
        PVAStructure pvaStructure = testStructure(firstInstant);

        HashMap<Instant, String> expectedStructure = new HashMap<>();
        ServerPV serverPV = pvaServer.createPV(pvName, pvaStructure);

        var type = ArchDBRTypes.DBR_V4_GENERIC_BYTES;
        MemBufWriter writer = new MemBufWriter(pvName, type);
        startArchivingPV(pvName, writer,configService,  type);

        var entry = updateStructure(pvaStructure, serverPV);
        expectedStructure.put(entry.getKey(), structureJson(entry.getKey(), ((PVAString) ((PVAStructure) entry.getValue().get("structure")).get("level 1")).get()));

        Map<Instant, SampleValue> actualValues = getReceivedValues(writer, configService);
        Map<Instant, String> jsonActualValues = convertToJSON(actualValues);

        logger.info("actual " + jsonActualValues);
        assertEquals(expectedStructure, jsonActualValues);
    }

    private Map<Instant, String> convertToJSON(Map<Instant, SampleValue> actualValues) {
        return actualValues.entrySet().stream().map((e) ->
                Map.entry(e.getKey(), e.getValue().toJSONString())
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
