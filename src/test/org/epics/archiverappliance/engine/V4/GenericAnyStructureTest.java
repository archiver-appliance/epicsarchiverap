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
import org.epics.pva.data.*;
import org.epics.pva.data.nt.PVATimeStamp;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.ServerPV;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.epics.archiverappliance.engine.V4.PVAccessUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Checks the storage of a generic PVAAccess structure format
 * and that it can be reliably decoded.
 */
public class GenericAnyStructureTest {

    private static final Logger logger = LogManager.getLogger(GenericAnyStructureTest.class.getName());

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

    private String structureJson(Instant instant) {
        String json = "{\"alarm\":{\"severity\":0,\"status\":0}," + 
        "\"structure\":" + 
            "{\"any0\":{\"any\":\"Any1String\"},\"anyarray\":[{\"any\":[0,1]}],\"structArray\":[{\"string1\":\"String1\"}],\"union\":{\"union\":\"String2\"}}," +
                "\"timeStamp\":{\"nanoseconds\":%d,\"secondsPastEpoch\":%d,\"userTag\":0}}";
        return String.format(json, instant.getNano(), instant.getEpochSecond());
    }

    private PVAStructure testStructure(Instant instant) {
        PVATimeStamp timeStamp = new PVATimeStamp(instant);
        String struct_name = "epics:nt/NTScalar:1.0";
        var any0 = new PVAny("any0", new PVAString("any0String", "Any0String"));
        var intArray = new PVAIntArray("any1IntArray", false);
        intArray.set(new int[] { 0, 1 });
        var anyarray = new PVAAnyArray("anyarray", new PVAny("any1", intArray));
        var pvaStructrueArray = new PVAStructureArray("structArray",
                new PVAStructure("struct_name", "s", new PVAString("string1")),
                new PVAStructure("struct_name", "s", new PVAString("string1", "String1")));
        var pvaUniion = new PVAUnion("union","union_s", 0, new PVAString("string2", "String2"));
        var value = new PVAStructure("structure", "structure_name", any0, anyarray, pvaStructrueArray, pvaUniion);
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
        startArchivingPV(pvName, writer, configService, type);

        var entry = updateStructure(pvaStructure, serverPV);
        expectedStructure.put(entry.getKey(), entry.getValue());

        Map<Instant, SampleValue> actualValues = getReceivedValues(writer, configService);
        Map<Instant, PVAData> convertedActualValues = convertBytesToPVAStructure(actualValues);

        logger.info("actualValues" + actualValues);
        logger.info("convertedActualValues" + convertedActualValues);
        logger.info("expected" + expectedStructure);
        assertEquals(expectedStructure, convertedActualValues);
    }

    private ByteBuffer encodedStructure(PVAStructure expectedStructure) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        expectedStructure.encodeType(buffer, new BitSet());
        expectedStructure.encode(buffer);
        buffer.flip();
        return buffer;
    }

    public static Map.Entry<Instant, PVAStructure> updateStructure(PVAStructure pvaStructure, ServerPV serverPV) {

        try {
            ((PVAny) ((PVAStructure) pvaStructure.get("structure")).get("any0"))
                    .setValue(new PVAString("any1String", "Any1String"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
        Instant instant = Instant.now();
        ((PVATimeStamp) pvaStructure.get("timeStamp")).set(instant);
        try {
            serverPV.update(pvaStructure);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        return Map.entry(instant, pvaStructure);
    }

    /**
     * Test for a pv changing other values than 'value'
     *
     * @throws InterruptedException If interrupted in operation
     */
    @Test
    public void testJSONOutput() throws Exception {

        String pvName = "PV:" + GenericAnyStructureTest.class.getName() + ":"
                + UUID.randomUUID();

        logger.info("Starting pvAccess test for pv " + pvName);

        Instant firstInstant = Instant.ofEpochSecond(1000, 100);
        PVAStructure pvaStructure = testStructure(firstInstant);

        HashMap<Instant, String> expectedStructure = new HashMap<>();
        ServerPV serverPV = pvaServer.createPV(pvName, pvaStructure);

        var type = ArchDBRTypes.DBR_V4_GENERIC_BYTES;
        MemBufWriter writer = new MemBufWriter(pvName, type);
        startArchivingPV(pvName, writer, configService, type);

        var entry = updateStructure(pvaStructure, serverPV);
        expectedStructure.put(entry.getKey(), structureJson(entry.getKey()));

        Map<Instant, SampleValue> actualValues = getReceivedValues(writer, configService);
        Map<Instant, String> jsonActualValues = convertToJSON(actualValues);

        logger.info("actual " + jsonActualValues);
        assertEquals(expectedStructure, jsonActualValues);
    }

    private Map<Instant, String> convertToJSON(Map<Instant, SampleValue> actualValues) {
        return actualValues.entrySet().stream().map((e) -> Map.entry(e.getKey(), e.getValue().toJSONString()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
