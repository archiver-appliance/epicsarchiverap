/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.mergededup.MergeDedupEventStream;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * Test the MergeDedupEventStream
 * @author mshankar
 *
 */
public class MergeDedupEventStreamTest {
    private static Logger logger = LogManager.getLogger(MergeDedupEventStreamTest.class.getName());
    String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":MergeDedupEventStreamTest";
    ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
    short currentYear = TimeUtils.getCurrentYear();

    @Test
    public void testMergeDedup() throws Exception {
        ArrayListEventStream odd =
                new ArrayListEventStream(60 * 30 * 24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
        for (int s = 0; s < 60 * 30 * 24; s++) {
            odd.add(new SimulationEvent(s * 2 + 1, currentYear, dbrType, new ScalarValue<Double>((double) s * 2 + 1)));
        }
        ArrayListEventStream even =
                new ArrayListEventStream(60 * 30 * 24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
        for (int s = 0; s < 60 * 30 * 24; s++) {
            even.add(new SimulationEvent(s * 2, currentYear, dbrType, new ScalarValue<Double>((double) s * 2)));
        }
        ArrayListEventStream firsthalf =
                new ArrayListEventStream(60 * 30 * 24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
        for (int s = 0; s < 60 * 30 * 24; s++) {
            firsthalf.add(new SimulationEvent(s, currentYear, dbrType, new ScalarValue<Double>((double) s)));
        }
        ArrayListEventStream secondhalf =
                new ArrayListEventStream(60 * 30 * 24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
        for (int s = 0; s < 60 * 30 * 24; s++) {
            secondhalf.add(new SimulationEvent(
                    s + 60 * 30 * 24, currentYear, dbrType, new ScalarValue<Double>((double) s + 60 * 30 * 24)));
        }
        ArrayListEventStream firstthreequarters =
                new ArrayListEventStream(60 * 45 * 24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
        for (int s = 0; s < 60 * 45 * 24; s++) {
            firstthreequarters.add(new SimulationEvent(s, currentYear, dbrType, new ScalarValue<Double>((double) s)));
        }

        ArrayListEventStream combn =
                new ArrayListEventStream(60 * 60 * 24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
        for (int s = 0; s < 60 * 60 * 24; s++) {
            combn.add(new SimulationEvent(s, currentYear, dbrType, new ScalarValue<Double>((double) s)));
        }

        Assertions.assertTrue(
                takeTwoStreamsMergeAndCompareWithExpected(odd, new EmptyEventStream(pvName, dbrType), odd),
                "odd+empty");
        Assertions.assertTrue(
                takeTwoStreamsMergeAndCompareWithExpected(new EmptyEventStream(pvName, dbrType), odd, odd),
                "empty+odd");
        Assertions.assertTrue(takeTwoStreamsMergeAndCompareWithExpected(odd, odd, odd), "odd+odd");
        Assertions.assertTrue(takeTwoStreamsMergeAndCompareWithExpected(odd, even, combn), "odd+even");
        Assertions.assertTrue(takeTwoStreamsMergeAndCompareWithExpected(even, odd, combn), "even+odd");
        Assertions.assertTrue(
                takeTwoStreamsMergeAndCompareWithExpected(firsthalf, secondhalf, combn), "firsthalf+secondhalf");
        Assertions.assertTrue(
                takeTwoStreamsMergeAndCompareWithExpected(secondhalf, firsthalf, combn), "secondhalf+firsthalf");
        Assertions.assertTrue(
                takeTwoStreamsMergeAndCompareWithExpected(firstthreequarters, secondhalf, combn),
                "firstthreequarters+secondhalf");
        Assertions.assertTrue(
                takeTwoStreamsMergeAndCompareWithExpected(secondhalf, firstthreequarters, combn),
                "secondhalf+firstthreequarters");
    }

    private boolean takeTwoStreamsMergeAndCompareWithExpected(
            EventStream strm1, EventStream strm2, EventStream expected) throws IOException {
        EventStream combn = new MergeDedupEventStream(strm1, strm2);
        Iterator<Event> it1 = combn.iterator();
        Iterator<Event> it2 = expected.iterator();
        int comparedEvents = 0;
        while (it1.hasNext() && it2.hasNext()) {
            Event e1 = it1.next(), e2 = it2.next();
            comparedEvents++;
            if (!e1.getEventTimeStamp().equals(e2.getEventTimeStamp())) {
                logger.error("Failed comparing " + TimeUtils.convertToHumanReadableString(e1.getEventTimeStamp())
                        + " and " + TimeUtils.convertToHumanReadableString(e2.getEventTimeStamp()));
                combn.close();
                expected.close();
                return false;
            }
            if (!e1.getSampleValue().getValue().equals(e2.getSampleValue().getValue())) {
                logger.error("Failed comparing " + TimeUtils.convertToHumanReadableString(e1.getEventTimeStamp())
                        + " and " + TimeUtils.convertToHumanReadableString(e2.getEventTimeStamp()));
                combn.close();
                expected.close();
                return false;
            }
        }
        if (it1.hasNext() != it2.hasNext()) {
            logger.error("Not done at the same time " + it1.hasNext() + " and " + it2.hasNext());
            combn.close();
            expected.close();
            return false;
        }

        logger.info("Compared " + comparedEvents + " events");

        combn.close();
        expected.close();
        return true;
    }
}
