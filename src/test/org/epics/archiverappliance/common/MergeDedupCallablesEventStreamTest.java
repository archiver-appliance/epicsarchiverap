/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.mergededup.MergeDedupWithCallablesEventStream;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.CallableEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the MergeDedupWithCallablesEventStream
 * @author mshankar
 *
 */
public class MergeDedupCallablesEventStreamTest {
	private static Logger logger = Logger.getLogger(MergeDedupCallablesEventStreamTest.class.getName());
	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":MergeDedupEventStreamTest";
	ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
	short currentYear = TimeUtils.getCurrentYear();
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	private List<Callable<EventStream>> breakStreamIntoChunks(int chunks, ArrayListEventStream strm) {
		List<Callable<EventStream>> ret = new LinkedList<Callable<EventStream>>();
		int perChunk = strm.size()/chunks, i = 0;
		ArrayListEventStream s = new ArrayListEventStream(0, strm.getDescription());
		ret.add(CallableEventStream.makeOneStreamCallable(s, null, false));
		for(Event e : strm) {
			if(i == perChunk) {
				i = 0;
				s = new ArrayListEventStream(0, strm.getDescription());
				ret.add(CallableEventStream.makeOneStreamCallable(s, null, false));
			}
			s.add(e);
			i++;
		}
		logger.info("Broke stream into " + ret.size() + " pieces");
		return ret;
	}

	@Test
	public void testMergeDedup() throws Exception {
		ArrayListEventStream odd = new ArrayListEventStream(60*30*24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
		for(int s = 0; s < 60*30*24; s++) {
			odd.add(new SimulationEvent(s*2 + 1, currentYear, dbrType, new ScalarValue<Double>((double) s*2 + 1)));
		}
		List<Callable<EventStream>> oddc = breakStreamIntoChunks(5, odd);
		
		ArrayListEventStream even = new ArrayListEventStream(60*30*24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
		for(int s = 0; s < 60*30*24; s++) {
			even.add(new SimulationEvent(s*2, currentYear, dbrType, new ScalarValue<Double>((double) s*2)));
		}
		List<Callable<EventStream>> evenc = breakStreamIntoChunks(10, even);

		ArrayListEventStream firsthalf = new ArrayListEventStream(60*30*24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
		for(int s = 0; s < 60*30*24; s++) {
			firsthalf.add(new SimulationEvent(s, currentYear, dbrType, new ScalarValue<Double>((double) s)));
		}
		List<Callable<EventStream>> firsthalfc = breakStreamIntoChunks(2, firsthalf);

		ArrayListEventStream secondhalf = new ArrayListEventStream(60*30*24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
		for(int s = 0; s < 60*30*24; s++) {
			secondhalf.add(new SimulationEvent(s+60*30*24, currentYear, dbrType, new ScalarValue<Double>((double) s+60*30*24)));
		}
		List<Callable<EventStream>> secondhalfc = CallableEventStream.makeOneStreamCallableList(secondhalf);

		ArrayListEventStream firstthreequarters = new ArrayListEventStream(60*45*24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
		for(int s = 0; s < 60*45*24; s++) {
			firstthreequarters.add(new SimulationEvent(s, currentYear, dbrType, new ScalarValue<Double>((double) s)));
		}
		List<Callable<EventStream>> firstthreequartersc = breakStreamIntoChunks(3, firstthreequarters);
		
		ArrayListEventStream combn = new ArrayListEventStream(60*60*24, new RemotableEventStreamDesc(dbrType, pvName, currentYear));
		for(int s = 0; s < 60*60*24; s++) {
			combn.add(new SimulationEvent(s, currentYear, dbrType, new ScalarValue<Double>((double) s)));
		}
		
		assertTrue("odd+empty", takeTwoStreamsMergeAndCompareWithExpected(oddc, new EmptyEventStream(pvName, dbrType), odd));
		assertTrue("odd+odd", takeTwoStreamsMergeAndCompareWithExpected(oddc, odd, odd));		
		assertTrue("odd+even", takeTwoStreamsMergeAndCompareWithExpected(oddc, even, combn));
		assertTrue("odd+even", takeTwoStreamsMergeAndCompareWithExpected(oddc, evenc, combn));
		assertTrue("even+odd", takeTwoStreamsMergeAndCompareWithExpected(evenc, odd, combn));
		assertTrue("even+odd", takeTwoStreamsMergeAndCompareWithExpected(evenc, oddc, combn));
		assertTrue("firsthalf+secondhalf", takeTwoStreamsMergeAndCompareWithExpected(firsthalfc, secondhalfc, combn));
		assertTrue("secondhalf+firsthalf", takeTwoStreamsMergeAndCompareWithExpected(secondhalfc, firsthalfc, combn));
		assertTrue("firstthreequarters+secondhalf", takeTwoStreamsMergeAndCompareWithExpected(firstthreequartersc, secondhalfc, combn));
		assertTrue("secondhalf+firstthreequarters", takeTwoStreamsMergeAndCompareWithExpected(secondhalfc, firstthreequartersc, combn));
	}

	private boolean takeTwoStreamsMergeAndCompareWithExpected(List<Callable<EventStream>> callables1, List<Callable<EventStream>> callables2, EventStream expected) throws IOException {
		MergeDedupWithCallablesEventStream combn = new MergeDedupWithCallablesEventStream(callables1, callables2);
		return takeTwoStreamsMergeAndCompareWithExpected(combn, expected);
	}

	private boolean takeTwoStreamsMergeAndCompareWithExpected(List<Callable<EventStream>> callables1, EventStream strm2, EventStream expected) throws IOException {
		MergeDedupWithCallablesEventStream combn = new MergeDedupWithCallablesEventStream(callables1, strm2, null);
		return takeTwoStreamsMergeAndCompareWithExpected(combn, expected);
	}
	
	private boolean takeTwoStreamsMergeAndCompareWithExpected(MergeDedupWithCallablesEventStream combn, EventStream expected) throws IOException {
		Iterator<Event> it1 = combn.iterator();
		Iterator<Event> it2 = expected.iterator();
		int comparedEvents = 0;
		while(it1.hasNext() && it2.hasNext()) {
			Event e1 = it1.next(), e2 = it2.next();
			comparedEvents++;
			if(!e1.getEventTimeStamp().equals(e2.getEventTimeStamp())) {
				logger.error("Failed comparing " + TimeUtils.convertToHumanReadableString(e1.getEventTimeStamp()) + " and " + TimeUtils.convertToHumanReadableString(e2.getEventTimeStamp()) );
				combn.close(); 
				expected.close(); 
				return false;
			}
			if(!e1.getSampleValue().getValue().equals(e2.getSampleValue().getValue())) {
				logger.error("Failed comparing " + TimeUtils.convertToHumanReadableString(e1.getEventTimeStamp()) + " and " + TimeUtils.convertToHumanReadableString(e2.getEventTimeStamp()) );
				combn.close(); 
				expected.close(); 
				return false; 
			}
		}
		if(it1.hasNext() != it2.hasNext()) {
			logger.error("Not done at the same time " + it1.hasNext() + " and " + it2.hasNext());
			combn.close(); 
			expected.close(); 
			return false;
		}
		
		logger.info("Compared " + comparedEvents + " events");

		combn.close(); expected.close(); return true;
	}
}
