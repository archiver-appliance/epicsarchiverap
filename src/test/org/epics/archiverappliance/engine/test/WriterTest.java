/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.IOException;

import junit.framework.TestCase;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.common.BasicContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
/**
 * do nothing ,just used for creating channels
 * @author Luofeng Li
 *
 */
public class WriterTest extends TestCase implements Writer {

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {

	}

	@Test
	public void testNothing() {

	}

	@Override
	public boolean appendData(BasicContext context, String arg0,
			EventStream arg1) throws IOException {
		//
		return false;
	}

	@Override
	public Event getLastKnownEvent(BasicContext context, String pvName)
			throws IOException {
		return null;
	}

}
