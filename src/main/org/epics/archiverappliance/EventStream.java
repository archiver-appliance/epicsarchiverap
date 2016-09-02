/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance;

import java.io.Closeable;



/**
 * An event stream is a sequence of events in temporal order.
 * The events in the stream come oldest events first. 
 * Implementations of EventStream (<i>should</i>) try their best to support streaming data transfers.
 * <div style="margin-top: 2.0em;"> 
 * EventStreams are typically backed by objects that consume system resources (like file handles, database connections etc).
 * So, clients <b>must</b> close the EventStream once they are done with it.
 * We strongly encourage the use of the Java 1.7 try-with-resources for this purpose.
 * <pre>
 * <code>
 *  <span style="color:blue;">try</span>(EventStream stream = reader.getDataForPV(...)) {
 *   <span style="color:blue;">for</span>(Event event : stream) {
 *   // Do stuff.
 *   }
 * }
 * </code>
 * </pre>
 * </div>
 * <div>
 * EventStreams are typically backed by streams (InputStreams, XMLStreams, database cursors) etc. 
 * While there may be one or two implementations that let you get multiple iterators out of a single EventStream, this is the exception rather than the norm.
 * For most implementation, assume that you can only get one iterator and once that iterator is finished, the EventStream is done.
 * Again, we strongly encourage the use of the Java 1.7 try-with-resources which automatically forces this usage.
 * </div>
 * <div style="margin-top: 2.0em;">
 * The use of Iterable&lt;Event&gt; permits us to use syntactic sugar of the form
 * <pre>
 * <code>
 *   <span style="color:blue;">for</span>(Event event : stream) {
 *   // Do stuff.
 *   }
 * </code>
 * </pre>
 * However, Iterable&lt;Event&gt; does not permit us to throw IOExceptions.
 * This is not ideal in that EventStreams are almost always backed by objects doing I/O and therefore can throw IOExceptions at all points. 
 * So, sometimes these are wrapped into subclasses of RuntimeException
 * </div>
 * @author mshankar
 *
 */
public interface EventStream extends Iterable<Event>, Closeable {
	public EventStreamDesc getDescription();
}
