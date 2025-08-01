/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance;

import com.google.protobuf.Message;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.SampleValue;

import java.time.Instant;

/**
 * An event represents an archiver sample.
 * An actual sample has much more info; this interface outlines the minimum needed for the archiver appliance server side code.
 * Additional information can be gathered by using one of the other event related interfaces in org.epics.archiverappliance.data.
 * All the implementations of Event so far also implement DBRTimeEvent; this may change in the future.
 * @author mshankar
 *
 */
public interface Event {
    /**
     * Get java epoch seconds of the timestamp of this event.
     * Note that we are skipping nanos.
     * To get to the nanos use the getEventTimeStamp method.
     * @return The java epoch seconds of this event.
     */
    public long getEpochSeconds();

    /**
     * Get the epoch seconds and the nanos..
     * We use Instant as the main timestamp class.
     * See TimeUtils for more time related utilities.
     * @return The java epoch seconds and the nanos of this event
     */
    public Instant getEventTimeStamp();

    /**
     * Return a serialized form of this event in the internal currency of the archiver appliance. For now, this is Google's Protocol Buffers
     * Note that the raw form is always escaped according to the archiver specification.
     * This is to have a minimum of conversion overhead when streaming data out to servers.
     * @return A serialization of this event in the internal currency of the archiver appliance.
     */
    public ByteArray getRawForm();

    /**
     * Get this event's value. The value for an EPICS sample is a complex thing and can be scalars and vectors of numbers and strings.
     * With EPICS v4, this can get even more complicated.
     * @return The valus of this event
     */
    public SampleValue getSampleValue();

    /**
     * Make a clone of this event free from the confines of its containing stream.
     * @return A clone of this event
     */
    public Event makeClone();

    public ArchDBRTypes getDBRType();

    /**
     * Return the protobuf message.
     */
    public Message getProtobufMessage();

    /**
     * Return the protobuf message class.
     */
    public Class<? extends Message> getProtobufMessageClass();
}
