/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.pv;

import gov.aps.jca.CAException;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A control system PV.
 *  <p>
 *  When 'start'ed, the PV will attempt to connect or
 *  do whatever is needed to obtain the meta information like
 *  units, precision, ... Then it will subscribe to updates
 *  of the current value.
 *  <p>
 *  While the {@link PVListener} might receive events on a
 *  non-UI thread, all the calls to the PV should come from
 *  the UI thread to prevent possible deadlocks.
 *  (The JNI CA client has deadlocked when both UI and non-UI
 *   threads called into it at the 'same' time).
 *
 *  @author Kay Kasemir
 * @version Initial version:CSS
 * @version 4-Jun-2012, Luofeng Li:added codes to support for the new archiver
 */
public interface PV {
    /** @return Returns the name. */
    public String getName();

    /** Add a new listener.
     * @param listener PVListener
     * @see PVListener
     */
    public void addListener(PVListener listener);

    /** Remove a listener.
     * @param listener PVListener
     */
    public void removeListener(PVListener listener);

    /** Start the PV: connect, get meta data, subscribe to updates,
     *  invoke {@link PVListener} for incoming values, ...
     *  @throws Exception  &emsp;
     *  @see #addListener(PVListener)
     *  @see #stop()
     */
    public void start() throws Exception;

    /** Stop the PV: disconnect, ...
     *  When the PV is no longer needed, one should 'stop' it
     *  to release resources.
     */
    public void stop();

    /** @return Returns <code>true</code> between <code>start()</code> and <code>stop()</code>. */
    public boolean isRunning();

    /**
     * @return Returns <code>true</code> when connected. While <code>isRunning</code>, we are subscribed for value
     * updates, but we might still be disconnected, at least temporarily.
     */
    public boolean isConnected();

    /**
     * @return Returns <code>true</code> when connected. While <code>isRunning</code>, we are subscribed for value
     * updates, but we might still be disconnected, at least temporarily.
     */
    public PVConnectionState connectionState();

    /***
     * get the archive DBR types for this pv
     * @return ArchDBRTypes  &emsp;
     */
    ArchDBRTypes getArchDBRTypes();

    /**
     * Combine the metadata from various sources and return the latest copy.
     * @return HashMap   &emsp;
     */
    public HashMap<String, String> getLatestMetadata();

    /**
     * Do a caget and update the metadata that is cached in the PV.
     * @throws IllegalStateException  &emsp;
     * @throws CAException  &emsp;
     */
    public void updateTotalMetaInfo() throws IllegalStateException, CAException;

    /**
     * Get the MetaInfo for this PV; used during initial MetaGet phase
     * @return MetaInfo  &emsp;
     */
    public MetaInfo getTotalMetaInfo();

    public String getHostName();

    /**
     * Get any low level info as a displayable list; this is typically meant for debugging purposes..
     * Add these to as key value pairs to the statuses
     */
    public void getLowLevelChannelInfo(List<Map<String, String>> statuses);

    /**
     * This method is called each time the ArchiveChannel has written changed a DBRTimeEvent into the buffers.
     */
    public void sampleWrittenIntoStores();

    /**
     * This method is called each time the Write thread is about to write a batch of samples.
     * The writer thread passes in the last sample of the previous batch of samples after buffer rotation.
     */
    public void aboutToWriteBuffer(DBRTimeEvent lastEvent);
}
