/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.pv;

import gov.aps.jca.CAException;

import java.util.HashMap;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.model.ArchiveChannel;






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
public interface PV
{
    /** @return Returns the name. */
    public String getName();


    /** Add a new listener.
     *  @see PVListener
     */
    public void addListener(PVListener listener);

    /** Remove a listener. */
    public void removeListener(PVListener listener);

    /** Start the PV: connect, get meta data, subscribe to updates,
     *  invoke {@link PVListener} for incoming values, ...
     *  @see #addListener(PVListener)
     *  @see #stop()
     */
    public void start() throws Exception;

    /** @return Returns <code>true</code> between <code>start()</code> and <code>stop()</code>. */
    public boolean isRunning();

    /** @return Returns <code>true</code> when connected.
     *  While <code>isRunning</code>, we are subscribed for value updates,
     *  but we might still be disconnected, at least temporarily.
     */
    public boolean isConnected();
    
	/**
	 * Has a CA search been issued on the wire. 
	 * Could be used to throttle connection requests.
	 */
	public boolean hasSearchBeenIssued();


    /** @return <code>true</code> if we have write access to the PV */
    public boolean isWriteAllowed();

    /** Internal state information on the PV.
     *  <p>
     *  Especially when <code>isConnected()</code> is <code>false</code>,
     *  this information might help to diagnose the problem:
     *  Did the PV never connect?
     *  Was it once connected, but some error occurred?
     *  @return Some human readable state info */
    public String getStateInfo();

    /** Stop the PV: disconnect, ...
     *  When the PV is no longer needed, one should 'stop' it
     *  to release resources.
     */
    public void stop();

    /** Get the value.
     *  <p>
     *  This is the most recent value.
     *  Check isConnected() to see if this is valid,
     *  or use inside a PV listener's value update.
     *
     *  @see PVListener
     *  @see #isConnected()
     *  @return Returns the most recent value,
     *          or <code>null</code> if there is none.
     */
    public DBRTimeEvent getValue();

    /** Set PV to given value.
     *  Should accept <code>Double</code>, <code>Double[]</code>,
     *  <code>Integer</code>,
     *  <code>String</code>, maybe more.
     *  @param new_value Value to write to PV
     *  @throws Exception on error
     */
    public void setValue(Object new_value) throws Exception;
    /**
     * get the current DBRTimeEvent
     * @return DBRTimeEvent
     */
	DBRTimeEvent getDBRTimeEvent();

   /***
    * get the archive DBR types for this pv
    * @return ArchDBRTypes
    */
	ArchDBRTypes getArchDBRTypes();

   /***
    * get the time of the first connection established
    * @return the the number of seconds since 1970/01/01
    */
	long getConnectionFirstEstablishedEpochSeconds();

	 /***
	    * get the time of the  connection reestablished
	    * @return the the number of seconds since 1970/01/01
	    */
	long getConnectionLastRestablishedEpochSeconds();

	 /***
	    *
	    * @return count of the connection lost and regained
	    */
	long getConnectionLossRegainCount();

	 /***
	    * get the time of the current connection established
	    * @return the the number of seconds since 1970/01/01
	    */
	long getConnectionEstablishedEpochSeconds();

	 /***
	    * get the time when the connection request was sent
	    * @return the the number of seconds since 1970/01/01
	    */
	long getConnectionRequestMadeEpochSeconds();
	
	
	/**
	 * Reset when we last lost the connection
	 */
	public void resetConnectionLastLostEpochSeconds();
	
	
	/**
	 * Add the cnxlostepsecs and cnxregainedepsecs to the specified DBRTimeEvent and then reset local state. 
	 * @param event
	 */
	public void addConnectionLostRegainedFields(DBRTimeEvent event);


	/***
	 *set the parent pv channel for this meta field  pv 
	 * @param channel
	 */
	public void setParentChannel(ArchiveChannel channel);
	
	/***
	 *set the parent pv channel for this meta field  pv 
	 * @param channel
	 * @param isRuntimeOnly - Only store values in the runtime.
	 */
	public void setParentChannel(ArchiveChannel channel, boolean isRuntimeOnly);

	/***
	 * update the value in the parent pv channel
	 * @param pvName  this meta field pv 's name
	 * @param fieldValue this meta field pv's value
	 */
	public void updataMetaField(String pvName,String fieldValue);
	
	/**
	 * Combine the metadata from various sources and return the latest copy.
	 * @return
	 */
	public HashMap<String, String> getLatestMetadata();
	
	/**
	 * Do a caget and update the metadata that is cached in the PV.
	 * @throws IllegalStateException
	 * @throws CAException
	 */
	public void updateTotalMetaInfo() throws IllegalStateException, CAException;


    /***
     * set this pv has meta field archived or not
     * @param hasMetaField
     */
	void setHasMetaField(boolean hasMetaField);
	
	
	/**
	 * Get the current value of all the meta fields. 
	 * @return - Can return null if this PV has no meta fields.
	 */
	public HashMap<String, String> getCurrentCopyOfMetaFields();
	
	
	public  String getHostName();
	
	
	/**
	 * Get any low level info as a display string; this is typically meant for debugging purposes..
	 * @return
	 */
	public String getLowLevelChannelInfo();
    
}