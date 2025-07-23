/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.pv;

import gov.aps.jca.CAException;
import gov.aps.jca.Channel;
import gov.aps.jca.Context;
import gov.aps.jca.event.ConnectionListener;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;

/**
 * Handle PV context, pool PVs by name.
 * 
 * When using the pure java CA client implementation, it returns the same
 * 'channel' when trying to access the same PV name multiple times. That's good,
 * but I don't know how to determine if the channel for this EPICS_V3_PV is
 * actually shared. Calling destroy() on such a shared channel creates problems.<br>
 * The PVContext adds its own hash map of channels and keeps a reference count.
 * 
 * @author Kay Kasemir
 * s@version Initial version:CSS
 * @version 4-Jun-2012, Luofeng Li:added codes to support for the new archiver
 */
@SuppressWarnings("nls")
public class PVContext {
	private static Logger logger = LogManager.getLogger(PVContext.class.getName());

	public enum MonitorMask {
		/** Listen to changes in value beyond 'MDEL' threshold or alarm state */
		VALUE(1 | 4),

		/** Listen to changes in value beyond 'ADEL' archive limit */
		ARCHIVE(2),

		/** Listen to changes in alarm state */
		ALARM(4);

		final private int mask;

		private MonitorMask(final int mask) {
			this.mask = mask;
		}

		/** @return Mask bits used in underlying CA call */
		public int getMask() {
			return mask;
		}
	}

	/**
	 * In principle, we like to close the context when it is no longer needed.
	 * 
	 * This is in fact required for CAJ to close all threads.
	 * 
	 * For JCA, however, there are problems: With R3.14.8.2 on Linux there were
	 * errors "pthread_create error Invalid argument". With R3.14.11 on OS X
	 * 10.5.8 the call to jca_context.destroy() caused an
	 * "Invalid memory access ..." crash.
	 * 
	 * -> We keep the context open.
	 */
	

	/**
	 * Set to <code>true</code> if the pure Java CA context should be used.
	 * <p>
	 * Changes only have an effect before the very first channel is created.
	 */
	

	/** The JCA context reference count. */
	static private long jca_refs = 0;

	private static ConfigService configservice;

	public static void setConfigservice(ConfigService configservice) {
		PVContext.configservice = configservice;
	}

	/** Initialize the JA library, start the command thread. */
	static void initJCA() throws Exception {

		++jca_refs;

	}

	/**
	 * Disconnect from the JA library.
	 * <p>
	 * Without this step, JCA threads can stay around and prevent the
	 * application from quitting.
	 */

	private static void exitJCA() {
		--jca_refs;
		

	}

	/**
	 * Get a new channel, or a reference to an existing one.
	 * 
	 * @param name
	 *            Channel name
	 * @param jcaCommandThreadId The JCA Command thread.
	 * @param conn_callback  &emsp;
	 * @return reference to channel
	 * @throws Exception
	 *             on error
	 * @see #releaseChannel
	 */
	public synchronized static Channel getChannel(final String name, int jcaCommandThreadId, final ConnectionListener conn_callback) throws Exception {

		initJCA();
		final Channel channel = configservice.getEngineContext()
				.getJCACommandThread(jcaCommandThreadId)
				.createChannel(name, conn_callback);
		
		if (channel == null)
			throw new Exception("Cannot create channel '" + name + "'");
		return channel;
	}

	/**
	 * Release a channel.
	 * 
	 * @param channel_ref
	 *            Channel to release.
	 * @throws CAException
	 * @throws IllegalStateException
	 * @see #getChannel(String)
	 */
	synchronized static void releaseChannel(
			final Channel channel,
			final ConnectionListener conn_callback)
			throws IllegalStateException, CAException {
		final String channelname = channel.getName();

		try { 
			channel.removeConnectionListener(conn_callback);
			channel.destroy();
		} catch(IllegalStateException ex) { 
			// You'd get this if the channel is already closed
			logger.debug("Exception removing connection listener from channel " + channelname, ex);
		}		
		exitJCA();
	}

	/**
	 * Add a command to the JCACommandThread.
	 * @param pvName The name of the PV that this applies to
	 * @param jcaCommandThreadId The JCA Command thread for this PV.
	 * @param theChannel this can be null
	 * @param msg  &emsp;
	 * @param command The runnable that will run in the specified command thread 
	 */
	public static void scheduleCommand(String pvName, int jcaCommandThreadId, Channel theChannel, String msg, final Runnable command) {
		try { 
			if(theChannel != null) { 
				if(!configservice.getEngineContext().doesChannelContextMatchThreadContext(theChannel, jcaCommandThreadId)) { 
					logger.error("Command for pv " + pvName + " is incorrectly scheduled on thread " + jcaCommandThreadId + " in " + msg);
//					try { 
//						throw new Exception();
//					} catch (Exception ex) { 
//						logger.error("Command for pv " + pvName + " is incorrectly scheduled on thread " + jcaCommandThreadId, ex);
//					}
				}
			}
		} catch(Throwable t) { 
			logger.error("Exception scheduling command for pv " + pvName, t);
		}
		configservice.getEngineContext().getJCACommandThread(jcaCommandThreadId).addCommand(command);
	}

	/**
	 * Helper for unit test.
	 * 
	 * @return <code>true</code> if all has been release.
	 */
	static boolean allReleased() {
		return jca_refs == 0;
	}	
}
