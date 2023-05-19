/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.pv;

import gov.aps.jca.Context;
import gov.aps.jca.JCALibrary;
import gov.aps.jca.configuration.Configuration;
import gov.aps.jca.configuration.DefaultConfigurationBuilder;
import gov.aps.jca.event.ContextExceptionListener;
import gov.aps.jca.event.ContextMessageListener;

import java.io.ByteArrayInputStream;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.epics.JCAConfigGen;
import org.epics.archiverappliance.engine.model.ContextErrorHandler;

/**
 * JCA command pump, added for two reasons:
 * <ol>
 * <li>JCA callbacks can't directly send JCA commands without danger of a
 * deadlock, at least not with JNI and the "DirectRequestDispatcher".
 * <li>Instead of calling 'flushIO' after each command, this thread allows for a
 * few requests to queue up, then periodically pumps them out with only a final
 * 'flush'
 * </ol>
 * 
 * @author Kay Kasemir
 * @version Initial version:CSS
 * @version 4-Jun-2012, Luofeng Li:added codes to support for the new archiver
 */
@SuppressWarnings("nls")
public class JCACommandThread extends Thread {
	/**
	 * Delay between queue inspection. Longer delay results in bigger 'batches',
	 * which is probably good, but also increases the latency.
	 */
	final private static long DELAY_MILLIS = 100;

	private static final Logger logger = LogManager.getLogger(JCACommandThread.class.getName());

	/** The JCA Context */
	private volatile Context jca_context = null;

	/** The Java CA Library instance. */
	private JCALibrary jca = null;

	/**
	 * Command queue.
	 * <p>
	 * SYNC on access
	 */
	final private LinkedList<Runnable> command_queue = new LinkedList<Runnable>();

	/** Maximum size that command_queue reached at runtime */
	private int max_size_reached = 0;

	/** Flag to tell thread to run or quit */
	private boolean run = false;
	
	private ConfigService configService;

	/**
	 * Construct, but don't start the thread.
	 * 
	 * @param configService ConfigService
	 * @see #start()
	 */
	public JCACommandThread(ConfigService configService) {
		super("JCA Command Thread");
		// this.jca_context = jca_context;
		this.configService = configService;

	}

	Context getContext() {
		return jca_context;
	}

	private void initContext() {
		try {
			if (jca == null) {
	
				ByteArrayInputStream bis = JCAConfigGen.generateJCAConfig(configService);
				jca = JCALibrary.getInstance();
				DefaultConfigurationBuilder configBuilder = new DefaultConfigurationBuilder();
				Configuration configuration;

				configuration = configBuilder.build(bis);

				jca_context = jca.createContext(configuration);

				

				// Per default, JNIContext adds a logger to System.err,
				// but we want this one:
				final ContextErrorHandler log_handler = new ContextErrorHandler();
				jca_context.addContextExceptionListener(log_handler);
				jca_context.addContextMessageListener(log_handler);

				// Debugger shows that JNIContext adds the System.err
				// loggers during initialize(), which for example happened
				// in response to the last addContext... calls, so fix
				// it after the fact:
				final ContextExceptionListener[] ex_lsnrs = jca_context
						.getContextExceptionListeners();
				for (ContextExceptionListener exl : ex_lsnrs)
					if (exl != log_handler)
						jca_context.removeContextExceptionListener(exl);

				// Same with message listeners
				final ContextMessageListener[] msg_lsnrs = jca_context
						.getContextMessageListeners();
				for (ContextMessageListener cml : msg_lsnrs)
					if (cml != log_handler)
						jca_context.removeContextMessageListener(cml);

			}

		} catch (Exception e) {
			//
			logger.error("exception when initing Context in JCACommandThread",
					e);

		}
	}

	/**
	 * Version of <code>start</code> that may be called multiple times.
	 * <p>
	 * The thread must only be started after the first PV has been created.
	 * Otherwise, if flush is called without PVs, JNI JCA reports pthread
	 * errors.
	 * <p>
	 * NOP when already running
	 */
	@Override
	public synchronized void start() {
		if (run)
			return;
		run = true;
		super.start();
	}

	/**
	 * Stop the thread and wait for it to finish
	 * 
	 * @throws InterruptedException  &emsp;
	 */
	public void shutdown() throws InterruptedException {

		// destory context

		destoryContext();

		for (int m = 0; m < 30; m++) {
			Thread.sleep(1000);
			if (command_queue.size() == 0)
				break;
		}
		run = false;
		
	}

	/**
	 * Add a command to the queue. add some cap on the command queue? At least
	 * for value updates?
	 * 
	 * @param command Runnable
	 */
	public void addCommand(final Runnable command) {
		synchronized (command_queue) {
			// New maximum queue length (+1 for the one about to get added)
			if (command_queue.size() >= max_size_reached)
				max_size_reached = command_queue.size() + 1;
			command_queue.addLast(command);
		}
	}

	/** @return Oldest queued command or <code>null</code> */
	private Runnable getCommand() {
		synchronized (command_queue) {
			if (command_queue.size() > 0)
				return command_queue.removeFirst();
		}
		return null;
	}

	@Override
	public void run() {
		initContext();

		while (run) {
			// Execute all the commands currently queued...
			Runnable command = getCommand();
			while (command != null) { // Execute one command
				try {
					command.run();
				} catch (Throwable ex) {
					logger.error("exception when command runs  in JCACommandThread",
							ex);
				}
				// Get next command
				command = getCommand();
			}
			// Flush.
			// Once, after executing all the accumulated commands.
			// Even when the command queue was empty,
			// there may be stuff worth flushing.
			try {
				if(jca_context!=null)jca_context.flushIO();
			} catch (Throwable ex) {
				logger.error("exception when flushing io  in JCACommandThread",
						ex);
			}
			// Then wait.
			try {
				Thread.sleep(DELAY_MILLIS);
			} catch (InterruptedException ex) { /* don't even ignore */
				logger.error("exception when thread sleeping in JCACommandThread",
						ex);
			}
		}
	}

	void destoryContext() {
		addCommand(new Runnable()

		{
			@Override
			public void run() {
				try {
					if (jca_context != null) {
						jca_context.destroy();
						jca_context = null;
						jca = null;
					}

				} catch (Exception ex) {
					logger.error("exception when destorying context  in JCACommandThread", ex);
				}
			}
		});

	}
}
