/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.pv;

import com.cosylab.epics.caj.CAJChannel;
import com.cosylab.epics.caj.CAJContext;
import gov.aps.jca.CAException;
import gov.aps.jca.Channel;
import gov.aps.jca.event.ConnectionListener;
import gov.aps.jca.event.ContextExceptionListener;
import gov.aps.jca.event.ContextMessageListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.engine.model.ContextErrorHandler;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
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
    private static final long DELAY_MILLIS = 100;

    private static final Logger logger = LogManager.getLogger(JCACommandThread.class.getName());

    /** The JCA Context */
    private final CAJContext jca_context;

    /**
     * Command queue.
     * <p>
     * SYNC on access
     */
    private final LinkedList<Runnable> command_queue = new LinkedList<Runnable>();

    /** Maximum size that command_queue reached at runtime */
    private int max_size_reached = 0;

    /** Flag to tell thread to run or quit */
    private boolean run = false;

    /**
     * Construct, but don't start the thread.
     *
     * @see #start()
     */
    public JCACommandThread() throws ConfigException {
        super("JCA Command Thread");
        try {
            jca_context = new CAJContext();
            jca_context.setDoNotShareChannels(true);
            final ContextErrorHandler log_handler = new ContextErrorHandler();
            jca_context.addContextExceptionListener(log_handler);
            jca_context.addContextMessageListener(log_handler);
            final ContextExceptionListener[] ex_lsnrs = jca_context.getContextExceptionListeners();
            for (ContextExceptionListener exl : ex_lsnrs) {
                if (exl != log_handler) {
                    jca_context.removeContextExceptionListener(exl);
                }
            }

            // Same with message listeners
            final ContextMessageListener[] msg_lsnrs = jca_context.getContextMessageListeners();
            for (ContextMessageListener cml : msg_lsnrs) {
                if (cml != log_handler) {
                    jca_context.removeContextMessageListener(cml);
                }
            }
        } catch (CAException ex) {
            logger.fatal("Fatal exception intializing CA context. Can't proceed", ex);
            throw new ConfigException("Fatal exception intializing CA context. Can't proceed", ex);
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
        if (run) return;
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
            if (command_queue.isEmpty()) break;
        }
        run = false;
    }

    public Channel createChannel(final String name, final ConnectionListener conn_callback)
            throws IllegalStateException, CAException {
        return this.jca_context.createChannel(name, conn_callback);
    }

    public boolean hasContextBeenInitialized() {
        return this.jca_context != null && this.jca_context.isInitialized();
    }

    public boolean doesChannelContextMatchThreadContext(Channel channel) {
        return this.jca_context.equals(channel.getContext());
    }

    public List<Channel> getAllChannelsForPV(String pvNameOnly) {
        var ret = new LinkedList<Channel>();
        for (Channel channel : this.jca_context.getChannels()) {
            String channelNameOnly = channel.getName().split("\\.")[0];
            if (channelNameOnly.equals(pvNameOnly)) {
                ret.add(channel);
            }
        }
        return ret;
    }

    public int getTotalChannelCount() {
        return this.jca_context.getChannels().length;
    }

    public int getChannelsWithPendingSearchRequests() {
        int channelsWithPendingSearchRequests = 0;
        for (Channel channel : this.jca_context.getChannels()) {
            CAJChannel cajChannel = (CAJChannel) channel;
            if (cajChannel.getTimerId() != null) channelsWithPendingSearchRequests++;
        }
        return channelsWithPendingSearchRequests;
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
            if (command_queue.size() >= max_size_reached) max_size_reached = command_queue.size() + 1;
            command_queue.addLast(command);
        }
    }

    /** @return Oldest queued command or <code>null</code> */
    private Runnable getCommand() {
        synchronized (command_queue) {
            if (!command_queue.isEmpty()) return command_queue.removeFirst();
        }
        return null;
    }

    @Override
    public void run() {
        while (run) {
            // Execute all the commands currently queued...
            Runnable command = getCommand();
            while (command != null) { // Execute one command
                try {
                    command.run();
                } catch (Throwable ex) {
                    logger.error("exception when command runs  in JCACommandThread", ex);
                }
                // Get next command
                command = getCommand();
            }
            // Flush.
            // Once, after executing all the accumulated commands.
            // Even when the command queue was empty,
            // there may be stuff worth flushing.
            try {
                if (jca_context != null && !jca_context.isDestroyed()) jca_context.flushIO();
            } catch (Throwable ex) {
                logger.error("exception when flushing io  in JCACommandThread", ex);
            }
            // Then wait.
            try {
                Thread.sleep(DELAY_MILLIS);
            } catch (InterruptedException ex) {
                /* don't even ignore */
                logger.error("exception when thread sleeping in JCACommandThread", ex);
            }
        }
    }

    void destoryContext() {
        addCommand(() -> {
            try {
                if (jca_context != null) {
                    jca_context.destroy();
                }

            } catch (Exception ex) {
                logger.error("exception when destorying context  in JCACommandThread", ex);
            }
        });
    }

    public List<Map<String, String>> getCommandThreadDetails(int threadId) {
        List<Map<String, String>> ret = new LinkedList<Map<String, String>>();
        {
            Map<String, String> obj = new LinkedHashMap<String, String>();
            obj.put("name", "Command thread id");
            obj.put("value", Integer.toString(threadId));
            obj.put("source", "engine");
            ret.add(obj);    
        }

        {
            Map<String, String> obj = new LinkedHashMap<String, String>();
            obj.put("name", "Current command queue size");
            obj.put("value", Integer.toString(this.command_queue.size()));
            obj.put("source", "engine");
            ret.add(obj);    
        }


        {
            Map<String, String> obj = new LinkedHashMap<String, String>();
            obj.put("name", "Max command queue size");
            obj.put("value", Integer.toString(this.max_size_reached));
            obj.put("source", "engine");
            ret.add(obj);    
        }

        return ret;
    }
}
