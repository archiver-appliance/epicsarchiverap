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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

    private final int commandThreadId;

    /** The JCA Context */
    private final CAJContext jca_context;

    /**
     * Command queue.
     * <p>
     * SYNC on access
     */
    private final LinkedList<Command> commandQueue = new LinkedList<>();

    private final AtomicReference<Command> currentCommand = new AtomicReference<>();
    private volatile long lastCommandCompletedAtMillis;
    private final AtomicLong commandsExecuted = new AtomicLong(0);

    /** Maximum size that commandQueue reached at runtime */
    private int max_size_reached = 0;

    /** Flag to tell thread to run or quit */
    private boolean run = false;

    /**
     * Construct, but don't start the thread.
     *
     * @see #start()
     */
    public JCACommandThread(int commandThreadId) throws ConfigException {
        super("JCA Command Thread " + commandThreadId);
        this.commandThreadId = commandThreadId;
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

        // Context destruction must run on the command thread itself, so enqueue it and give the
        // thread a brief window to drain the queue before stopping the loop.
        destoryContext();

        for (int m = 0; m < 30; m++) {
            if (commandQueue.isEmpty()) break;
            Thread.sleep(100);
        }
        run = false;

        // Wake the run-loop sleep and wait for the thread (and the CAJ context's own threads)
        // to actually exit before the engine webapp is undeployed.
        this.interrupt();
        this.join(10_000);
        if (this.isAlive()) {
            logger.warn("JCA command thread {} did not terminate within 10s of shutdown", commandThreadId);
        }
    }

    public Channel createChannel(final String name, final ConnectionListener conn_callback)
            throws IllegalStateException, CAException {
        return this.jca_context.createChannel(name, conn_callback);
    }

    public boolean hasContextBeenInitialized() {
        return this.jca_context != null && this.jca_context.isInitialized();
    }

    /** @return whether the underlying CAJ context has been destroyed */
    boolean isContextDestroyed() {
        return this.jca_context != null && this.jca_context.isDestroyed();
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

    private static class Command {
        private final String label;
        private final String pvName;
        private final Runnable runnable;
        private final long queuedAtMillis;
        private volatile long startedAtNanos;

        private Command(String label, String pvName, Runnable runnable, long queuedAtMillis) {
            this.label = label;
            this.pvName = pvName;
            this.runnable = runnable;
            this.queuedAtMillis = queuedAtMillis;
        }
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
        addCommand("unlabelled", null, command);
    }

    public void addCommand(String label, String pvName, final Runnable command) {
        Command queuedCommand = new Command(label, pvName, command, System.currentTimeMillis());
        synchronized (commandQueue) {
            // New maximum queue length (+1 for the one about to get added)
            if (commandQueue.size() >= max_size_reached) max_size_reached = commandQueue.size() + 1;
            commandQueue.addLast(queuedCommand);
        }
    }

    /** @return Oldest queued command or <code>null</code> */
    private Command getCommand() {
        synchronized (commandQueue) {
            if (!commandQueue.isEmpty()) return commandQueue.removeFirst();
        }
        return null;
    }

    @Override
    public void run() {
        while (run) {
            // Execute all the commands currently queued...
            Command command = getCommand();
            while (command != null) { // Execute one command
                command.startedAtNanos = System.nanoTime();
                currentCommand.set(command);
                try {
                    command.runnable.run();
                } catch (Throwable ex) {
                    logger.error(
                            "Exception running command '{}' for PV '{}' on JCA command thread {}",
                            command.label,
                            command.pvName,
                            commandThreadId,
                            ex);
                } finally {
                    commandsExecuted.incrementAndGet();
                    lastCommandCompletedAtMillis = System.currentTimeMillis();
                    currentCommand.set(null);
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
                logger.warn("Interrupted by shutdown.");
                break;
            }
        }
    }

    void destoryContext() {
        addCommand("destroyContext", null, () -> {
            try {
                if (jca_context != null) {
                    jca_context.destroy();
                }

            } catch (Exception ex) {
                logger.error("exception when destorying context  in JCACommandThread", ex);
            }
        });
    }

    public List<Map<String, String>> getCommandThreadDetails() {
        List<Map<String, String>> ret = new LinkedList<Map<String, String>>();
        {
            Map<String, String> obj = new LinkedHashMap<String, String>();
            obj.put("name", "Command thread id");
            obj.put("value", Integer.toString(commandThreadId));
            obj.put("source", "engine");
            ret.add(obj);
        }

        int queueSize;
        int maximumQueueSize;
        long oldestQueuedCommandAge;
        synchronized (commandQueue) {
            queueSize = commandQueue.size();
            maximumQueueSize = max_size_reached;
            oldestQueuedCommandAge = commandQueue.isEmpty()
                    ? 0
                    : Math.max(0, System.currentTimeMillis() - commandQueue.getFirst().queuedAtMillis);
        }
        addDetail(ret, "Current command queue size", Integer.toString(queueSize));
        addDetail(ret, "Max command queue size", Integer.toString(maximumQueueSize));

        Command runningCommand = currentCommand.get();
        addDetail(ret, "Current command label", runningCommand == null ? "" : runningCommand.label);
        addDetail(ret, "Current command PV", runningCommand == null ? "" : valueOrEmpty(runningCommand.pvName));
        addDetail(
                ret,
                "Current command running for (ms)",
                runningCommand == null || runningCommand.startedAtNanos == 0
                        ? "0"
                        : Long.toString(
                                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - runningCommand.startedAtNanos)));
        addDetail(ret, "Last command completed at (epoch ms)", Long.toString(lastCommandCompletedAtMillis));
        addDetail(ret, "Commands executed", Long.toString(commandsExecuted.get()));
        addDetail(ret, "Oldest queued command age (ms)", Long.toString(oldestQueuedCommandAge));

        return ret;
    }

    private static String valueOrEmpty(String value) {
        return value == null ? "" : value;
    }

    private void addDetail(List<Map<String, String>> details, String name, String value) {
        Map<String, String> obj = new LinkedHashMap<>();
        obj.put("name", name);
        obj.put("value", value);
        obj.put("source", "engine");
        details.add(obj);
    }
}
