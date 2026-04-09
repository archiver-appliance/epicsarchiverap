/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.model.SampleBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * WriterRunnable is scheduled by the executor in the engine context every writing period.
 * @author Luofeng Li
 *
 */
public class WriterRunnable implements Runnable {
    private static final Logger logger = LogManager.getLogger(WriterRunnable.class);
    /** Minimum write period [seconds] */
    private static final double MIN_WRITE_PERIOD = 1.0;
    /** the sample buffer hash map */
    private final ConcurrentHashMap<String, SampleBuffer> buffers = new ConcurrentHashMap<>();

    /** the configservice used by this WriterRunnable */
    private final ConfigService configservice;
    /** guards against concurrent write() invocations */
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    /** virtual thread executor for parallel per-channel I/O */
    private final ExecutorService writeExecutor = Executors.newVirtualThreadPerTaskExecutor();
    /** tracks in-flight year-change write futures per channel to prevent duplicate submission */
    private final ConcurrentHashMap<String, Future<?>> yearChangeFutures = new ConcurrentHashMap<>();
    /** optional semaphore capping concurrent channel writes; null means unlimited */
    private final Semaphore writeSemaphore;

    /**
     * the constructor
     * @param configservice the configservice used by this WriterRunnable
     */
    public WriterRunnable(ConfigService configservice) {
        this.configservice = configservice;
        int limit = Integer.parseInt(configservice
                .getInstallationProperties()
                .getProperty("org.epics.archiverappliance.engine.epics.writeThreadCount", "0"));
        this.writeSemaphore = (limit > 0) ? new Semaphore(limit) : null;
    }

    /** Add a channel's buffer that this thread reads
     * @param channel ArchiveChannel
     */
    public void addChannel(final ArchiveChannel channel) {
        addSampleBuffer(channel.getName(), channel.getSampleBuffer());
    }

    /**
     * remove one sample buffer from the buffer hash map.
     * At the same time. it also removes the channel from the channel hash map in the engine context
     * @param channelName the name of the channel who and whose sample buffer are removed
     */
    public void removeChannel(final String channelName) {
        SampleBuffer buffer = buffers.get(channelName);
        if (buffer != null) {
            ConcurrentHashMap<String, ArchiveChannel> channelList =
                    configservice.getEngineContext().getChannelList();
            buffer.resetSamples();
            ArrayListEventStream previousSamples = buffer.getPreviousSamples();
            if (!previousSamples.isEmpty()) {
                ArchiveChannel channel = channelList.get(channelName);
                if (channel != null) {
                    try (BasicContext ctx = new BasicContext()) {
                        channel.setlastRotateLogsEpochSeconds(System.currentTimeMillis() / 1000);
                        channel.getWriter().appendData(ctx, channelName, previousSamples);
                    } catch (IOException e) {
                        logger.error("Exception flushing buffer on channel removal for " + channelName, e);
                    }
                }
            }
        }
        buffers.remove(channelName);
    }

    /**
     * add sample buffer into this writer runnable and add year listener to each sample buffer
     * @param name the name of the channel
     * @param buffer the sample buffer for this channel
     */
    void addSampleBuffer(final String name, final SampleBuffer buffer) {
        buffers.put(name, buffer);
        buffer.addYearListener(this::writeYearChange);
    }

    /**
     * set the writing period. when the writing period is at least 10 seconds.
     * When write_period &lt; 10 , the writing period is 10 seconds actually.
     * @param write_period  the writing period in second
     * @return the actual writing period in second
     */
    public double setWritingPeriod(double write_period) {
        double tempwrite_period = write_period;
        if (tempwrite_period < MIN_WRITE_PERIOD) {
            tempwrite_period = MIN_WRITE_PERIOD;
        }
        return tempwrite_period;
    }

    @Override
    public void run() {
        try {
            long startTime = System.currentTimeMillis();
            write();
            long endTime = System.currentTimeMillis();
            configservice.getEngineContext().setSecondsConsumedByWriter((double) (endTime - startTime) / 1000);
        } catch (Exception e) {
            logger.error("Exception", e);
        }
    }

    /**
     * Flush a single channel's sample buffer on a year boundary.
     * Submits the blocking I/O to the virtual thread executor. Skips submission if a
     * year-change write for this channel is already in-flight, preventing duplicate writes.
     * @param buffer the sample buffer to be written
     */
    private void writeYearChange(SampleBuffer buffer) {
        if (writeExecutor.isShutdown()) {
            logger.warn("Skipping year-change flush for {} — executor already shut down", buffer.getChannelName());
            return;
        }
        String channelName = buffer.getChannelName();
        ConcurrentHashMap<String, ArchiveChannel> channelList =
                configservice.getEngineContext().getChannelList();

        buffer.resetSamples();
        ArrayListEventStream previousSamples = buffer.getPreviousSamples();
        if (previousSamples.isEmpty()) return;

        ArchiveChannel channel = channelList.get(channelName);
        if (channel == null) return;

        Future<?> existing = yearChangeFutures.get(channelName);
        if (existing != null && !existing.isDone()) {
            logger.debug("Year-change write already in-flight for {}; skipping duplicate", channelName);
            return;
        }

        channel.aboutToWriteBuffer((DBRTimeEvent) previousSamples.getLast());
        channel.setlastRotateLogsEpochSeconds(System.currentTimeMillis() / 1000);

        Future<?> future = writeExecutor.submit(() -> {
            try (BasicContext ctx = new BasicContext()) {
                channel.getWriter().appendData(ctx, channelName, previousSamples);
                logger.info(channelName + ": year change write complete");
            } catch (IOException e) {
                logger.error("Exception writing year-change buffer for " + channelName, e);
            } finally {
                yearChangeFutures.remove(channelName);
            }
        });
        yearChangeFutures.put(channelName, future);
    }

    /**
     * Write all sample buffers into short term storage in parallel using Java 21 virtual threads.
     * All buffer swaps happen on the scheduler thread before fan-out so that every channel shares
     * a consistent epoch snapshot for the write cycle.
     * @throws Exception error occurs during writing the sample buffer to the short term storage
     */
    private void write() throws Exception {
        if (!isRunning.compareAndSet(false, true)) return;

        final long writeTimestamp = System.currentTimeMillis() / 1000;
        ConcurrentHashMap<String, ArchiveChannel> channelList =
                configservice.getEngineContext().getChannelList();

        try {
            // Phase 1 (scheduler thread): swap all active buffers and prepare write tasks.
            // Doing all swaps before submitting any I/O ensures a consistent epoch snapshot —
            // no channel accumulates new data in its "previous" buffer while another is still writing.
            record WriteTask(ArchiveChannel channel, String name, ArrayListEventStream samples) {}
            List<WriteTask> tasks = new ArrayList<>(buffers.size());

            for (Entry<String, SampleBuffer> entry : buffers.entrySet()) {
                SampleBuffer buffer = entry.getValue();
                if (!buffer.hasCurrentSamples()) continue;

                String channelName = buffer.getChannelName();
                buffer.resetSamples();
                ArrayListEventStream previousSamples = buffer.getPreviousSamples();
                if (previousSamples.isEmpty()) continue;

                ArchiveChannel channel = channelList.get(channelName);
                if (channel == null) continue;

                channel.aboutToWriteBuffer((DBRTimeEvent) previousSamples.getLast());
                channel.setlastRotateLogsEpochSeconds(writeTimestamp);
                tasks.add(new WriteTask(channel, channelName, previousSamples));
            }

            // Phase 2: fan out blocking I/O to virtual threads
            List<Future<?>> futures = new ArrayList<>(tasks.size());
            for (WriteTask task : tasks) {
                futures.add(writeExecutor.submit(() -> {
                    if (writeSemaphore != null) writeSemaphore.acquireUninterruptibly();
                    try (BasicContext ctx = new BasicContext()) {
                        task.channel().getWriter().appendData(ctx, task.name(), task.samples());
                    } catch (IOException e) {
                        logger.error("Exception writing channel " + task.name(), e);
                    } finally {
                        if (writeSemaphore != null) writeSemaphore.release();
                    }
                }));
            }

            // Phase 3: join — preserves backpressure on the scheduler
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            isRunning.set(false);
        }
    }

    /**
     * flush out the sample buffer to the short term storage before shutting down the engine
     * @throws Exception  error occurs during writing the sample buffer to the short term storage
     */
    public void flushBuffer() throws Exception {
        write();
    }

    /**
     * Shut down the virtual thread executor, waiting up to 30 seconds for in-flight writes to complete.
     */
    public void shutdown() {
        writeExecutor.shutdown();
        try {
            if (!writeExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warn("Write executor did not terminate within 30 seconds; forcing shutdown.");
                writeExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            writeExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
